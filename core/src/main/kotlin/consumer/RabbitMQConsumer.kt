package consumer

import channel.ConsumerChannelProvider
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.Delivery
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import model.PendingRabbitMQMessage
import converter.Converter
import exception.RabbitMQException
import model.RabbitMQAccess
import model.Response
import util.convertToRabbitMQException

private const val MAX_PREFETCH_COUNT = 65000

class RabbitMQConsumer<T: Any>(
    rabbitMQAccess: RabbitMQAccess,
    virtualHost: String,
    queueName: String,
    defaultDispatcher: CoroutineDispatcher,
    private val converter: Converter,
    private val type: Class<T>,
    prefetchCount: Int,
    watchDogIntervalMillis: Long
) {
    private val messageBuffer = Channel<Delivery>(
        capacity = prefetchCount,
        onUndeliveredElement = {
            resendMessage(it)
        }
    )

    private val channelProvider: ConsumerChannelProvider

    init {
        validatePrefetchCount(prefetchCount)
        val deliveryCallback = DeliverCallback { _, message ->
            runBlocking {
                messageBuffer.send(message)
            }
        }
        try {
            channelProvider = ConsumerChannelProvider(
                rabbitMQAccess, virtualHost, queueName, defaultDispatcher,
                deliveryCallback, prefetchCount, watchDogIntervalMillis
            )
        } catch (e: Throwable) {
            throw convertToRabbitMQException(e)
        }

    }

    private fun resendMessage(message: Delivery) {
        runBlocking {
            messageBuffer.send(message)
        }
    }

    private fun validatePrefetchCount(prefetchCount: Int) {
        if(prefetchCount !in 1..MAX_PREFETCH_COUNT) {
            val message = "Invalid prefetch count $prefetchCount." +
                    "Prefetch count must be between 1 and $MAX_PREFETCH_COUNT"
            error(message)
        }
    }

    fun ackMessage(pending: PendingRabbitMQMessage<T>): Response<PendingRabbitMQMessage<T>> {
        return try {
            channelProvider.recreateChannel()
            channelProvider.ack(pending.deliveryTag)
            Response.Success(pending)
        } catch (e: Throwable) {
            Response.Failure(e)
        }
    }


    fun nackMessage(pending: PendingRabbitMQMessage<T>): Response<PendingRabbitMQMessage<T>> {
        return try {
            channelProvider.recreateChannel()
            channelProvider.nack(pending.deliveryTag)
            Response.Success(pending)
        } catch (e: Throwable) {
            Response.Failure(e)
        }
    }

    suspend fun collectNextMessages(
        timeoutMillis: Long = 1000,
        limit: Int = 100
    ): Response<List<PendingRabbitMQMessage<T>>> {
        val list = mutableListOf<PendingRabbitMQMessage<T>>()
        return try {
            if(!channelProvider.channelIsOpen() && messageBuffer.isEmpty) {
                throw RabbitMQException("Consumer lost connection to rabbitmq broker", null)
            }
            withTimeoutOrNull(timeoutMillis) {
                while (list.size < limit) {
                    val message = messageBuffer.receive()
                    try {
                        val converted = converter.toObject(message.body, type)
                        list.add(PendingRabbitMQMessage(converted, message.envelope.deliveryTag))
                    } catch (e: Throwable) {
                        channelProvider.tryAck(message.envelope.deliveryTag)
                        throw e
                    }
                }
            }
            Response.Success(list)
        } catch (e: Throwable) {
            list.forEach { channelProvider.tryNack(it.deliveryTag) }
            Response.Failure(e)
        }
    }

    fun close() {
        try {
            channelProvider.close()
        } catch (_: Throwable) {
            return
        }
    }
}
