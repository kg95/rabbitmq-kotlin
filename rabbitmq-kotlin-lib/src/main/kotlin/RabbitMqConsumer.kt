package io.github.kg95.rabbitmq.lib

import io.github.kg95.rabbitmq.lib.channel.ConsumerChannelProvider
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.Delivery
import com.rabbitmq.client.ShutdownListener
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import io.github.kg95.rabbitmq.lib.model.PendingRabbitMqMessage
import io.github.kg95.rabbitmq.lib.converter.Converter
import io.github.kg95.rabbitmq.lib.exception.RabbitMqException
import io.github.kg95.rabbitmq.lib.model.RabbitMqAccess
import io.github.kg95.rabbitmq.lib.model.Response
import io.github.kg95.rabbitmq.lib.util.convertToRabbitMqException

private const val MAX_PREFETCH_COUNT = 65000

class RabbitMqConsumer<T: Any>(
    rabbitmqAccess: RabbitMqAccess,
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

    private var channelVersion: Long = 1

    private val channelProvider: ConsumerChannelProvider

    init {
        validatePrefetchCount(prefetchCount)
        val deliveryCallback = DeliverCallback { _, message ->
            runBlocking {
                messageBuffer.send(message)
            }
        }
        val shutdownListener = ShutdownListener {
            channelVersion++
        }
        try {
            channelProvider = ConsumerChannelProvider(
                rabbitmqAccess, virtualHost, queueName, defaultDispatcher,
                deliveryCallback, shutdownListener, prefetchCount, watchDogIntervalMillis
            )
        } catch (e: Throwable) {
            throw convertToRabbitMqException(e)
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

    fun ackMessage(pending: PendingRabbitMqMessage<T>): Response<PendingRabbitMqMessage<T>> {
        if(pending.channelVersion != channelVersion) {
            return Response.Success(pending)
        }
        return try {
            channelProvider.recreateChannel()
            channelProvider.ack(pending.deliveryTag)
            Response.Success(pending)
        } catch (e: Throwable) {
            Response.Failure(e)
        }
    }

    fun ackMessages(pending: List<PendingRabbitMqMessage<T>>): Response<List<PendingRabbitMqMessage<T>>>{
        var operationFailed = false
        var error: Throwable? = null
        pending.forEach {
            if(it.channelVersion == channelVersion) {
                try {
                    channelProvider.recreateChannel()
                    channelProvider.ack(it.deliveryTag)
                } catch (e: Throwable) {
                    operationFailed = true
                    error = e
                }
            }
        }
        return when(operationFailed) {
            true -> Response.Failure(error!!)
            false -> Response.Success(pending)
        }
    }

    fun nackMessage(pending: PendingRabbitMqMessage<T>): Response<PendingRabbitMqMessage<T>> {
        if(pending.channelVersion != channelVersion) {
            return Response.Success(pending)
        }
        return try {
            channelProvider.recreateChannel()
            channelProvider.nack(pending.deliveryTag)
            Response.Success(pending)
        } catch (e: Throwable) {
            Response.Failure(e)
        }
    }

    fun nackMessages(pending: List<PendingRabbitMqMessage<T>>): Response<List<PendingRabbitMqMessage<T>>>{
        var operationFailed = false
        var error: Throwable? = null
        pending.forEach {
            if(it.channelVersion == channelVersion) {
                try {
                    channelProvider.recreateChannel()
                    channelProvider.nack(it.deliveryTag)
                } catch (e: Throwable) {
                    operationFailed = true
                    error = e
                }
            }
        }
        return when(operationFailed) {
            true -> Response.Failure(error!!)
            false -> Response.Success(pending)
        }
    }

    suspend fun collectNextMessages(
        timeoutMillis: Long = 1000,
        limit: Int = 100
    ): Response<List<PendingRabbitMqMessage<T>>> {
        val list = mutableListOf<PendingRabbitMqMessage<T>>()
        return try {
            if(!channelProvider.channelIsOpen() && messageBuffer.isEmpty) {
                throw RabbitMqException("Consumer lost connection to rabbitmq broker", null)
            }
            withTimeoutOrNull(timeoutMillis) {
                while (list.size < limit) {
                    val message = messageBuffer.receive()
                    try {
                        val converted = converter.toObject(message.body, type)
                        list.add(PendingRabbitMqMessage(converted, message.envelope.deliveryTag, channelVersion))
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
