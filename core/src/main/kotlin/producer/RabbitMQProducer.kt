package producer

import channel.ProducerChannelProvider
import com.rabbitmq.client.ReturnListener
import kotlinx.coroutines.delay
import converter.Converter
import kotlinx.coroutines.runBlocking
import model.RabbitMQAccess
import model.Response
import util.convertToRabbitMQException

class RabbitMQProducer<T: Any> (
    rabbitMQAccess: RabbitMQAccess,
    virtualHost: String,
    queueName: String,
    private val converter: Converter,
    private val type: Class<T>,
    private val publishAttemptCount: Int,
    private val publishAttemptDelayMillis: Long,
) {
    private var channelProvider: ProducerChannelProvider

    init {
        val onReturn = ReturnListener { _, _, _, _, _, body ->
            runBlocking { publish(body) }
        }
        try {
            channelProvider = ProducerChannelProvider(
                rabbitMQAccess, virtualHost, queueName, onReturn
            )
        } catch (e: Throwable) {
            throw convertToRabbitMQException(e)
        }
    }

    suspend fun sendMessage(message: T):Response<T> {
        return try {
            sendMessagesInBytes(
                listOf(converter.toByteArray(message, type))
            )
            Response.Success(message)
        } catch (e: Throwable) {
            Response.Failure(e)
        }

    }

    suspend fun sendMessages(messages: Iterable<T>): Response<List<T>> {
        return try {
            sendMessagesInBytes(
                messages.map { converter.toByteArray(it, type) }
            )
            Response.Success(messages.toList())
        } catch (e: Throwable) {
            Response.Failure(e)
        }
    }

    private suspend fun sendMessagesInBytes(messages: Iterable<ByteArray>) {
        messages.forEach {
            publish(it)
        }
    }

    private suspend fun publish(message: ByteArray) {
        var lastException: Throwable? = null
        repeat(publishAttemptCount) {
            try {
                channelProvider.recreateChannel()
                channelProvider.publish(message)
                return
            } catch (e: Throwable) {
                lastException = e
            }
            delay(publishAttemptDelayMillis)
        }
        lastException?.let {
            throw convertToRabbitMQException(it)
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
