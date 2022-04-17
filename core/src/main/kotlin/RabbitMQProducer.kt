import channel.ProducerChannelProvider
import com.rabbitmq.client.ReturnListener
import kotlinx.coroutines.delay
import converter.Converter
import kotlinx.coroutines.runBlocking
import model.ConnectionProperties
import util.convertToRabbitMQException

private const val DEFAULT_PUBLISH_ATTEMPT_COUNT = 1
private const val DEFAULT_PUBLISH_ATTEMPT_DELAY_MILLIS = 1000L

class RabbitMQProducer<T: Any> (
    connectionProperties: ConnectionProperties,
    queueName: String,
    private val converter: Converter,
    private val type: Class<T>,
    private val publishAttemptCount: Int = DEFAULT_PUBLISH_ATTEMPT_COUNT,
    private val publishAttemptDelayMillis: Long = DEFAULT_PUBLISH_ATTEMPT_DELAY_MILLIS,
) {
    private var channelProvider: ProducerChannelProvider

    init {
        val onReturn = ReturnListener { _, _, _, _, _, body ->
            runBlocking { publish(body) }
        }
        try {
            channelProvider = ProducerChannelProvider(connectionProperties, queueName, onReturn)
        } catch (e: Throwable) {
            throw convertToRabbitMQException(e)
        }
    }

    suspend fun sendMessage(message: T) {
        sendMessagesInBytes(
            listOf(converter.toByteArray(message, type))
        )
    }

    suspend fun sendMessages(messages: Iterable<T>) {
        sendMessagesInBytes(
            messages.map { converter.toByteArray(it, type) }
        )
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
