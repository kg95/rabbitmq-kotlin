import channel.ProducerChannelProvider
import com.rabbitmq.client.ReturnListener
import kotlinx.coroutines.delay
import model.RabbitMqAccess
import model.Queue
import org.slf4j.Logger
import converter.Converter
import exception.RabbitMqMessageReturnedException
import kotlinx.coroutines.runBlocking
import model.ConnectionProperties
import util.getLogger

private const val DEFAULT_PUBLISH_ATTEMPT_COUNT = 1
private const val DEFAULT_PUBLISH_ATTEMPT_DELAY_MILLIS = 1000L

class RabbitMQProducer<T: Any> (
    access: RabbitMqAccess,
    queue: Queue,
    private val converter: Converter,
    private val type: Class<T>,
    private val publishAttemptCount: Int = DEFAULT_PUBLISH_ATTEMPT_COUNT,
    private val publishAttemptDelayMillis: Long = DEFAULT_PUBLISH_ATTEMPT_DELAY_MILLIS,
    private val logger: Logger = getLogger(RabbitMQProducer::class.java)
) {
    private var channelProvider: ProducerChannelProvider

    init {
        val onReturn = ReturnListener { replyCode, replyText, exchange, routingKey, _, body ->
            val message =
                "Message returned from exchange: $exchange with routingKey: $routingKey ," +
                        " replyCode: $replyCode replyText: $replyText, attempting to republish"
            logger.error(message, RabbitMqMessageReturnedException(message))
            runBlocking { publish(body) }
        }
        val connectionProperties = ConnectionProperties(
            access.username, access.password, access.host, access.port, queue.virtualHost
        )
        channelProvider = ProducerChannelProvider(
            connectionProperties, queue.queueName, onReturn
        )
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
        logger.debug("send message: {}", String(message, Charsets.UTF_8))
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
            logger.error("Failed to publish message $publishAttemptCount times", it)
            throw it
        }
    }

    fun close() {
        channelProvider.close()
    }
}
