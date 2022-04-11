import channel.ProducerChannelProvider
import com.rabbitmq.client.ReturnListener
import kotlinx.coroutines.delay
import org.slf4j.Logger
import converter.Converter
import exception.RabbitMqMessageReturnedException
import kotlinx.coroutines.runBlocking
import model.ConnectionProperties
import model.ProducerProperties
import util.getLogger

class RabbitMQProducer<T: Any> (
    connectionProperties: ConnectionProperties,
    queueName: String,
    private val converter: Converter,
    private val type: Class<T>,
    private val producerProperties: ProducerProperties = ProducerProperties(),
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
        channelProvider = ProducerChannelProvider(connectionProperties, queueName, onReturn)
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
        repeat(producerProperties.publishAttemptCount) {
            try {
                channelProvider.recreateChannel()
                channelProvider.publish(message)
                return
            } catch (e: Throwable) {
                lastException = e
            }
            delay(producerProperties.publishAttemptDelayMillis)
        }
        lastException?.let {
            logger.error("Failed to publish message ${producerProperties.publishAttemptCount} times", it)
            throw it
        }
    }

    fun close() {
        channelProvider.close()
    }
}
