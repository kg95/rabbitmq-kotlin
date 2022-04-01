import channel.ProducerChannelProvider
import com.rabbitmq.client.ReturnListener
import kotlinx.coroutines.delay
import model.RabbitMqAccess
import model.Queue
import org.slf4j.Logger
import converter.Converter
import exception.RabbitMqMessageReturnedException
import kotlinx.coroutines.runBlocking
import util.getLogger
import javax.annotation.PreDestroy

class RabbitMQProducer<T: Any> (
    connectionFactory: connection.ConnectionFactory,
    access: RabbitMqAccess,
    queue: Queue,
    private val converter: Converter,
    private val type: Class<T>,
    private val logger: Logger = getLogger(RabbitMQProducer::class.java),
    private val publishAttemptCount: Int = 1,
    private val publishAttemptDelaySeconds: Long = 20
) {
    private val channelProvider: ProducerChannelProvider

    init {
        val onReturn = ReturnListener { replyCode, replyText, exchange, routingKey, _, body ->
            val message =
                "Message returned from exchange: $exchange with routingKey: $routingKey ," +
                        " replyCode: $replyCode replyText: $replyText, attempting to republish"
            logger.error(message, RabbitMqMessageReturnedException(message))
            runBlocking { publish(body) }
        }
        channelProvider = ProducerChannelProvider(
            connectionFactory, access, queue, onReturn
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
            delay(publishAttemptDelaySeconds)
        }
        lastException?.let {
            logger.error("Failed to publish message $publishAttemptCount times", lastException)
        }
    }

    @PreDestroy
    fun tearDown() {
        channelProvider.close()
    }
}
