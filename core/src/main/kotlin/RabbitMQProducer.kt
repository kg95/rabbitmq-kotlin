import channel.ProducerChannelProvider
import kotlinx.coroutines.delay
import model.RabbitMqAccess
import model.Queue
import org.slf4j.Logger
import converter.Converter
import util.getLogger
import javax.annotation.PreDestroy

private const val PUBLISH_TRY_AGAIN_DELAY = 20000L
private const val PUBLISH_TRY_COUNT = 6

class RabbitMQProducer<T: Any> (
    connectionFactory: connection.ConnectionFactory,
    access: RabbitMqAccess,
    queue: Queue,
    private val converter: Converter,
    private val type: Class<T>,
    private val logger: Logger = getLogger(RabbitMQProducer::class.java)
) {
    private val channelProvider = ProducerChannelProvider(
        connectionFactory, access, queue
    )

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
        repeat(PUBLISH_TRY_COUNT) {
            try {
                channelProvider.publish(message)
                return
            } catch (e: Throwable) {
                lastException = e
            }
            delay(PUBLISH_TRY_AGAIN_DELAY)
        }
        lastException?.let {
            logger.error("Failed to publish message $PUBLISH_TRY_COUNT times", lastException)
        }
    }

    @PreDestroy
    fun tearDown() {
        channelProvider.close()
    }
}
