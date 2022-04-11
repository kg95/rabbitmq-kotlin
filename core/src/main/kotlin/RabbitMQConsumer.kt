import channel.ConsumerChannelProvider
import com.rabbitmq.client.DeliverCallback
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.newSingleThreadContext
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import model.PendingRabbitMQMessage
import util.getLogger
import converter.Converter
import kotlinx.coroutines.delay
import model.ConnectionProperties
import model.ConsumerChannelProperties
import model.ConsumerProperties
import org.slf4j.Logger
import java.time.Duration
import javax.annotation.PreDestroy

@ObsoleteCoroutinesApi
open class RabbitMQConsumer<T>(
    connectionProperties: ConnectionProperties,
    queueName: String,
    defaultDispatcher: CoroutineDispatcher,
    private val converter: Converter,
    private val type: Class<T>,
    private val consumerProperties: ConsumerProperties = ConsumerProperties(),
    consumerChannelProperties: ConsumerChannelProperties = ConsumerChannelProperties(),
    private val logger: Logger = getLogger(RabbitMQConsumer::class.java)
) {
    private val messageBuffer = Channel<PendingRabbitMQMessage<T>>(
        capacity = consumerChannelProperties.prefetchCount,
        onUndeliveredElement = {
            resendMessage(it)
        }
    )
    private val channelIncrementContext = newSingleThreadContext("channelCreationContext")
    private var channelVersion = 1

    private lateinit var channelProvider: ConsumerChannelProvider

    init {
        val deliveryCallback = DeliverCallback { _, message ->
            runBlocking {
                val transformed = converter.toObject(message.body, type)
                if (transformed != null) {
                    messageBuffer.send(
                        PendingRabbitMQMessage(transformed, message.envelope.deliveryTag, channelVersion)
                    )
                } else {
                    channelProvider.tryAck(message.envelope.deliveryTag)
                }
            }
        }
        channelProvider = ConsumerChannelProvider(
            connectionProperties, queueName, defaultDispatcher, deliveryCallback, consumerChannelProperties
        )
    }

    private fun resendMessage(message: PendingRabbitMQMessage<T>) {
        runBlocking {
            messageBuffer.send(message)
        }
    }

    suspend fun ackMessage(pending: PendingRabbitMQMessage<T>) {
        logger.debug("Ack message with tag {} and value {}", pending.deliveryTag, pending.value)
        if (pending.channelVersion != channelVersion) {
            logger.debug("Ack for outdated channel, dropping message")
            return
        }
        var lastException: Throwable? = null
        repeat(consumerProperties.ackAttemptCount) {
            try {
                channelProvider.recreateChannel()
                channelProvider.tryAck(pending.deliveryTag)
                return
            } catch (e: Throwable) {
                lastException = e
                channelProvider.recreateChannel()
            }
            delay(consumerProperties.ackAttemptDelayMillis)
        }
        lastException?.let {
            val message = "Failed to ack message for tag ${pending.deliveryTag} and value ${pending.value} " +
                    "${consumerProperties.ackAttemptCount} times"
            logger.error(message, it)
            withContext(channelIncrementContext) {
                if (pending.channelVersion == channelVersion) {
                    logger.debug("Try ack failed, switch to new channel version")
                    channelVersion += 1
                }
            }
            throw it
        }
    }

    suspend fun nackMessage(pending: PendingRabbitMQMessage<T>) {
        logger.debug("Nack message with tag {} and value {}", pending.deliveryTag, pending.value)
        if (pending.channelVersion != channelVersion) {
            logger.debug("Nack for outdated channel, dropping message")
            return
        }
        var lastException: Throwable? = null
        repeat(consumerProperties.ackAttemptCount) {
            try {
                channelProvider.tryNack(pending.deliveryTag)
                return
            } catch (e: Throwable) {
                lastException = e
                channelProvider.recreateChannel()
            }
            delay(consumerProperties.ackAttemptDelayMillis)
        }
        lastException?.let {
            val message = "Failed to nack message for tag ${pending.deliveryTag} and value ${pending.value} " +
                    "${consumerProperties.ackAttemptCount} times"
            logger.error(message, it)
            withContext(channelIncrementContext) {
                if (pending.channelVersion == channelVersion) {
                    logger.info("Try nack failed, switch to new channel version")
                    channelVersion += 1
                }
            }
            throw it
        }
    }

    suspend fun collectSingleMessage() = messageBuffer.receive()

    suspend fun collectNextMessages(timeoutInSeconds: Long = 1, limit: Int = 100): List<PendingRabbitMQMessage<T>> {
        val list = mutableListOf<PendingRabbitMQMessage<T>>()
        withTimeoutOrNull(Duration.ofSeconds(timeoutInSeconds).toMillis()) {
            while (list.size < limit) {
                val message = messageBuffer.receive()
                list.add(message)
            }
        }
        return list
    }

    @PreDestroy
    fun close() {
        channelProvider.close()
    }
}
