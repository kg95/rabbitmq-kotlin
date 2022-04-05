import channel.ConsumerChannelProvider
import com.rabbitmq.client.DeliverCallback
import connection.ConnectionFactory
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.newSingleThreadContext
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import model.PendingRabbitMQMessage
import model.Queue
import model.RabbitMqAccess
import util.getLogger
import converter.Converter
import kotlinx.coroutines.delay
import org.slf4j.Logger
import java.time.Duration
import javax.annotation.PreDestroy

private const val CHANNEL_CAPACITY = 65000
private const val DEFAULT_PREFETCH_COUNT = 1000
private const val DEFAULT_ACK_ATTEMPT_COUNT = 1
private const val DEFAULT_ACK_ATTEMPT_DELAY_MILLIS = 1000L

@ObsoleteCoroutinesApi
open class RabbitMQConsumer<T>(
    private val connectionFactory: ConnectionFactory,
    private val defaultDispatcher: CoroutineDispatcher,
    private val access: RabbitMqAccess,
    private val queue: Queue,
    private val converter: Converter,
    private val type: Class<T>,
    private val prefetchCount: Int = DEFAULT_PREFETCH_COUNT,
    private val ackAttemptCount: Int = DEFAULT_ACK_ATTEMPT_COUNT,
    private val ackAttemptDelayMillis: Long = DEFAULT_ACK_ATTEMPT_DELAY_MILLIS,
    lateInitConnection: Boolean = false,
    private val logger: Logger = getLogger(RabbitMQConsumer::class.java)
) {
    private val messageBuffer = Channel<PendingRabbitMQMessage<T>>(
        CHANNEL_CAPACITY,
        onUndeliveredElement = {
            resendMessage(it)
        }
    )
    private val channelIncrementContext = newSingleThreadContext("channelCreationContext")
    private var channelVersion = 1

    private lateinit var channelProvider: ConsumerChannelProvider

    init {
        if(!lateInitConnection) {
            init()
        }
    }

    fun init() {
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
            connectionFactory, access, queue, defaultDispatcher, deliveryCallback, prefetchCount
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
        repeat(ackAttemptCount) {
            try {
                channelProvider.recreateChannel()
                channelProvider.tryAck(pending.deliveryTag)
                return
            } catch (e: Throwable) {
                lastException = e
                channelProvider.recreateChannel()
            }
            delay(ackAttemptDelayMillis)
        }
        lastException?.let {
            val message = "Failed to ack message for tag ${pending.deliveryTag} and value ${pending.value} " +
                    "$ackAttemptCount times"
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
        repeat(ackAttemptCount) {
            try {
                channelProvider.tryNack(pending.deliveryTag)
                return
            } catch (e: Throwable) {
                lastException = e
                channelProvider.recreateChannel()
            }
            delay(ackAttemptDelayMillis)
        }
        lastException?.let {
            val message = "Failed to nack message for tag ${pending.deliveryTag} and value ${pending.value} " +
                    "$ackAttemptCount times"
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
