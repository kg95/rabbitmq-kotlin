import channel.ConsumerChannelProvider
import com.rabbitmq.client.DeliverCallback
import connection.ConnectionFactory
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.ExperimentalCoroutinesApi
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
import java.time.Duration
import javax.annotation.PreDestroy

private const val CHANNEL_CAPACITY = 40000
private const val DEFAULT_PREFETCH_COUNT = 5

@ExperimentalCoroutinesApi
@ObsoleteCoroutinesApi
open class RabbitMQConsumer<T>(
    connectionFactory: ConnectionFactory,
    defaultDispatcher: CoroutineDispatcher,
    access: RabbitMqAccess,
    queue: Queue,
    private val converter: Converter,
    private val type: Class<T>,
    private val prefetchCount: Int = DEFAULT_PREFETCH_COUNT
) {
    private val ids = Channel<PendingRabbitMQMessage<T>>(
        CHANNEL_CAPACITY,
        onUndeliveredElement = {
            resendMessage(it)
        }
    )
    private val channelIncrementContext = newSingleThreadContext("channelCreationContext")
    private var channelVersion = 1

    private val logger = getLogger(RabbitMQConsumer::class.java)

    private lateinit var channelProvider: ConsumerChannelProvider

    init {
        connectionFactory.virtualHost = queue.virtualHost

        val deliveryCallback = DeliverCallback { _, message ->
            runBlocking {
                val transformed = converter.toObject(message.body, type)
                if (transformed != null) {
                    ids.send(PendingRabbitMQMessage(transformed, message.envelope.deliveryTag, channelVersion))
                } else {
                    channelProvider.tryAck(message.envelope.deliveryTag)
                }
            }
        }

        channelProvider = ConsumerChannelProvider(
            connectionFactory, access, queue, defaultDispatcher, deliveryCallback
        )
    }

    private fun resendMessage(message: PendingRabbitMQMessage<T>) {
        runBlocking {
            ids.send(message)
        }
    }

    suspend fun ackMessage(pending: PendingRabbitMQMessage<T>) {
        logger.debug("Ack tag: {} for value:{}", pending.deliveryTag, pending.value)
        if (pending.channelVersion != channelVersion) {
            logger.info("Ack for outdated channel, dropping message")
            return
        }
        channelProvider.waitUntilChannelIsReady()
        if (!channelProvider.tryAck(pending.deliveryTag)) {
            logger.info("Try ack failed for tag: {} for value: {}", pending.deliveryTag, pending.value)
            withContext(channelIncrementContext) {
                if (pending.channelVersion == channelVersion) {
                    logger.info("Try ack failed, switch to new channel version")
                    channelVersion += 1
                }
            }
        }
    }

    suspend fun nackMessage(pending: PendingRabbitMQMessage<T>) {
        if (pending.channelVersion != channelVersion) {
            logger.info("Nack for outdated channel, dropping message")
            return
        }
        channelProvider.waitUntilChannelIsReady()
        if (!channelProvider.tryNack(pending.deliveryTag)) {
            logger.info("Try nack failed for tag: {} for value: {}", pending.deliveryTag, pending.value)
            withContext(channelIncrementContext) {
                if (pending.channelVersion == channelVersion) {
                    logger.info("Try nack failed, switch to new channel version")
                    channelVersion += 1
                }
            }
        }
    }

    suspend fun collectSingleMessage() = ids.receive()

    suspend fun collectNextMessages(timeoutInSeconds: Long = 1, limit: Int = 100): List<PendingRabbitMQMessage<T>> {
        val list = mutableListOf<PendingRabbitMQMessage<T>>()
        withTimeoutOrNull(Duration.ofSeconds(timeoutInSeconds).toMillis()) {
            while (list.size < limit) {
                val message = ids.receive()
                list.add(message)
            }
        }
        return list
    }

    @PreDestroy
    fun tearDown() {
        channelProvider.close()
    }
}
