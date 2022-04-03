package channel

import com.rabbitmq.client.CancelCallback
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConsumerShutdownSignalCallback
import com.rabbitmq.client.DeliverCallback
import connection.ConnectionFactory
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import model.Queue
import model.RabbitMqAccess
import java.time.Duration

private const val RECONNECT_DELAY: Long = 5000
private const val WATCH_DOG_INTERVAL = 5000L
private const val MAX_PREFETCH_COUNT = 65000
private const val CHANNEL_RENEW_TIMEOUT_SECONDS = 10L

class ConsumerChannelProvider(
    connectionFactory: ConnectionFactory,
    rabbitMqAccess: RabbitMqAccess,
    queue: Queue,
    dispatcher: CoroutineDispatcher,
    private val deliverCallback: DeliverCallback,
    private val prefetchCount: Int,
): AbstractChannelProvider(connectionFactory, rabbitMqAccess, queue) {

    private var watchDog: Job? = null
    private val watchDogScope = CoroutineScope(dispatcher + SupervisorJob())

    init {
        if(prefetchCount  !in 1..MAX_PREFETCH_COUNT) {
            error(
                "Invalid prefetch count $prefetchCount. Prefetch count must be between 1 and $MAX_PREFETCH_COUNT"
            )
        }
        connection = createConnection()
        channel = createChannel()
        startWatchdog()
    }

    override fun createChannel(): Channel {
        return super.createChannel().apply {
            val cancelCallback = CancelCallback {
                closeChannel(this@apply)
            }
            val shutdownCallback = ConsumerShutdownSignalCallback { _, _ ->
                closeChannel(this@apply)
            }
            basicQos(prefetchCount)
            basicConsume(queue.queueName, deliverCallback, cancelCallback, shutdownCallback)
        }
    }

    private fun closeChannel(channel: Channel) {
        try {
            if(channel.isOpen) {
                channel.close()
            }
        } catch (e: Throwable) {
            return
        }
    }

    private fun startWatchdog() {
        if (watchDog != null && watchDog?.isActive == true) {
            return
        }
        watchDog = watchDogScope.launch {
            while (isActive) {
                delay(WATCH_DOG_INTERVAL)
                withTimeoutOrNull(Duration.ofSeconds(CHANNEL_RENEW_TIMEOUT_SECONDS).toMillis()) {
                    tryRenew()
                }
            }
        }
    }

    private suspend fun tryRenew() {
        try {
            if (!channel.isOpen) {
                renewChannel()
            }
        } catch (e: Throwable) {
            delay(RECONNECT_DELAY)
        }
    }

    private fun renewChannel() {
        closeChannel(channel)
        channel = createChannel()
    }

    fun channelIsOpen() = channel.isOpen

    fun recreateChannel() {
        if(!channel.isOpen) {
            startWatchdog()
        }
    }

    fun tryAck(deliveryTag: Long) =
        channel.basicAck(deliveryTag, false)

    fun tryNack(deliveryTag: Long) =
        channel.basicNack(deliveryTag, false, true)

    override fun close() {
        watchDogScope.cancel()
        super.close()
    }
}
