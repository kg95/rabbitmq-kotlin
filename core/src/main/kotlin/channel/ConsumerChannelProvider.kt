package channel

import com.rabbitmq.client.CancelCallback
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConsumerShutdownSignalCallback
import com.rabbitmq.client.DeliverCallback
import connection.ConnectionProvider
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import model.ConnectionProperties

private const val WATCH_DOG_INTERVAL_MILLIS = 5000L
private const val MAX_PREFETCH_COUNT = 65000
private const val CHANNEL_RENEW_TIMEOUT_MILLIS = 10000L
private const val CHANNEL_RENEW_DELAY_MILLIS = 5000L

class ConsumerChannelProvider(
    connectionProvider: ConnectionProvider,
    connectionProperties: ConnectionProperties,
    dispatcher: CoroutineDispatcher,
    private val queueName: String,
    private val deliverCallback: DeliverCallback,
    private val prefetchCount: Int,
) {
    private val connectionManager: ConnectionManager
    private var channel: Channel

    private var watchDog: Job? = null
    private val watchDogScope = CoroutineScope(dispatcher + SupervisorJob())

    init {
        if(prefetchCount  !in 1..MAX_PREFETCH_COUNT) {
            error(
                "Invalid prefetch count $prefetchCount. Prefetch count must be between 1 and $MAX_PREFETCH_COUNT"
            )
        }
        connectionManager = ConnectionManager(connectionProvider, connectionProperties)
        channel = createChannel()
        startWatchdog()
    }

    private fun createChannel(): Channel {
        return connectionManager.createChannel().apply {
            val cancelCallback = CancelCallback {
                closeChannel(this@apply)
            }
            val shutdownCallback = ConsumerShutdownSignalCallback { _, _ ->
                closeChannel(this@apply)
            }
            basicQos(prefetchCount)
            basicConsume(queueName, deliverCallback, cancelCallback, shutdownCallback)
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
                delay(WATCH_DOG_INTERVAL_MILLIS)
                withTimeoutOrNull(CHANNEL_RENEW_TIMEOUT_MILLIS) {
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
            delay(CHANNEL_RENEW_DELAY_MILLIS)
        }
    }

    private fun renewChannel() {
        closeChannel(channel)
        channel = createChannel()
    }

    fun channelIsOpen() = channel.isOpen

    fun recreateChannel() {
        startWatchdog()
    }

    fun tryAck(deliveryTag: Long) =
        channel.basicAck(deliveryTag, false)

    fun tryNack(deliveryTag: Long) =
        channel.basicNack(deliveryTag, false, true)

    fun close() {
        watchDogScope.cancel()
        connectionManager.close()
    }
}
