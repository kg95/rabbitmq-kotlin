package channel

import com.rabbitmq.client.CancelCallback
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConsumerShutdownSignalCallback
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.ShutdownListener
import connection.ConnectionProvider
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import model.ConnectionProperties
import model.ConsumerChannelProperties

internal class ConsumerChannelProvider(
    connectionProperties: ConnectionProperties,
    private val queueName: String,
    dispatcher: CoroutineDispatcher,
    private val deliverCallback: DeliverCallback,
    private val properties: ConsumerChannelProperties
) {
    private val connectionProvider: ConnectionProvider
    private var channel: Channel

    private var watchDog: Job? = null
    private val watchDogScope = CoroutineScope(dispatcher + SupervisorJob())

    init {
        connectionProvider = ConnectionProvider(connectionProperties)
        channel = createChannel()
        startWatchdog()
    }

    private fun createChannel(): Channel {
        return connectionProvider.createChannel().apply {
            val shutDownListener = ShutdownListener {
                closeChannel(this@apply)
            }
            val cancelCallback = CancelCallback {
                closeChannel(this@apply)
            }
            val shutdownCallback = ConsumerShutdownSignalCallback { _, _ ->
                closeChannel(this@apply)
            }
            addShutdownListener(shutDownListener)
            basicQos(properties.prefetchCount)
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
                delay(properties.watchDogIntervalMillis)
                tryRenew()
            }
        }
    }

    private fun tryRenew() {
        if (!channel.isOpen) {
            try {
                renewChannel()
            } catch (e: Throwable) {
                return
            }
        }
    }

    private fun renewChannel() {
        closeChannel(channel)
        channel = createChannel()
    }

    fun channelIsOpen() = channel.isOpen

    fun recreateChannel() {
        if(!channel.isOpen) {
            channel = createChannel()
        }
        startWatchdog()
    }

    fun tryAck(deliveryTag: Long) =
        channel.basicAck(deliveryTag, false)

    fun tryNack(deliveryTag: Long) =
        channel.basicNack(deliveryTag, false, true)

    fun close() {
        watchDogScope.cancel()
        connectionProvider.close()
    }
}
