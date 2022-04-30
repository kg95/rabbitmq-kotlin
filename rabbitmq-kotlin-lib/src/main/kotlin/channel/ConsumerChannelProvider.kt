package io.github.kg95.rabbitmq.lib.channel

import com.rabbitmq.client.CancelCallback
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConsumerShutdownSignalCallback
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.ShutdownListener
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import io.github.kg95.rabbitmq.lib.model.RabbitMQAccess

internal class ConsumerChannelProvider(
    rabbitMQAccess: RabbitMQAccess,
    virtualHost: String,
    private val queueName: String,
    dispatcher: CoroutineDispatcher,
    private val deliverCallback: DeliverCallback,
    private val shutdownListener: ShutdownListener,
    private val prefetchCount: Int,
    private val watchDogIntervalMillis: Long
): AbstractChannelProvider(rabbitMQAccess, virtualHost) {

    private var watchDog: Job? = null
    private val watchDogScope = CoroutineScope(dispatcher + SupervisorJob())

    init {
        channel = createChannel()
        startWatchdog()
    }

    override fun createChannel(): Channel {
        return super.createChannel().apply {
            val cancelCallback = CancelCallback {
                super.closeChannel(this@apply)
            }
            val shutdownCallback = ConsumerShutdownSignalCallback { _, _ ->
                super.closeChannel(this@apply)
            }
            addShutdownListener(shutdownListener)
            basicQos(prefetchCount)
            basicConsume(queueName, deliverCallback, cancelCallback, shutdownCallback)
        }
    }

    private fun startWatchdog() {
        if (watchDog != null && watchDog?.isActive == true) {
            return
        }
        watchDog = watchDogScope.launch {
            while (isActive) {
                delay(watchDogIntervalMillis)
                tryRenew()
            }
        }
    }

    private fun tryRenew() {
        if (!channel.isOpen) {
            try {
                renewChannel()
            } catch (_: Throwable) {
                return
            }
        }
    }

    private fun renewChannel() {
        closeChannel(channel)
        channel = createChannel()
    }

    fun tryAck(deliveryTag: Long) {
        try {
            channel.basicAck(deliveryTag, false)
        } catch (_: Throwable) {
            return
        }
    }

    fun tryNack(deliveryTag: Long) {
        try {
            channel.basicNack(deliveryTag, false, true)
        } catch (_: Throwable) {
            return
        }
    }

    fun ack(deliveryTag: Long) {
        channel.basicAck(deliveryTag, false)
    }

    fun nack(deliveryTag: Long) {
        channel.basicNack(deliveryTag, false, true)
    }

    override fun close() {
        watchDogScope.cancel()
        super.close()
    }
}
