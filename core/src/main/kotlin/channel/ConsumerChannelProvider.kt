package channel

import com.rabbitmq.client.CancelCallback
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConsumerShutdownSignalCallback
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.ShutdownSignalException
import connection.ConnectionFactory
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import model.Queue
import model.RabbitMqAccess
import org.slf4j.Logger
import util.getLogger
import java.io.IOException
import java.time.Duration

private const val WAIT_FOR_CHANNEL_TO_BE_READY_DELAY: Long = 5000
private const val RECONNECT_DELAY: Long = 5000
private const val WATCH_DOG_INTERVAL = 5000L
private const val MAX_PREFETCH_COUNT = 65000
private const val CHANNEL_RENEW_TIMEOUT_SECONDS = 10L

class ConsumerChannelProvider(
    connectionFactory: ConnectionFactory,
    rabbitMqAccess: RabbitMqAccess,
    queue: Queue,
    dispatcher: CoroutineDispatcher,
    private val deliverCallback: DeliverCallback
): AbstractChannelProvider(connectionFactory, rabbitMqAccess, queue) {

    override val logger: Logger = getLogger(ConsumerChannelProvider::class.java)
    private var watchDog: Job? = null
    private val watchDogScope = CoroutineScope(dispatcher + SupervisorJob())

    init {
        connection = createConnection()
        channel = createChannel()
        startWatchdog()
    }

    override fun createChannel(): Channel {
        return super.createChannel().apply {
            val cancelCallback = CancelCallback {
                logger.error("channel got canceled")
                closeChannel(this@apply)
            }
            val shutdownCallback = ConsumerShutdownSignalCallback { _, _ ->
                logger.info("channel got shut down")
            }
            basicQos(MAX_PREFETCH_COUNT)
            basicConsume(queue.queueName, deliverCallback, cancelCallback, shutdownCallback)
        }
    }

    private fun closeChannel(channel: Channel) {
        try {
            if(channel.isOpen) {
                channel.close()
            }
        } catch (e: IOException) {
            logger.warn("Error closing channel: {}", e.message)
        } catch (e: ShutdownSignalException) {
            logger.info("Channel already shut down")
        }
    }

    private fun startWatchdog() {
        if (watchDog != null && watchDog?.isActive == true) {
            return
        }
        watchDog = watchDogScope.launch {
            while (isActive) {
                delay(WATCH_DOG_INTERVAL)
                logger.debug("Checking channel connectivity!")
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
            logger.warn("Renewing channel failed waiting {} seconds", RECONNECT_DELAY)
            delay(RECONNECT_DELAY)
        }
    }

    private fun renewChannel() {
        logger.warn("Lost connection renewing it!")
        closeChannel(channel)
        channel = createChannel()
    }

    suspend fun waitUntilChannelIsReady() {
        while (!channel.isOpen) {
            startWatchdog()
            delay(WAIT_FOR_CHANNEL_TO_BE_READY_DELAY)
            logger.debug("Channel is not ready, waiting for it!")
        }
    }

    fun tryAck(deliveryTag: Long): Boolean {
        return try {
            channel.basicAck(deliveryTag, false)
            true
        } catch (e: Throwable) {
            handleError("Ack", e)
        }
    }

    fun tryNack(deliveryTag: Long): Boolean {
        return try {
            channel.basicNack(deliveryTag, false, true)
            true
        } catch (e: Throwable) {
            handleError("Nack", e)
        }
    }

    private fun handleError(type: String, e: Throwable): Boolean {
        logger.warn("Basic $type crashed: {}", e.message)
        return false
    }

    override fun close() {
        watchDogScope.cancel()
        super.close()
    }
}
