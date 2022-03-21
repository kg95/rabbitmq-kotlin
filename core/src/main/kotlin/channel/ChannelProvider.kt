package channel

import com.rabbitmq.client.CancelCallback
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConsumerShutdownSignalCallback
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.ShutdownSignalException
import connection.ConnectionFactory
import util.getLogger
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
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory
import java.io.IOException
import java.time.Duration

private const val WAIT_FOR_CHANNEL_TO_BE_READY_DELAY: Long = 5000
private const val RECONNECT_DELAY: Long = 5000
private const val WATCH_DOG_INTERVAL = 5000L
private const val MAX_PREFETCH_COUNT = 40000
private const val CHANNEL_RENEW_TIMEOUT_SECONDS = 10L
private const val CONNECT_TRY_COUNT = 6
private const val CONNECT_TRY_AGAIN_DELAY = 20000L

@Suppress("TooManyFunctions")
internal class ChannelProvider(
    private val connectionFactory: ConnectionFactory,
    private val rabbitMqAccess: RabbitMqAccess,
    private val queue: Queue,
    defaultDispatcher: CoroutineDispatcher,
    private val deliveryCallback: DeliverCallback,
    private val prefetchCount: Int
) {
    private val logger = getLogger(ChannelProvider::class.java)
    private var channelIsReady = false
    private lateinit var channel: Channel
    private val watchdogScope = CoroutineScope(defaultDispatcher + SupervisorJob())
    private var watchDog: Job? = null

    suspend fun init() {
        connectionFactory.apply {
            username = rabbitMqAccess.username
            password = rabbitMqAccess.password
            host = rabbitMqAccess.host
            port = rabbitMqAccess.port
            virtualHost = queue.virtualHost
        }
        if (prefetchCount > MAX_PREFETCH_COUNT) {
            error("Prefetch count must be below $MAX_PREFETCH_COUNT")
        }
        if (prefetchCount < 0) {
            error("Prefetch count must be above 0")
        }
        channel = createChannel()
        startWatchdog()
    }

    private suspend fun createChannel(): Channel {
        logger.debug("init rabbit channel")
        return newConnection().createChannel().apply {
            val cancelCallback = CancelCallback {
                logger.error("channel got canceled")
                runBlocking {
                    channelIsReady = false
                    closeChannel(this@apply)
                }
            }
            val shutdownCallback = ConsumerShutdownSignalCallback { consumerTag, sig ->
                logger.info("channel got shut down")
                runBlocking {
                    channelIsReady = false
                }
            }
            basicQos(prefetchCount)
            basicConsume(queue.queueName, deliveryCallback, cancelCallback, shutdownCallback)
            channelIsReady = true
        }
    }

    @Suppress("TooGenericExceptionCaught")
    private suspend fun newConnection(): Connection {
        var lastException: Throwable? = null
        repeat(CONNECT_TRY_COUNT) {
            try {
                return connectionFactory.createConnection()
            } catch (e: Throwable) {
                lastException = e
                logger.warn(
                    "Exception while connecting to RabbitMq: {} trying again in: {} ms", e.message,
                    CONNECT_TRY_AGAIN_DELAY
                )
                delay(CONNECT_TRY_AGAIN_DELAY)
            }
        }
        lastException?.let {
            throw it
        }
        error("Should throw exception or return earlier")
    }

    fun tryAck(deliveryTag: Long): Boolean {
        return try {
            channel.basicAck(deliveryTag, false)
            true
        } catch (e: IOException) {
            handleError("Ack", e)
        } catch (e: ShutdownSignalException) {
            handleError("Ack", e)
        }
    }

    private fun handleError(type: String, e: Exception): Boolean {
        logger.warn("Basic $type crashed: {}", e.message)
        if (channelIsReady) {
            channelIsReady = false
            logger.warn("Basic $type crashed, channel not ready: {}", e.message)
        }
        return false
    }

    fun tryNack(deliveryTag: Long): Boolean {
        return try {
            channel.basicNack(deliveryTag, false, true)
            true
        } catch (e: IOException) {
            handleError("Nack", e)
        } catch (e: ShutdownSignalException) {
            handleError("Nack", e)
        }
    }

    private fun startWatchdog() {
        if (watchDog != null && watchDog?.isActive == true) {
            return
        }
        watchDog = watchdogScope.launch {
            while (isActive) {
                delay(WATCH_DOG_INTERVAL)
                logger.debug("Checking channel connectivity!")
                withTimeoutOrNull(Duration.ofSeconds(CHANNEL_RENEW_TIMEOUT_SECONDS).toMillis()) {
                    tryRenew()
                }
            }
        }
    }

    @Suppress("TooGenericExceptionCaught")
    private suspend fun tryRenew() {
        try {
            if (!channelIsReady) {
                renewConnection()
            }
        } catch (e: Throwable) {
            logger.warn("Renewing channel failed waiting 5s", e)
            delay(RECONNECT_DELAY)
        }
    }

    private suspend fun renewConnection() {
        logger.warn("Lost connection renewing it!")
        closeChannel(channel)
        channel = createChannel()
    }

    private suspend fun recreateChannel() {
        channel = createChannel()
    }

    suspend fun waitTillChannelIsReady() {
        while (!channelIsReady) {
            startWatchdog()
            delay(WAIT_FOR_CHANNEL_TO_BE_READY_DELAY)
            logger.debug("Channel is not ready, waiting for it!")
        }
    }

    @Suppress("BlockingMethodInNonBlockingContext")
    private fun closeChannel(channel: Channel) {
        if (!channel.isOpen || !channelIsReady) {
            return
        }
        try {
            channel.close()
        } catch (e: IOException) {
            logger.warn("Error closing channel: {}", e.message)
        } catch (e: ShutdownSignalException) {
            logger.info("Channel already shutdowned")
        }
    }

    fun cancel() {
        watchdogScope.cancel()
        closeChannel(channel)
    }
}
