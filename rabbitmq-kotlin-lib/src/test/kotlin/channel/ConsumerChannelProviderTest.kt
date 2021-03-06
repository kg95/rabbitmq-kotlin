package io.github.kg95.rabbitmq.lib.channel

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.ShutdownListener
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkConstructor
import io.mockk.verify
import kotlinx.coroutines.ExperimentalCoroutinesApi
import io.github.kg95.rabbitmq.lib.model.RabbitMqAccess
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException
import java.net.ConnectException

@ExperimentalCoroutinesApi
internal class ConsumerChannelProviderTest {

    private val testDispatcher = UnconfinedTestDispatcher()
    private val testDelivery: DeliverCallback = mockk(relaxed = true)
    private val testShutdown: ShutdownListener = mockk(relaxed = true)
    private val rabbitmqAccess: RabbitMqAccess = mockk(relaxed = true)
    private val virtualHost: String = "/"
    private val queueName: String = "testQueue"
    private val testPrefetchCount: Int = 1000
    private val testWatchDogInterval: Long = 10000

    @BeforeEach
    fun initialize() {
        mockkConstructor(ConnectionFactory::class)
    }

    @AfterEach
    fun tearDown() {
        clearAllMocks()
    }

    @Test
    fun testInitialize() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val channelProvider = ConsumerChannelProvider(
            rabbitmqAccess, virtualHost, queueName, testDispatcher,
            testDelivery, testShutdown, testPrefetchCount, testWatchDogInterval
        )
        verify {
            anyConstructed<ConnectionFactory>().username = rabbitmqAccess.username
            anyConstructed<ConnectionFactory>().password = rabbitmqAccess.password
            anyConstructed<ConnectionFactory>().host = rabbitmqAccess.host
            anyConstructed<ConnectionFactory>().port = rabbitmqAccess.port
            anyConstructed<ConnectionFactory>().virtualHost = virtualHost
            anyConstructed<ConnectionFactory>().isAutomaticRecoveryEnabled = false
            anyConstructed<ConnectionFactory>().newConnection()
            connection.createChannel()
            channel.basicQos(testPrefetchCount)
            channel.basicConsume(queueName, testDelivery, any(), any())
        }
        channelProvider.close()
    }

    @Test
    fun testInitialize_connectionError() {
        every { anyConstructed<ConnectionFactory>().newConnection() } throws ConnectException()
        assertThrows<ConnectException> {
            ConsumerChannelProvider(
                rabbitmqAccess, virtualHost, queueName, testDispatcher,
                testDelivery, testShutdown, testPrefetchCount, testWatchDogInterval
            )
        }
    }

    @Test
    fun testTryAck() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val channelProvider = ConsumerChannelProvider(
            rabbitmqAccess, virtualHost, queueName, testDispatcher,
            testDelivery, testShutdown, testPrefetchCount, testWatchDogInterval
        )

        val deliveryTag = 1L
        channelProvider.tryAck(deliveryTag)

        verify {
            channel.basicAck(deliveryTag, false)
        }
        channelProvider.close()
    }

    @Test
    fun testTryAck_error() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val channelProvider = ConsumerChannelProvider(
            rabbitmqAccess, virtualHost, queueName, testDispatcher,
            testDelivery, testShutdown, testPrefetchCount, testWatchDogInterval
        )

        every { channel.basicAck(any(), false) } throws IOException()

        val deliveryTag = 1L
        channelProvider.tryAck(deliveryTag)

        verify {
            channel.basicAck(deliveryTag, false)
        }
        channelProvider.close()
    }

    @Test
    fun testTryNack() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val channelProvider = ConsumerChannelProvider(
            rabbitmqAccess, virtualHost, queueName, testDispatcher,
            testDelivery, testShutdown, testPrefetchCount, testWatchDogInterval
        )

        val deliveryTag = 1L
        channelProvider.tryNack(deliveryTag)

        verify {
            channel.basicNack(deliveryTag, false, true)
        }
    }

    @Test
    fun testTryNack_error() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val channelProvider = ConsumerChannelProvider(
            rabbitmqAccess, virtualHost, queueName, testDispatcher,
            testDelivery, testShutdown, testPrefetchCount, testWatchDogInterval
        )

        every { channel.basicNack(any(), false, true) } throws IOException()

        val deliveryTag = 1L
        channelProvider.tryNack(deliveryTag)

        verify {
            channel.basicNack(deliveryTag, false, true)
        }
        channelProvider.close()
    }

    @Test
    fun testAck() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val channelProvider = ConsumerChannelProvider(
            rabbitmqAccess, virtualHost, queueName, testDispatcher,
            testDelivery, testShutdown, testPrefetchCount, testWatchDogInterval
        )

        val deliveryTag = 1L
        channelProvider.ack(deliveryTag)

        verify {
            channel.basicAck(deliveryTag, false)
        }
    }

    @Test
    fun testAck_error() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val channelProvider = ConsumerChannelProvider(
            rabbitmqAccess, virtualHost, queueName, testDispatcher,
            testDelivery, testShutdown, testPrefetchCount, testWatchDogInterval
        )

        every { channel.basicAck(any(), false) } throws IOException()

        val deliveryTag = 1L
        assertThrows<IOException> {
            channelProvider.ack(deliveryTag)
        }

        verify {
            channel.basicAck(deliveryTag, false)
        }
        channelProvider.close()
    }

    @Test
    fun testNack() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val channelProvider = ConsumerChannelProvider(
            rabbitmqAccess, virtualHost, queueName, testDispatcher,
            testDelivery, testShutdown, testPrefetchCount, testWatchDogInterval
        )

        val deliveryTag = 1L
        channelProvider.nack(deliveryTag)

        verify {
            channel.basicNack(deliveryTag, false, true)
        }
    }

    @Test
    fun testNack_error() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val channelProvider = ConsumerChannelProvider(
            rabbitmqAccess, virtualHost, queueName, testDispatcher,
            testDelivery, testShutdown, testPrefetchCount, testWatchDogInterval
        )

        every { channel.basicNack(any(), false, true) } throws IOException()

        val deliveryTag = 1L
        assertThrows<IOException> {
            channelProvider.nack(deliveryTag)
        }

        verify {
            channel.basicNack(deliveryTag, false, true)
        }
        channelProvider.close()
    }

    @Test
    fun testRecreateChannel() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val channelProvider = ConsumerChannelProvider(
            rabbitmqAccess, virtualHost, queueName, testDispatcher,
            testDelivery, testShutdown, testPrefetchCount, testWatchDogInterval
        )

        every { channel.isOpen } returns false
        every { connection.isOpen } returns false

        channelProvider.recreateChannel()

        verify(exactly = 2) {
            anyConstructed<ConnectionFactory>().newConnection()
        }
    }

    @Test
    fun testRecreateChannel_error() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val channelProvider = ConsumerChannelProvider(
            rabbitmqAccess, virtualHost, queueName, testDispatcher,
            testDelivery, testShutdown, testPrefetchCount, testWatchDogInterval
        )

        every { channel.isOpen } returns false
        every { connection.isOpen } returns false
        every { anyConstructed<ConnectionFactory>().newConnection() } throws ConnectException()

        assertThrows<ConnectException> { channelProvider.recreateChannel() }

        verify(exactly = 2) {
            anyConstructed<ConnectionFactory>().newConnection()
        }
    }

    @Test
    fun testReconnect() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val channelProvider = ConsumerChannelProvider(
            rabbitmqAccess, virtualHost, queueName, testDispatcher,
            testDelivery, testShutdown, testPrefetchCount, testWatchDogInterval
        )

        every { connection.isOpen } returns false
        every { channel.isOpen } returns false
        every { anyConstructed<ConnectionFactory>().newConnection() } throws IOException()

        testDispatcher.scheduler.advanceTimeBy(20001)

        assertThat(channelProvider.channelIsOpen()).isFalse
        verify(atLeast = 2) { anyConstructed<ConnectionFactory>().newConnection() }

        val newConnection = mockNewSuccessfulConnection()
        mockNewSuccessfulChannel(newConnection)

        testDispatcher.scheduler.advanceTimeBy(20001)

        assertThat(channelProvider.channelIsOpen()).isTrue
    }

    @Test
    fun testReconnectFailed() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val channelProvider = ConsumerChannelProvider(
            rabbitmqAccess, virtualHost, queueName, testDispatcher,
            testDelivery, testShutdown, testPrefetchCount, testWatchDogInterval
        )

        every { connection.isOpen } returns false
        every { channel.isOpen } returns false
        every { anyConstructed<ConnectionFactory>().newConnection() } throws IOException()

        testDispatcher.scheduler.advanceTimeBy(20001)

        verify(atLeast = 3) {
            anyConstructed<ConnectionFactory>().newConnection()
        }
        channelProvider.close()
    }

    @Test
    fun testChannelIsOpen() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val channelProvider = ConsumerChannelProvider(
            rabbitmqAccess, virtualHost, queueName, testDispatcher,
            testDelivery, testShutdown, testPrefetchCount, testWatchDogInterval
        )

        assertThat(channelProvider.channelIsOpen()).isTrue

        every { channel.isOpen } returns false

        assertThat(channelProvider.channelIsOpen()).isFalse
    }

    @Test
    fun testClose() {
        val connection = mockNewSuccessfulConnection()
        mockNewSuccessfulChannel(connection)
        val channelProvider = ConsumerChannelProvider(
            rabbitmqAccess, virtualHost, queueName, testDispatcher,
            testDelivery, testShutdown, testPrefetchCount, testWatchDogInterval
        )

        channelProvider.close()
        verify {
            connection.close()
        }
    }

    private fun mockNewSuccessfulConnection(): Connection {
        val connection = mockk<Connection>(relaxed = true)
        every { anyConstructed<ConnectionFactory>().newConnection() } returns connection
        every { connection.isOpen } returns true
        return connection
    }

    private fun mockNewSuccessfulChannel(connection: Connection): Channel {
        val channel = mockk<Channel>(relaxed = true)
        every { connection.createChannel() } returns channel
        every { channel.isOpen } returns true
        return channel
    }
}
