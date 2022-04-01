package channel

import com.rabbitmq.client.CancelCallback
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConsumerShutdownSignalCallback
import com.rabbitmq.client.DeliverCallback
import connection.DefaultConnectionFactory
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import kotlinx.coroutines.test.TestCoroutineDispatcher
import model.Queue
import model.RabbitMqAccess
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException

internal class ConsumerChannelProviderTest {

    private val connectionFactory: DefaultConnectionFactory = mockk(relaxed = true)
    private val testDispatcher = TestCoroutineDispatcher()
    private val testDelivery: DeliverCallback = mockk(relaxed = true)
    private val access: RabbitMqAccess = RabbitMqAccess("rabbitmq", "rabbitmq", "localhost", 5672)
    private val queue: Queue = Queue("test", "/")

    @Test
    fun testCreateChannel() {
        val channel = mockChannelSuccessful()
        val channelProvider = ConsumerChannelProvider(
            connectionFactory, access, queue, testDispatcher, testDelivery
        )
        verify {
            channel.basicConsume(queue.queueName, testDelivery, any(), any())
        }
        channelProvider.close()
    }

    @Test
    fun createChannel_error() {
        mockChannelConnectionError()
        assertThrows<IOException> {
            ConsumerChannelProvider(
                connectionFactory, access, queue, testDispatcher, testDelivery
            )
        }
    }

    @Test
    fun testTryAck() {
        val channel = mockChannelSuccessful()

        val channelProvider = ConsumerChannelProvider(
            connectionFactory, access, queue, testDispatcher, testDelivery
        )
        verify {
            channel.basicConsume(queue.queueName, testDelivery, any(), any())
        }

        val deliveryTag = 1L
        channelProvider.tryAck(deliveryTag)

        verify {
            channel.basicAck(deliveryTag, false)
        }
        channelProvider.close()
    }

    @Test
    fun testTryAck_error() {
        val channel = mockChannelSuccessful()

        val channelProvider = ConsumerChannelProvider(
            connectionFactory, access, queue, testDispatcher, testDelivery
        )
        verify {
            channel.basicConsume(queue.queueName, testDelivery, any(), any())
        }

        every { channel.basicAck(any(), false) }.throws(IOException("testError"))

        val deliveryTag = 1L
        assertThrows<IOException> {
            channelProvider.tryAck(deliveryTag)
        }

        verify {
            channel.basicAck(deliveryTag, false)
        }
        channelProvider.close()
    }

    @Test
    fun testTryNack() {
        val channel = mockChannelSuccessful()

        val channelProvider = ConsumerChannelProvider(
            connectionFactory, access, queue, testDispatcher, testDelivery
        )
        verify {
            channel.basicConsume(queue.queueName, testDelivery, any(), any())
        }

        val deliveryTag = 1L
        channelProvider.tryNack(deliveryTag)

        verify {
            channel.basicNack(deliveryTag, false, true)
        }
    }

    @Test
    fun testTryNack_error() {
        val channel = mockChannelSuccessful()

        val channelProvider = ConsumerChannelProvider(
            connectionFactory, access, queue, testDispatcher, testDelivery
        )
        verify {
            channel.basicConsume(queue.queueName, testDelivery, any(), any())
        }

        every { channel.basicNack(any(), false, true) }.throws(IOException("testError"))

        val deliveryTag = 1L
        assertThrows<IOException> {
            channelProvider.tryNack(deliveryTag)
        }

        verify {
            channel.basicNack(deliveryTag, false, true)
        }
        channelProvider.close()
    }

    @Test
    fun testReconnectFailed() {
        val connection = mockConnection()
        val channel = mockk<Channel>(relaxed = true)
        every { connection.createChannel() }.returns(channel)
        every { channel.isOpen } returns true

        val channelProvider = ConsumerChannelProvider(
            connectionFactory, access, queue, testDispatcher, testDelivery
        )

        verify {
            channel.basicConsume(queue.queueName, testDelivery, any(), any())
        }

        every { connection.isOpen } returns false
        every { channel.isOpen } returns false
        every { connectionFactory.createConnection() }.throws(IOException("testError"))

        testDispatcher.advanceTimeBy(20001)

        verify(atLeast = 3, atMost = 5) {
            connectionFactory.createConnection()
        }
        channelProvider.close()
    }

    @Test
    fun testChannelIsOpen() {
        val channel = mockChannelSuccessful()
        val channelProvider = ConsumerChannelProvider(
            connectionFactory, access, queue, testDispatcher, testDelivery
        )
        verify {
            channel.basicConsume(queue.queueName, testDelivery, any(), any())
        }

        assertThat(channelProvider.channelIsOpen()).isTrue

        every { channel.isOpen } returns false

        assertThat(channelProvider.channelIsOpen()).isFalse
    }

    @Test
    fun testRecreateChannel() {
        val connection = mockConnection()
        val channel = mockk<Channel>(relaxed = true)
        every { connection.createChannel() } returns channel
        every { channel.isOpen } returns true

        val channelProvider = ConsumerChannelProvider(
            connectionFactory, access, queue, testDispatcher, testDelivery
        )
        verify {
            channel.basicConsume(queue.queueName, testDelivery, any(), any())
        }

        every { connection.isOpen } returns false
        every { channel.isOpen } returns false

        channelProvider.recreateChannel()

        testDispatcher.advanceTimeBy(10000)

        verify(atLeast = 2) {
            connectionFactory.createConnection()
        }
    }

    @Test
    fun testClose() {
        val connection = mockConnection()
        val channel = mockk<Channel>(relaxed = true)
        every { connection.createChannel() }.returns(channel)
        every { channel.isOpen } returns true

        val channelProvider = ConsumerChannelProvider(
            connectionFactory, access, queue, testDispatcher, testDelivery
        )
        verify {
            channel.basicConsume(queue.queueName, testDelivery, any(), any())
        }
        channelProvider.close()
        verify {
            connection.close()
        }
    }

    @Test
    fun testShutDownSignalCallBack() {
        val channel = mockChannelSuccessful()
        val shutdownCallback = slot<ConsumerShutdownSignalCallback>()
        ConsumerChannelProvider(
            connectionFactory, access, queue, testDispatcher, testDelivery
        )
        verify {
            channel.basicConsume(queue.queueName, testDelivery, any(), capture(shutdownCallback))
        }
        assertThat(shutdownCallback.isCaptured).isTrue
        assertThat(shutdownCallback.isNull).isFalse
    }

    @Test
    fun testCancelCallBack() {
        val channel = mockChannelSuccessful()
        val cancelCallback = slot<CancelCallback>()
        ConsumerChannelProvider(
            connectionFactory, access, queue, testDispatcher, testDelivery
        )
        verify {
            channel.basicConsume(queue.queueName, testDelivery, capture(cancelCallback), any())
        }
        assertThat(cancelCallback.isCaptured).isTrue
        assertThat(cancelCallback.isNull).isFalse
    }

    private fun mockConnection(): Connection {
        val connection = mockk<Connection>(relaxed = true)
        every { connectionFactory.createConnection() }.returns(connection)
        every { connection.isOpen } returns true
        return connection
    }

    private fun mockChannelSuccessful(): Channel {

        val connection = mockConnection()
        val channel = mockk<Channel>(relaxed = true)
        every { connection.createChannel() }.returns(channel)
        every { channel.isOpen } returns true
        return channel
    }

    private fun mockChannelConnectionError(): Channel {
        val connection = mockConnection()
        every { connectionFactory.createConnection()
        }.throws(IOException("testError"))
        every { connection.isOpen } returns false

        val channel = mockk<Channel>(relaxed = true)
        every { connection.createChannel() }.returns(channel)
        every { channel.isOpen } returns false
        return channel
    }
}
