package channel

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.MessageProperties
import com.rabbitmq.client.ReturnListener
import connection.DefaultConnectionFactory
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import kotlinx.coroutines.isActive
import model.Queue
import model.RabbitMqAccess
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException

class ProducerChannelProviderTest {

    private val connectionFactory: DefaultConnectionFactory = mockk(relaxed = true)
    private val access: RabbitMqAccess = RabbitMqAccess("rabbitmq", "rabbitmq", "localhost", 5672)
    private val queue: Queue = Queue("test", "/")
    private val returnListener: ReturnListener = mockk(relaxed = true)

    @Test
    fun testCreateChannel() {
        val channel = mockChannelSuccessful()
        ProducerChannelProvider(
            connectionFactory, access, queue, returnListener
        )
        verify {
            channel.addReturnListener(returnListener)
        }
    }

    @Test
    fun createChannel_error() {
        mockChannelConnectionError()
        assertThrows<IOException> {
            ProducerChannelProvider(
                connectionFactory, access, queue, returnListener
            )
        }
    }

    @Test
    fun testPublish() {
        val channel = mockChannelSuccessful()
        val channelProvider = ProducerChannelProvider(
            connectionFactory, access, queue, returnListener
        )
        verify {
            channel.addReturnListener(returnListener)
        }
        val message = ByteArray(10)
        channelProvider.publish(message)

        verify {
            channel.basicPublish("", queue.queueName, true, MessageProperties.PERSISTENT_BASIC, message)
        }
    }

    @Test
    fun testPublish_error() {
        val channel = mockChannelSuccessful()
        val channelProvider = ProducerChannelProvider(
            connectionFactory, access, queue, returnListener
        )
        verify {
            channel.addReturnListener(returnListener)
        }

        every {
            channel.basicPublish("", queue.queueName, true, MessageProperties.PERSISTENT_BASIC, any())
        } throws IOException("testException")

        val message = ByteArray(10)
        assertThrows<IOException> {
            channelProvider.publish(message)
        }

        verify {
            channel.basicPublish("", queue.queueName, true, MessageProperties.PERSISTENT_BASIC, message)
        }
    }

    @Test
    fun testRecreateChannel() {
        var connection = mockConnection()
        val channel = mockk<Channel>(relaxed = true)
        every { connection.createChannel() } returns channel
        every { connection.isOpen } returns true
        every { channel.isOpen } returns true

        val channelProvider = ProducerChannelProvider(
            connectionFactory, access, queue, returnListener
        )
        verify {
            channel.addReturnListener(returnListener)
        }

        every { connection.isOpen } returns false
        every { channel.isOpen } returns false

        channelProvider.recreateChannel()

        verify(exactly = 2) { connectionFactory.createConnection() }
    }

    @Test
    fun testRecreateChannelError() {
        var connection = mockConnection()
        val channel = mockk<Channel>(relaxed = true)
        every { connection.createChannel() } returns channel
        every { connection.isOpen } returns true
        every { channel.isOpen } returns true

        val channelProvider = ProducerChannelProvider(
            connectionFactory, access, queue, returnListener
        )
        verify {
            channel.addReturnListener(returnListener)
        }

        every { connection.isOpen } returns false
        every { channel.isOpen } returns false
        every { connectionFactory.createConnection() } throws IOException("testException")

        assertThrows<IOException> {
            channelProvider.recreateChannel()
        }
    }

    @Test
    fun testClose() {
        val connection = mockConnection()
        val channel = mockk<Channel>(relaxed = true)
        every { connection.createChannel() }.returns(channel)
        every { channel.isOpen } returns true

        val channelProvider = ProducerChannelProvider(
            connectionFactory, access, queue, returnListener
        )
        verify {
            channel.addReturnListener(returnListener)
        }
        channelProvider.close()
        verify {
            connection.close()
        }
    }


    @Test
    fun testReturnCallBack() {
        val channel = mockChannelSuccessful()
        val returnListenerSlot = slot<ReturnListener>()
        ProducerChannelProvider(
            connectionFactory, access, queue, returnListener
        )
        verify {
            channel.addReturnListener(capture(returnListenerSlot))
        }
        assertThat(returnListenerSlot.isCaptured).isTrue
        assertThat(returnListenerSlot.isNull).isFalse
        assertThat(returnListenerSlot.captured).isEqualTo(returnListener)
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