package channel

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.MessageProperties
import com.rabbitmq.client.ReturnListener
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkConstructor
import io.mockk.verify
import model.ConnectionProperties
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException
import java.net.ConnectException

internal class ProducerChannelProviderTest {

    private val connectionProperties: ConnectionProperties = mockk(relaxed = true)
    private val virtualHost: String = "/"
    private val queueName: String = "testQueue"
    private val returnListener: ReturnListener = mockk(relaxed = true)

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
        ProducerChannelProvider(
            connectionProperties, queueName, virtualHost, returnListener
        )
        verify {
            anyConstructed<ConnectionFactory>().username = connectionProperties.username
            anyConstructed<ConnectionFactory>().password = connectionProperties.password
            anyConstructed<ConnectionFactory>().host = connectionProperties.host
            anyConstructed<ConnectionFactory>().port = connectionProperties.port
            anyConstructed<ConnectionFactory>().virtualHost = virtualHost
            anyConstructed<ConnectionFactory>().isAutomaticRecoveryEnabled = false
            anyConstructed<ConnectionFactory>().newConnection()
            connection.createChannel()
            channel.addReturnListener(returnListener)
        }
    }

    @Test
    fun testInitialize_connectionError() {
        every { anyConstructed<ConnectionFactory>().newConnection() } throws ConnectException()
        assertThrows<ConnectException> {
            ProducerChannelProvider(connectionProperties, queueName, virtualHost, returnListener)
        }
    }

    @Test
    fun testPublish() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val channelProvider = ProducerChannelProvider(
            connectionProperties, virtualHost, queueName, returnListener
        )

        val message = ByteArray(10)
        channelProvider.publish(message)

        verify {
            channel.basicPublish("", queueName, true, MessageProperties.PERSISTENT_BASIC, message)
        }
    }

    @Test
    fun testPublish_error() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val channelProvider = ProducerChannelProvider(
            connectionProperties, virtualHost, queueName, returnListener
        )

        every {
            channel.basicPublish("", queueName, true, MessageProperties.PERSISTENT_BASIC, any())
        } throws IOException("testException")

        val message = ByteArray(10)
        assertThrows<IOException> {
            channelProvider.publish(message)
        }
    }

    @Test
    fun testRecreateChannel() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val channelProvider = ProducerChannelProvider(
            connectionProperties, virtualHost, queueName, returnListener
        )

        every { connection.isOpen } returns false
        every { channel.isOpen } returns false

        channelProvider.recreateChannel()

        verify(exactly = 2) { anyConstructed<ConnectionFactory>().newConnection() }
    }

    @Test
    fun testRecreateChannelError() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val channelProvider = ProducerChannelProvider(
            connectionProperties, virtualHost, queueName, returnListener
        )

        every { connection.isOpen } returns false
        every { channel.isOpen } returns false
        every { anyConstructed<ConnectionFactory>().newConnection() } throws IOException("testException")

        assertThrows<IOException> {
            channelProvider.recreateChannel()
        }
    }

    @Test
    fun testClose() {
        val connection = mockNewSuccessfulConnection()
        mockNewSuccessfulChannel(connection)
        val channelProvider = ProducerChannelProvider(
            connectionProperties, virtualHost, queueName, returnListener
        )

        channelProvider.close()
        verify {
            connection.close()
        }
    }


    @Test
    fun testReturnCallBack() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        ProducerChannelProvider(connectionProperties, virtualHost, queueName, returnListener)
        verify {
            channel.addReturnListener(returnListener)
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
