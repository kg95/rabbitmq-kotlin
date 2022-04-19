package connection

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkConstructor
import io.mockk.verify
import model.RabbitMQAccess
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException
import java.net.ConnectException

internal class ConnectionProviderTest {
    private val rabbitMQAccess: RabbitMQAccess = mockk(relaxed = true)
    private val virtualHost: String = "/"
    private val connection: Connection = mockk(relaxed = true)
    private val channel: Channel = mockk(relaxed = true)

    @BeforeEach
    private fun init() {
        mockkConstructor(ConnectionFactory::class)
        every {
            anyConstructed<ConnectionFactory>().newConnection()
        }.returns(connection)
        every { connection.createChannel() } returns channel
    }

    @AfterEach
    private fun shutdown() {
        clearAllMocks()
    }

    @Test
    fun testInitialization() {
        mockSuccessfulConnection()
        ConnectionProvider(rabbitMQAccess, virtualHost)
        verify {
            anyConstructed<ConnectionFactory>().newConnection()
            anyConstructed<ConnectionFactory>().username = rabbitMQAccess.username
            anyConstructed<ConnectionFactory>().password = rabbitMQAccess.password
            anyConstructed<ConnectionFactory>().host = rabbitMQAccess.host
            anyConstructed<ConnectionFactory>().port = rabbitMQAccess.port
            anyConstructed<ConnectionFactory>().virtualHost = virtualHost
            anyConstructed<ConnectionFactory>().isAutomaticRecoveryEnabled = false
        }
    }

    @Test
    fun testInitialization_connectionError() {
        mockFailedConnection()
        assertThrows<ConnectException> {
            ConnectionProvider(rabbitMQAccess, virtualHost)
        }
    }

    @Test
    fun testCreateChannel() {
        mockSuccessfulConnection()
        val connectionProvider = ConnectionProvider(
            rabbitMQAccess, virtualHost
        )
        val returnedChannel = connectionProvider.createChannel()
        verify {
            anyConstructed<ConnectionFactory>().newConnection()
            connection.createChannel()
        }
        assertThat(returnedChannel).isEqualTo(channel)
    }

    @Test
    fun testCreateChannel_error() {
        mockSuccessfulConnection()
        val connectionProvider = ConnectionProvider(
            rabbitMQAccess, virtualHost
        )
        verify { anyConstructed<ConnectionFactory>().newConnection() }

        every { connection.isOpen } returns false
        every { anyConstructed<ConnectionFactory>().newConnection() } throws IOException()

        assertThrows<IOException> { connectionProvider.createChannel() }
    }

    @Test
    fun testClose() {
        mockSuccessfulConnection()
        val connectionProvider = ConnectionProvider(
            rabbitMQAccess, virtualHost
        )
        verify { anyConstructed<ConnectionFactory>().newConnection() }

        connectionProvider.close()

        verify { connection.close() }
    }

    private fun mockSuccessfulConnection() {
        mockkConstructor(ConnectionFactory::class)
        every {
            anyConstructed<ConnectionFactory>().newConnection()
        }.returns(connection)
        every { connection.isOpen } returns true
        every { connection.createChannel() } returns channel
        every { channel.isOpen } returns true
    }

    private fun mockFailedConnection() {
        mockkConstructor(ConnectionFactory::class)
        every {
            anyConstructed<ConnectionFactory>().newConnection()
        } throws ConnectException()
    }
}