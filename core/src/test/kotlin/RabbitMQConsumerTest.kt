import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import converter.DefaultConverter
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkConstructor
import io.mockk.verify
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.test.TestCoroutineDispatcher
import kotlinx.coroutines.test.runBlockingTest
import model.ConnectionProperties
import model.PendingRabbitMQMessage
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException

@ExperimentalCoroutinesApi
@ObsoleteCoroutinesApi
internal class RabbitMQConsumerTest {

    private val connectionProperties: ConnectionProperties = mockk(relaxed = true)
    private val queueName: String = "testQueue"
    private val dispatcher = TestCoroutineDispatcher()
    private val converter = mockk<DefaultConverter>(relaxed = true)
    private val type = String::class.java

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
        RabbitMQConsumer(connectionProperties, queueName, dispatcher, converter, type)

        verify {
            anyConstructed<ConnectionFactory>().username = connectionProperties.username
            anyConstructed<ConnectionFactory>().password = connectionProperties.password
            anyConstructed<ConnectionFactory>().host = connectionProperties.host
            anyConstructed<ConnectionFactory>().port = connectionProperties.port
            anyConstructed<ConnectionFactory>().virtualHost = connectionProperties.virtualHost
            anyConstructed<ConnectionFactory>().isAutomaticRecoveryEnabled = false
            anyConstructed<ConnectionFactory>().newConnection()
            connection.createChannel()
            channel.basicQos(any())
            channel.basicConsume(queueName, any() as DeliverCallback, any(), any())
        }
    }

    @Test
    fun testCreation_error() {
        every { anyConstructed<ConnectionFactory>().newConnection() } throws IOException()
        assertThrows<IOException> {
            RabbitMQConsumer(connectionProperties, queueName, dispatcher, converter, type)
        }
    }

    @Test
    fun testClose() {
        val connection = mockNewSuccessfulConnection()
        mockNewSuccessfulChannel(connection)
        val consumer = RabbitMQConsumer(connectionProperties, queueName, dispatcher, converter, type)

        consumer.close()

        verify {
            connection.close()
        }
    }

    @Test
    fun testAckMessage() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val consumer = RabbitMQConsumer(connectionProperties, queueName, dispatcher, converter, type)

        val message = PendingRabbitMQMessage("message", 1L, 1)
        runBlockingTest {
            consumer.ackMessage(message)
        }

        verify {
            channel.basicAck(1, false)
        }
    }

    @Test
    fun testNackMessage() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val consumer = RabbitMQConsumer(connectionProperties, queueName, dispatcher, converter, type)

        val message = PendingRabbitMQMessage("message", 1L, 1)
        runBlockingTest {
            consumer.nackMessage(message)
        }

        verify {
            channel.basicNack(1, false, true)
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
