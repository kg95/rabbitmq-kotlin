import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.MessageProperties
import com.rabbitmq.client.ReturnListener
import converter.DefaultConverter
import exception.ConverterException
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkConstructor
import io.mockk.slot
import io.mockk.verify
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runBlockingTest
import model.ConnectionProperties
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException

@ExperimentalCoroutinesApi
class RabbitMQProducerTest {

    private val connectionProperties: ConnectionProperties = mockk(relaxed = true)
    private val converter: DefaultConverter = mockk(relaxed = true)
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
        RabbitMQProducer(connectionProperties, converter, type)

        verify {
            anyConstructed<ConnectionFactory>().username = connectionProperties.username
            anyConstructed<ConnectionFactory>().password = connectionProperties.password
            anyConstructed<ConnectionFactory>().host = connectionProperties.host
            anyConstructed<ConnectionFactory>().port = connectionProperties.port
            anyConstructed<ConnectionFactory>().virtualHost = connectionProperties.virtualHost
            anyConstructed<ConnectionFactory>().isAutomaticRecoveryEnabled = false
            anyConstructed<ConnectionFactory>().newConnection()
            connection.createChannel()
            channel.addReturnListener(any() as ReturnListener)
        }
    }

    @Test
    fun testCreation_error() {
        every { anyConstructed<ConnectionFactory>().newConnection() } throws IOException()
        assertThrows<IOException> {
            RabbitMQProducer(connectionProperties, converter, type)
        }
    }

    @Test
    fun testClose() {
        val connection = mockNewSuccessfulConnection()
        mockNewSuccessfulChannel(connection)
        val producer = RabbitMQProducer(connectionProperties, converter, type)

        producer.close()

        verify {
            connection.close()
        }
    }

    @Test
    fun testSendMessages() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val producer = RabbitMQProducer(connectionProperties, converter, type)

        val messages = listOf( "message1", "message2", "message3")
        for (message in messages) {
            every { converter.toByteArray(message, type) } returns message.toByteArray()
        }

        runBlockingTest {
            producer.sendMessages(messages)
        }

        val caughtMessages = mutableListOf<ByteArray>()
        verify(exactly = 3) {
            channel.basicPublish(
                "", any(), true, MessageProperties.PERSISTENT_BASIC, capture(caughtMessages)
            )
        }

        assertThat(caughtMessages.size).isEqualTo(3)
        for (message in caughtMessages) {
            assertThat(String(message)).isIn(messages)
        }
    }

    @Test
    fun testSendMessages_conversionError() {
        val connection = mockNewSuccessfulConnection()
        mockNewSuccessfulChannel(connection)
        val producer = RabbitMQProducer(connectionProperties, converter, type)

        val messages = listOf( "message")
        every { converter.toByteArray(messages.first(), type) } throws ConverterException("testError")

        runBlockingTest {
            assertThrows<ConverterException> {
                producer.sendMessages(messages)
            }
        }
    }

    @Test
    fun testSendMessages_rabbitMQError() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val producer = RabbitMQProducer(connectionProperties, converter, type)

        val messages = listOf( "message1", "message2", "message3")
        for (message in messages) {
            every { converter.toByteArray(message, type) } returns message.toByteArray()
        }
        every { channel.isOpen } returns false
        every { connection.isOpen } returns false
        every { anyConstructed<ConnectionFactory>().newConnection() } throws IOException()

        runBlockingTest {
            assertThrows<IOException> {
                producer.sendMessages(messages)
            }
        }
    }

    @Test
    fun testSendMessages_reconnect() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val producer = RabbitMQProducer(connectionProperties, converter, type)

        val messages = listOf( "message1")
        every { converter.toByteArray(messages.first(), type) } returns messages.first().toByteArray()
        every { channel.isOpen } returns false
        every { connection.isOpen } returns false

        val newConnection = mockNewSuccessfulConnection()
        val newChannel = mockNewSuccessfulChannel(newConnection)

        runBlockingTest {
            producer.sendMessages(messages)
        }

        val caughtMessage = slot<ByteArray>()
        verify(exactly = 1) {
            newChannel.basicPublish(
                "", any(), true, MessageProperties.PERSISTENT_BASIC, capture(caughtMessage)
            )
        }
        assertThat(String(caughtMessage.captured)).isEqualTo(messages.first())
    }

    @Test
    fun testSendMessage() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val producer = RabbitMQProducer(connectionProperties, converter, type)

        val message = "message"
        every { converter.toByteArray(message, type) } returns message.toByteArray()

        runBlockingTest {
            producer.sendMessage(message)
        }

        val caughtMessage = slot<ByteArray>()
        verify(exactly = 1) {
            channel.basicPublish(
                "", connectionProperties.queueName, true, MessageProperties.PERSISTENT_BASIC, capture(caughtMessage)
            )
        }
        assertThat(String(caughtMessage.captured)).isEqualTo(message)
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
