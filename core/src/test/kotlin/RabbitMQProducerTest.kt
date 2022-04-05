import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.MessageProperties
import com.rabbitmq.client.ReturnListener
import connection.DefaultConnectionFactory
import converter.DefaultConverter
import exception.ConverterException
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runBlockingTest
import model.Queue
import model.RabbitMqAccess
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException

@ExperimentalCoroutinesApi
class RabbitMQProducerTest {

    private val connectionFactory = mockk<DefaultConnectionFactory>(relaxed = true)
    private val access = RabbitMqAccess("rabbitmq", "rabbitmq", "localhost", 5672)
    private val queue = Queue("testQueue", "/")
    private val converter = mockk<DefaultConverter>(relaxed = true)
    private val type = String::class.java

    @Test
    fun testCreation() {
        val connection = mockConnection()
        val channel = mockChannelSuccessful(connection)

        RabbitMQProducer(
            connectionFactory, access, queue, converter, type
        )

        verify {
            connectionFactory.createConnection()
            connection.createChannel()
            channel.addReturnListener(any() as ReturnListener)
        }
    }

    @Test
    fun testCreation_error() {
        val connection = mockConnection()
        mockChannelFailed(connection)
        assertThrows<IOException> {
            RabbitMQProducer(
                connectionFactory, access, queue, converter, type
            )
        }
    }

    @Test
    fun testClose() {
        val connection = mockConnection()
        val channel = mockChannelSuccessful(connection)

        val producer = RabbitMQProducer(
            connectionFactory, access, queue, converter, type
        )
        verify {
            connectionFactory.createConnection()
            connection.createChannel()
            channel.addReturnListener(any() as ReturnListener)
        }

        producer.close()

        verify {
            connection.close()
        }
    }

    @Test
    fun testSendMessages() {
        val connection = mockConnection()
        val channel = mockChannelSuccessful(connection)

        val producer = RabbitMQProducer(
            connectionFactory, access, queue, converter, type
        )
        verify {
            connectionFactory.createConnection()
            connection.createChannel()
            channel.addReturnListener(any() as ReturnListener)
        }

        val messages = listOf( "message1", "message2", "message3")
        for (message in messages) {
            every { converter.toByteArray(message, String::class.java) } returns message.toByteArray()
        }

        runBlockingTest {
            producer.sendMessages(messages)
        }

        val caughtMessages = mutableListOf<ByteArray>()
        verify(exactly = 3) {
            channel.basicPublish("", queue.queueName, true, MessageProperties.PERSISTENT_BASIC, capture(caughtMessages))
        }

        assertThat(caughtMessages.size).isEqualTo(3)
        for (message in caughtMessages) {
            assertThat(String(message)).isIn(messages)
        }
    }

    @Test
    fun testSendMessages_conversionError() {
        val connection = mockConnection()
        val channel = mockChannelSuccessful(connection)

        val producer = RabbitMQProducer(
            connectionFactory, access, queue, converter, type
        )
        verify {
            connectionFactory.createConnection()
            connection.createChannel()
            channel.addReturnListener(any() as ReturnListener)
        }

        val messages = listOf( "message1")
        every { converter.toByteArray(messages.first(), String::class.java) } throws ConverterException("testError")

        runBlockingTest {
            assertThrows<ConverterException> {
                producer.sendMessages(messages)
            }
        }
    }

    @Test
    fun testSendMessages_rabbitMQError() {
        val connection = mockConnection()
        val channel = mockChannelSuccessful(connection)

        val producer = RabbitMQProducer(
            connectionFactory, access, queue, converter, type
        )
        verify {
            connectionFactory.createConnection()
            connection.createChannel()
            channel.addReturnListener(any() as ReturnListener)
        }

        val messages = listOf( "message1", "message2", "message3")
        for (message in messages) {
            every { converter.toByteArray(message, String::class.java) } returns message.toByteArray()
        }
        every { channel.isOpen } returns false
        every { connection.isOpen } returns false
        every { connection.createChannel() } throws IOException()

        runBlockingTest {
            assertThrows<IOException> {
                producer.sendMessages(messages)
            }
        }
    }

    @Test
    fun testSendMessages_reconnect() {
        val connection = mockConnection()
        val channel = mockChannelSuccessful(connection)

        val producer = RabbitMQProducer(
            connectionFactory, access, queue, converter, type
        )
        verify {
            connectionFactory.createConnection()
            connection.createChannel()
            channel.addReturnListener(any() as ReturnListener)
        }

        val messages = listOf( "message1")
        every { converter.toByteArray(messages.first(), String::class.java) } returns messages.first().toByteArray()
        every { channel.isOpen } returns false
        every { connection.isOpen } returns false

        val newConnection = mockConnection()
        val newChannel = mockChannelSuccessful(newConnection)
        every { newConnection.createChannel() } returns newChannel

        runBlockingTest {
            producer.sendMessages(messages)
        }

        val caughtMessage = slot<ByteArray>()
        verify(exactly = 1) {
            channel.basicPublish("", queue.queueName, true, MessageProperties.PERSISTENT_BASIC, capture(caughtMessage))
        }

        assertThat(String(caughtMessage.captured)).isEqualTo(messages.first())
    }

    @Test
    fun testSendMessage() {
        val connection = mockConnection()
        val channel = mockChannelSuccessful(connection)

        val producer = RabbitMQProducer(
            connectionFactory, access, queue, converter, type
        )
        verify {
            connectionFactory.createConnection()
            connection.createChannel()
            channel.addReturnListener(any() as ReturnListener)
        }

        val message = "message"
        every { converter.toByteArray(message, String::class.java) } returns message.toByteArray()

        runBlockingTest {
            producer.sendMessage(message)
        }

        val caughtMessage = slot<ByteArray>()
        verify(exactly = 1) {
            channel.basicPublish("", queue.queueName, true, MessageProperties.PERSISTENT_BASIC, capture(caughtMessage))
        }

        assertThat(String(caughtMessage.captured)).isEqualTo(message)
    }

    private fun mockConnection(): Connection {
        val connection = mockk<Connection>(relaxed = true)
        every { connectionFactory.createConnection() } returns connection
        every { connection.isOpen } returns true
        return connection
    }

    private fun mockChannelSuccessful(connection: Connection): Channel {
        val channel = mockk<Channel>(relaxed = true)
        every { connection.createChannel() } returns channel
        every { channel.isOpen } returns true
        return channel
    }

    private fun mockChannelFailed(connection: Connection): Channel {
        val channel = mockk<Channel>(relaxed = true)
        every { connection.createChannel() } throws IOException()
        every { channel.isOpen } returns false
        every { connection.isOpen } returns false
        return channel
    }
}