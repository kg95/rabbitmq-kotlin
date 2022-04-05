import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.DeliverCallback
import connection.DefaultConnectionFactory
import converter.DefaultConverter
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.TestCoroutineDispatcher
import kotlinx.coroutines.test.runBlockingTest
import model.PendingRabbitMQMessage
import model.Queue
import model.RabbitMqAccess
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException

@ExperimentalCoroutinesApi
internal class RabbitMQConsumerTest {

    private val connectionFactory = mockk<DefaultConnectionFactory>(relaxed = true)
    private val dispatcher = TestCoroutineDispatcher()
    private val access = RabbitMqAccess("rabbitmq", "rabbitmq", "localhost", 5672)
    private val queue = Queue("testQueue", "/")
    private val converter = mockk<DefaultConverter>(relaxed = true)
    private val type = String::class.java

    @Test
    fun testCreation() {
        val connection = mockConnection()
        val channel = mockChannelSuccessful(connection)

        RabbitMQConsumer(
            connectionFactory, dispatcher, access, queue, converter, type
        )

        verify {
            connectionFactory.createConnection()
            connection.createChannel()
            channel.basicQos(any())
            channel.basicConsume(queue.queueName, any() as DeliverCallback, any(), any())
        }
    }

    @Test
    fun testCreation_error() {
        val connection = mockConnection()
        mockChannelFailed(connection)
        assertThrows<IOException> {
            RabbitMQConsumer(
                connectionFactory, dispatcher, access, queue, converter, type
            )
        }
    }

    @Test
    fun testClose() {
        val connection = mockConnection()
        val channel = mockChannelSuccessful(connection)

        val consumer = RabbitMQConsumer(
            connectionFactory, dispatcher, access, queue, converter, type
        )
        verify {
            connectionFactory.createConnection()
            connection.createChannel()
            channel.basicQos(any())
            channel.basicConsume(queue.queueName, any() as DeliverCallback, any(), any())
        }

        consumer.close()

        verify {
            connection.close()
        }
    }

    @Test
    fun testAckMessage() {
        val connection = mockConnection()
        val channel = mockChannelSuccessful(connection)

        val consumer = RabbitMQConsumer(
            connectionFactory, dispatcher, access, queue, converter, type
        )
        verify {
            connectionFactory.createConnection()
            connection.createChannel()
            channel.basicQos(any())
            channel.basicConsume(queue.queueName, any() as DeliverCallback, any(), any())
        }
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
        val connection = mockConnection()
        val channel = mockChannelSuccessful(connection)

        val consumer = RabbitMQConsumer(
            connectionFactory, dispatcher, access, queue, converter, type
        )
        verify {
            connectionFactory.createConnection()
            connection.createChannel()
            channel.basicQos(any())
            channel.basicConsume(queue.queueName, any() as DeliverCallback, any(), any())
        }
        val message = PendingRabbitMQMessage("message", 1L, 1)
        runBlockingTest {
            consumer.nackMessage(message)
        }

        verify {
            channel.basicNack(1, false, true)
        }
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