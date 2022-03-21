package channel

import channel.ConsumerChannelProvider
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.DeliverCallback
import connection.CachingConnectionFactory
import connection.ConnectionFactory
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.TestCoroutineDispatcher
import model.Queue
import model.RabbitMqAccess
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.cache.annotation.Caching

@ExperimentalCoroutinesApi
internal class ConsumerChannelProviderTest {

    private val connectionFactory: CachingConnectionFactory = mockk(relaxed = true)
    private val testDispatcher = TestCoroutineDispatcher()
    private val testDelivery: DeliverCallback = mockk(relaxed = true)
    private val access: RabbitMqAccess = RabbitMqAccess("rabbitmq", "rabbitmq", "localhost", 5672)
    private val queue: Queue = Queue("test", "/")

    @Test
    fun testDelivery() {
        val channel = mockChannel()

        val channelProvider = ConsumerChannelProvider(
            connectionFactory, access, queue, testDispatcher, testDelivery
        )

        verify {
            channel.basicConsume(queue.queueName, testDelivery, any(), any())
        }

        val deliveryTag = 1L
        assertThat(channelProvider.tryAck(deliveryTag)).isTrue

        verify {
            channel.basicAck(deliveryTag, false)
        }
    }

    private fun mockChannel(): Channel {

        val connection = mockk<Connection>()
        every {
            connectionFactory.createConnection()
        }.returns(connection)

        val channel = mockk<Channel>(relaxed = true)
        every {
            connection.createChannel()
        }.returns(channel)
        return channel
    }
}
