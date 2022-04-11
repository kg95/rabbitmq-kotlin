import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.MessageProperties
import converter.DefaultConverter
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.runBlocking
import model.ConnectionProperties
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

@ObsoleteCoroutinesApi
internal class RabbitMQConsumerIT {

    private lateinit var channel: Channel
    private val connectionProperties = ConnectionProperties(
        "rabbitmq", "rabbitmq", "localhost", 5672, "/", "testQueue"
    )

    @BeforeEach
    fun initialize() {
        channel = ConnectionFactory().apply {
            username = "rabbitmq"
            password = "rabbitmq"
            host = "localhost"
            port = 5672
            virtualHost = "/"
        }.newConnection().createChannel()
        channel.queueDeclare("testQueue", true, false, false, null)
    }

    @AfterEach
    fun tearDown() {
        channel.queuePurge("testQueue")
        channel.queueDelete("testQueue")
        channel.close()
    }

    @Test
    fun testCollectNextMessages() {
        val consumer = RabbitMQConsumer(
            connectionProperties, Dispatchers.Default, DefaultConverter(), String::class.java
        )

        val messages = listOf("message1", "message2", "message3")
        for (message in messages) {
            channel.basicPublish("", "testQueue", true, MessageProperties.PERSISTENT_BASIC, message.toByteArray())
        }

        runBlocking {
            val returnedMessages = consumer.collectNextMessages(1, 10)

            assertThat(returnedMessages.size).isEqualTo(3)
            for (message in returnedMessages) {
                assertThat(message.value).isIn(messages)
            }
        }
    }

    @Test
    fun testAckMessage() {
        val consumer = RabbitMQConsumer(
            connectionProperties, Dispatchers.Default, DefaultConverter(), String::class.java
        )

        val messages = listOf("message1", "message2", "message3")
        for (message in messages) {
            channel.basicPublish("", "testQueue", true, MessageProperties.PERSISTENT_BASIC, message.toByteArray())
        }

        runBlocking {
            val returnedMessages = consumer.collectNextMessages(1, 10)
            for (message in returnedMessages) {
                consumer.ackMessage(message)
            }
        }
        await.atMost(5000, TimeUnit.MILLISECONDS).until {
            channel.messageCount("testQueue") == 0L
        }
        assertThat(channel.messageCount("testQueue")).isEqualTo(0)
    }

    @Test
    fun testNackMessage() {
        val consumer = RabbitMQConsumer(
            connectionProperties, Dispatchers.Default, DefaultConverter(), String::class.java
        )

        val messages = listOf("message1", "message2", "message3")
        for (message in messages) {
            channel.basicPublish("", "testQueue", true, MessageProperties.PERSISTENT_BASIC, message.toByteArray())
        }

        runBlocking {
            val returnedMessages = consumer.collectNextMessages(1, 10)
            for (message in returnedMessages) {
                consumer.nackMessage(message)
            }
        }
        consumer.close()
        await.atMost(5000, TimeUnit.MILLISECONDS).until {
            channel.messageCount("testQueue") == 3L
        }
    }

    @Test
    fun testClose() {
        val consumer = RabbitMQConsumer(
            connectionProperties, Dispatchers.Default, DefaultConverter(), String::class.java
        )

        val messages = listOf("message1", "message2", "message3")
        for (message in messages) {
            channel.basicPublish("", "testQueue", true, MessageProperties.PERSISTENT_BASIC, message.toByteArray())
        }

        consumer.close()
        await.atMost(5000, TimeUnit.MILLISECONDS).until {
            channel.messageCount("testQueue") == 3L
        }
    }
}
