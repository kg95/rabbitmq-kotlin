package consumer

import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.MessageProperties
import converter.DefaultConverter
import converter.JacksonConverter
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.runBlocking
import model.RabbitMQAccess
import model.Response
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

@ObsoleteCoroutinesApi
internal class RabbitMQConsumerIT {

    private lateinit var channel: Channel
    private val rabbitMQAccess = RabbitMQAccess(
        "rabbitmq", "rabbitmq", "localhost", 5672,
    )
    private val virtualHost: String = "/"
    private val queueName: String = "testQueue"

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
            rabbitMQAccess, virtualHost, queueName, Dispatchers.Default,
            DefaultConverter(), String::class.java
        )

        val messages = listOf("message1", "message2", "message3")
        for (message in messages) {
            channel.basicPublish("", "testQueue", true, MessageProperties.PERSISTENT_BASIC, message.toByteArray())
        }

        runBlocking {
            val response = consumer.collectNextMessages(1000, 10)
            assertThat(response).isInstanceOf(Response.Success::class.java)

            val returnedMessages = (response as Response.Success).value
            assertThat(returnedMessages.size).isEqualTo(3)
            for (message in returnedMessages) {
                assertThat(message.value).isIn(messages)
            }
        }
    }

    @Test
    fun testCollectNextMessages_invalidMessage() {
        val consumer = RabbitMQConsumer(
            rabbitMQAccess, virtualHost, queueName, Dispatchers.Default,
            JacksonConverter(), Int::class.java
        )

        val invalidMessage = false
        channel.basicPublish(
            "", "testQueue", true, MessageProperties.PERSISTENT_BASIC,
            invalidMessage.toString().toByteArray()
        )

        runBlocking {
            val response = consumer.collectNextMessages(1000, 10)
            assertThat(response).isInstanceOf(Response.Failure::class.java)
        }

        consumer.close()
        await.atMost(5000, TimeUnit.MILLISECONDS).until {
            channel.messageCount("testQueue") == 0L
        }
    }

    @Test
    fun testCollectNextMessages_partiallyInvalidMessages() {
        val consumer = RabbitMQConsumer(
            rabbitMQAccess, virtualHost, queueName, Dispatchers.Default,
            JacksonConverter(), Int::class.java
        )

        val validMessage = 1
        channel.basicPublish(
            "", "testQueue", true, MessageProperties.PERSISTENT_BASIC,
            validMessage.toString().toByteArray()
        )
        val invalidMessage = false
        channel.basicPublish(
            "", "testQueue", true, MessageProperties.PERSISTENT_BASIC,
            invalidMessage.toString().toByteArray()
        )

        runBlocking {
            val responseFailure = consumer.collectNextMessages(1000, 10)
            assertThat(responseFailure).isInstanceOf(Response.Failure::class.java)

            val responseSuccess = consumer.collectNextMessages(1000, 10)
            assertThat(responseSuccess).isInstanceOf(Response.Success::class.java)

            val pendingList = (responseSuccess as Response.Success).value
            assertThat(pendingList.size).isEqualTo(1)
            assertThat(pendingList.first().value).isEqualTo(validMessage)
            consumer.ackMessage(pendingList.first())
        }

        consumer.close()
        await.atMost(5000, TimeUnit.MILLISECONDS).until {
            channel.messageCount("testQueue") == 0L
        }
    }

    @Test
    fun testAckMessage() {
        val consumer = RabbitMQConsumer(
            rabbitMQAccess, virtualHost, queueName, Dispatchers.Default,
            DefaultConverter(), String::class.java
        )

        val messages = listOf("message1", "message2", "message3")
        for (message in messages) {
            channel.basicPublish("", "testQueue", true, MessageProperties.PERSISTENT_BASIC, message.toByteArray())
        }

        runBlocking {
            val returnedMessages = consumer.collectNextMessages(1000, 10).let {
                (it as Response.Success).value
            }
            for (message in returnedMessages) {
                val response = consumer.ackMessage(message)
                assertThat(response).isInstanceOf(Response.Success::class.java)
            }
        }
        consumer.close()
        await.atMost(5000, TimeUnit.MILLISECONDS).until {
            channel.messageCount("testQueue") == 0L
        }
    }

    @Test
    fun testNackMessage() {
        val consumer = RabbitMQConsumer(
            rabbitMQAccess, virtualHost, queueName, Dispatchers.Default,
            DefaultConverter(), String::class.java
        )

        val messages = listOf("message1", "message2", "message3")
        for (message in messages) {
            channel.basicPublish("", "testQueue", true, MessageProperties.PERSISTENT_BASIC, message.toByteArray())
        }

        runBlocking {
            val returnedMessages = consumer.collectNextMessages(1000, 10).let {
                (it as Response.Success).value
            }
            for (message in returnedMessages) {
                val response = consumer.nackMessage(message)
                assertThat(response).isInstanceOf(Response.Success::class.java)
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
            rabbitMQAccess, virtualHost, queueName, Dispatchers.Default,
            DefaultConverter(), String::class.java
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
