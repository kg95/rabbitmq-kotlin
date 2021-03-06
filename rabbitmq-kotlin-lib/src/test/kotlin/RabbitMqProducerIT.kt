package io.github.kg95.rabbitmq.lib

import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.ConsumerShutdownSignalCallback
import com.rabbitmq.client.DeliverCallback
import io.github.kg95.rabbitmq.lib.converter.DefaultConverter
import kotlinx.coroutines.runBlocking
import io.github.kg95.rabbitmq.lib.model.RabbitMqAccess
import io.github.kg95.rabbitmq.lib.model.Response
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

internal class RabbitMqProducerIT {

    private lateinit var channel: Channel
    private val messageBuffer: MutableList<ByteArray> = mutableListOf()
    private val rabbitmqAccess = RabbitMqAccess(
        "rabbitmq", "rabbitmq", "localhost", 5672
    )
    private val virtualHost: String = "/"
    private val queueName: String = "testQueue"
    private val publishCount = 1
    private val publishDelayMillis = 1000L

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
        val onDelivery = DeliverCallback { _, message ->
            messageBuffer.add(message.body)
        }
        val onShutDown = ConsumerShutdownSignalCallback { _, _ ->  }
        channel.basicConsume("testQueue", onDelivery, onShutDown)
    }

    @AfterEach
    fun tearDown() {
        channel.queuePurge("testQueue")
        channel.queueDelete("testQueue")
        messageBuffer.clear()
        channel.close()
    }

    @Test
    fun testSendMessages() {
        val rabbitProducer = RabbitMqProducer(
            rabbitmqAccess, virtualHost, queueName, DefaultConverter(), String::class.java,
            publishCount, publishDelayMillis
        )
        val messages = listOf("message1", "message2", "message3")
        runBlocking {
            val response = rabbitProducer.sendMessages(messages)
            assertThat(response).isInstanceOf(Response.Success::class.java)
        }

        await.atMost(1000, TimeUnit.MILLISECONDS).until {
            messageBuffer.size == 3
        }

        assertThat(messageBuffer.size).isEqualTo(3)
        for (message in messageBuffer) {
            assertThat(DefaultConverter().toObject(message, String::class.java)).isIn(messages)
        }
    }
}