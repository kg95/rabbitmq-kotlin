package io.github.kg95.rabbitmq.lib

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.MessageProperties
import com.rabbitmq.client.ReturnListener
import io.github.kg95.rabbitmq.lib.converter.DefaultConverter
import io.github.kg95.rabbitmq.lib.exception.RabbitMQException
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkConstructor
import io.mockk.verify
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runBlockingTest
import io.github.kg95.rabbitmq.lib.model.RabbitMQAccess
import io.github.kg95.rabbitmq.lib.model.Response
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException
import java.net.ConnectException

@ExperimentalCoroutinesApi
class RabbitMQProducerTest {

    private val rabbitMQAccess: RabbitMQAccess = mockk(relaxed = true)
    private val virtualHost: String = "/"
    private val queueName: String = "testQueue"
    private val converter: DefaultConverter = mockk(relaxed = true)
    private val type = String::class.java
    private val publishCount = 1
    private val publishDelayMillis = 1000L

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
        RabbitMQProducer(
            rabbitMQAccess, virtualHost, queueName, converter,
            type, publishCount, publishDelayMillis
        )

        verify {
            anyConstructed<ConnectionFactory>().username = rabbitMQAccess.username
            anyConstructed<ConnectionFactory>().password = rabbitMQAccess.password
            anyConstructed<ConnectionFactory>().host = rabbitMQAccess.host
            anyConstructed<ConnectionFactory>().port = rabbitMQAccess.port
            anyConstructed<ConnectionFactory>().virtualHost = virtualHost
            anyConstructed<ConnectionFactory>().isAutomaticRecoveryEnabled = false
            anyConstructed<ConnectionFactory>().newConnection()
            connection.createChannel()
            channel.addReturnListener(any() as ReturnListener)
        }
    }

    @Test
    fun testCreation_connectionError() {
        every { anyConstructed<ConnectionFactory>().newConnection() } throws ConnectException()
        val exception = assertThrows<RabbitMQException> {
            RabbitMQProducer(
                rabbitMQAccess, virtualHost, queueName, converter,
                type, publishCount, publishDelayMillis
            )
        }
        val message = "Failed to connect to rabbitmq message broker. Ensure that the broker " +
                "is running and your ConnectionProperties are set correctly"
        assertThat(exception.message).isEqualTo(message)
        assertThat(exception.cause).isInstanceOf(ConnectException::class.java)
    }

    @Test
    fun testClose() {
        val connection = mockNewSuccessfulConnection()
        mockNewSuccessfulChannel(connection)
        val producer = RabbitMQProducer(
            rabbitMQAccess, virtualHost, queueName, converter,
            type, publishCount, publishDelayMillis
        )

        producer.close()

        verify {
            connection.close()
        }
    }

    @Test
    fun testSendMessages() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val producer = RabbitMQProducer(
            rabbitMQAccess, virtualHost, queueName, converter,
            type, publishCount, publishDelayMillis
        )

        val messages = listOf( "message1", "message2", "message3")
        for (message in messages) {
            every { converter.toByteArray(message, type) } returns message.toByteArray()
        }

        runBlockingTest {
            val response = producer.sendMessages(messages)
            assertThat(response).isInstanceOf(Response.Success::class.java)
            val successfulMessages = (response as Response.Success).value
            assertThat(successfulMessages.size).isEqualTo(3)
            for (message in successfulMessages) {
                assertThat(message).isIn(messages)
            }
        }

        verify(exactly = 3) {
            channel.basicPublish(
                "", any(), true, MessageProperties.PERSISTENT_BASIC, any()
            )
        }
    }

    @Test
    fun testSendMessages_conversionError() {
        val connection = mockNewSuccessfulConnection()
        mockNewSuccessfulChannel(connection)
        val producer = RabbitMQProducer(
            rabbitMQAccess, virtualHost, queueName, converter,
            type, publishCount, publishDelayMillis
        )

        val messages = listOf( "message")
        every { converter.toByteArray(messages.first(), type) } throws IllegalStateException()

        runBlockingTest {
            val response = producer.sendMessages(messages)
            assertThat(response).isInstanceOf(Response.Failure::class.java)
            assertThat(
                (response as Response.Failure).error
            ).isInstanceOf(IllegalStateException::class.java)
        }
    }

    @Test
    fun testSendMessages_rabbitMQError() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val producer = RabbitMQProducer(
            rabbitMQAccess, virtualHost, queueName, converter,
            type, publishCount, publishDelayMillis
        )

        val messages = listOf( "message1", "message2", "message3")
        for (message in messages) {
            every { converter.toByteArray(message, type) } returns message.toByteArray()
        }
        every { channel.isOpen } returns false
        every { connection.isOpen } returns false
        every { anyConstructed<ConnectionFactory>().newConnection() } throws IOException()

        runBlockingTest {
            val response = producer.sendMessages(messages)
            assertThat(response).isInstanceOf(Response.Failure::class.java)
            assertThat(
                (response as Response.Failure).error
            ).isInstanceOf(RabbitMQException::class.java)
        }
    }

    @Test
    fun testSendMessages_reconnect() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val producer = RabbitMQProducer(
            rabbitMQAccess, virtualHost, queueName, converter,
            type, publishCount, publishDelayMillis
        )

        val messages = listOf( "message1")
        every { converter.toByteArray(messages.first(), type) } returns messages.first().toByteArray()
        every { channel.isOpen } returns false
        every { connection.isOpen } returns false

        val newConnection = mockNewSuccessfulConnection()
        val newChannel = mockNewSuccessfulChannel(newConnection)

        runBlockingTest {
            val response = producer.sendMessages(messages)
            assertThat(response).isInstanceOf(Response.Success::class.java)
            val successfulMessages = (response as Response.Success).value
            assertThat(successfulMessages.size).isEqualTo(1)
            assertThat(successfulMessages.first()).isEqualTo(messages.first())
        }

        verify(exactly = 1) {
            newChannel.basicPublish(
                "", any(), true, MessageProperties.PERSISTENT_BASIC, any()
            )
        }
    }

    @Test
    fun testSendMessage() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val producer = RabbitMQProducer(
            rabbitMQAccess, virtualHost, queueName, converter,
            type, publishCount, publishDelayMillis
        )

        val message = "message"
        every { converter.toByteArray(message, type) } returns message.toByteArray()

        runBlockingTest {
            val response = producer.sendMessage(message)
            assertThat(response).isInstanceOf(Response.Success::class.java)
            val successfulMessage = (response as Response.Success).value
            assertThat(successfulMessage).isEqualTo(message)
        }

        verify(exactly = 1) {
            channel.basicPublish(
                "", queueName, true, MessageProperties.PERSISTENT_BASIC, any()
            )
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