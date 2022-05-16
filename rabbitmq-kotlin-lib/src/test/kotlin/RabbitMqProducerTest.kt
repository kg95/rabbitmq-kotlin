package io.github.kg95.rabbitmq.lib

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.MessageProperties
import com.rabbitmq.client.ReturnListener
import io.github.kg95.rabbitmq.lib.converter.DefaultConverter
import io.github.kg95.rabbitmq.lib.exception.RabbitMqException
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkConstructor
import io.mockk.verify
import kotlinx.coroutines.ExperimentalCoroutinesApi
import io.github.kg95.rabbitmq.lib.model.RabbitMqAccess
import io.github.kg95.rabbitmq.lib.model.Response
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException
import java.net.ConnectException

@ExperimentalCoroutinesApi
class RabbitMqProducerTest {

    private val rabbitmqAccess: RabbitMqAccess = mockk(relaxed = true)
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
        RabbitMqProducer(
            rabbitmqAccess, virtualHost, queueName, converter,
            type, publishCount, publishDelayMillis
        )

        verify {
            anyConstructed<ConnectionFactory>().username = rabbitmqAccess.username
            anyConstructed<ConnectionFactory>().password = rabbitmqAccess.password
            anyConstructed<ConnectionFactory>().host = rabbitmqAccess.host
            anyConstructed<ConnectionFactory>().port = rabbitmqAccess.port
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
        val exception = assertThrows<RabbitMqException> {
            RabbitMqProducer(
                rabbitmqAccess, virtualHost, queueName, converter,
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
        val producer = RabbitMqProducer(
            rabbitmqAccess, virtualHost, queueName, converter,
            type, publishCount, publishDelayMillis
        )

        producer.close()

        verify {
            connection.close()
        }
    }

    @Test
    fun testSendMessages() = runTest(UnconfinedTestDispatcher()) {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val producer = RabbitMqProducer(
            rabbitmqAccess, virtualHost, queueName, converter,
            type, publishCount, publishDelayMillis
        )

        val messages = listOf( "message1", "message2", "message3")
        for (message in messages) {
            every { converter.toByteArray(message, type) } returns message.toByteArray()
        }

        val response = producer.sendMessages(messages)
        assertThat(response).isInstanceOf(Response.Success::class.java)
        val successfulMessages = (response as Response.Success).value
        assertThat(successfulMessages.size).isEqualTo(3)
        for (message in successfulMessages) {
            assertThat(message).isIn(messages)
        }

        verify(exactly = 3) {
            channel.basicPublish(
                "", any(), true, MessageProperties.PERSISTENT_BASIC, any()
            )
        }
    }

    @Test
    fun testSendMessages_conversionError() = runTest(UnconfinedTestDispatcher()) {
        val connection = mockNewSuccessfulConnection()
        mockNewSuccessfulChannel(connection)
        val producer = RabbitMqProducer(
            rabbitmqAccess, virtualHost, queueName, converter,
            type, publishCount, publishDelayMillis
        )

        val messages = listOf( "message")
        every { converter.toByteArray(messages.first(), type) } throws IllegalStateException()

        val response = producer.sendMessages(messages)
        assertThat(response).isInstanceOf(Response.Failure::class.java)
        assertThat(
            (response as Response.Failure).error
        ).isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun testSendMessages_rabbitMQError() = runTest(UnconfinedTestDispatcher()) {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val producer = RabbitMqProducer(
            rabbitmqAccess, virtualHost, queueName, converter,
            type, publishCount, publishDelayMillis
        )

        val messages = listOf( "message1", "message2", "message3")
        for (message in messages) {
            every { converter.toByteArray(message, type) } returns message.toByteArray()
        }
        every { channel.isOpen } returns false
        every { connection.isOpen } returns false
        every { anyConstructed<ConnectionFactory>().newConnection() } throws IOException()

        val response = producer.sendMessages(messages)
        assertThat(response).isInstanceOf(Response.Failure::class.java)
        assertThat(
            (response as Response.Failure).error
        ).isInstanceOf(RabbitMqException::class.java)
    }

    @Test
    fun testSendMessages_reconnect() = runTest(UnconfinedTestDispatcher()) {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val producer = RabbitMqProducer(
            rabbitmqAccess, virtualHost, queueName, converter,
            type, publishCount, publishDelayMillis
        )

        val messages = listOf( "message1")
        every { converter.toByteArray(messages.first(), type) } returns messages.first().toByteArray()
        every { channel.isOpen } returns false
        every { connection.isOpen } returns false

        val newConnection = mockNewSuccessfulConnection()
        val newChannel = mockNewSuccessfulChannel(newConnection)

        val response = producer.sendMessages(messages)
        assertThat(response).isInstanceOf(Response.Success::class.java)
        val successfulMessages = (response as Response.Success).value
        assertThat(successfulMessages.size).isEqualTo(1)
        assertThat(successfulMessages.first()).isEqualTo(messages.first())

        verify(exactly = 1) {
            newChannel.basicPublish(
                "", any(), true, MessageProperties.PERSISTENT_BASIC, any()
            )
        }
    }

    @Test
    fun testSendMessage() = runTest(UnconfinedTestDispatcher()) {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val producer = RabbitMqProducer(
            rabbitmqAccess, virtualHost, queueName, converter,
            type, publishCount, publishDelayMillis
        )

        val message = "message"
        every { converter.toByteArray(message, type) } returns message.toByteArray()

        val response = producer.sendMessage(message)
        assertThat(response).isInstanceOf(Response.Success::class.java)
        val successfulMessage = (response as Response.Success).value
        assertThat(successfulMessage).isEqualTo(message)

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
