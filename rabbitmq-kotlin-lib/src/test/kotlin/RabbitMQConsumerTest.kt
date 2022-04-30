package io.github.kg95.rabbitmq.lib

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.ShutdownSignalException
import io.github.kg95.rabbitmq.lib.converter.DefaultConverter
import io.github.kg95.rabbitmq.lib.exception.RabbitMQException
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkConstructor
import io.mockk.verify
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.test.TestCoroutineDispatcher
import kotlinx.coroutines.test.runBlockingTest
import io.github.kg95.rabbitmq.lib.model.RabbitMQAccess
import io.github.kg95.rabbitmq.lib.model.PendingRabbitMQMessage
import io.github.kg95.rabbitmq.lib.model.Response
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException
import java.net.ConnectException

@ExperimentalCoroutinesApi
@ObsoleteCoroutinesApi
internal class RabbitMQConsumerTest {

    private val rabbitMQAccess: RabbitMQAccess = mockk(relaxed = true)
    private val virtualHost: String = "/"
    private val queueName: String = "testQueue"
    private val dispatcher = TestCoroutineDispatcher()
    private val converter = mockk<DefaultConverter>(relaxed = true)
    private val type = String::class.java
    private val prefetchCount = 1000
    private val watchDogIntervalMillis = 10000L

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
        RabbitMQConsumer(
            rabbitMQAccess, virtualHost, queueName, dispatcher,
            converter, type, prefetchCount, watchDogIntervalMillis
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
            channel.basicQos(prefetchCount)
            channel.basicConsume(queueName, any() as DeliverCallback, any(), any())
        }
    }

    @Test
    fun testCreation_connectionError() {
        every { anyConstructed<ConnectionFactory>().newConnection() } throws ConnectException()
        val exception = assertThrows<RabbitMQException> {
            RabbitMQConsumer(
                rabbitMQAccess, virtualHost, queueName, dispatcher, converter,
                type, prefetchCount, watchDogIntervalMillis
            )
        }
        val message = "Failed to connect to rabbitmq message broker. Ensure that the broker " +
                "is running and your ConnectionProperties are set correctly"
        assertThat(exception.message).isEqualTo(message)
        assertThat(exception.cause).isInstanceOf(ConnectException::class.java)
    }

    @Test
    fun testInitialization_queueDoesNotExist() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        every {
            channel.basicConsume(queueName, any() as DeliverCallback, any(), any())
        } throws IOException(null, ShutdownSignalException(false, false, null, null))
        val exception = assertThrows<RabbitMQException> {
            RabbitMQConsumer(
                rabbitMQAccess, virtualHost, queueName, dispatcher, converter,
                type, prefetchCount, watchDogIntervalMillis
            )
        }
        assertThat(exception.message).isEqualTo("IOException during rabbitmq operation, channel got shut down")
        assertThat(exception.cause).isInstanceOf(ShutdownSignalException::class.java)
    }

    @Test
    fun testClose() {
        val connection = mockNewSuccessfulConnection()
        mockNewSuccessfulChannel(connection)
        val consumer = RabbitMQConsumer(
            rabbitMQAccess, virtualHost, queueName, dispatcher, converter,
            type, prefetchCount, watchDogIntervalMillis
        )

        consumer.close()

        verify {
            connection.close()
        }
    }

    @Test
    fun testAckMessage() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val consumer = RabbitMQConsumer(
            rabbitMQAccess, virtualHost, queueName, dispatcher, converter,
            type, prefetchCount, watchDogIntervalMillis
        )

        val message = PendingRabbitMQMessage("message", 1L, 1L)
        runBlockingTest {
            val response = consumer.ackMessage(message)
            assertThat(response).isInstanceOf(Response.Success::class.java)
            assertThat((response as Response.Success).value).isEqualTo(message)
        }

        verify {
            channel.basicAck(1, false)
        }
    }

    @Test
    fun testAckMessage_error() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val consumer = RabbitMQConsumer(
            rabbitMQAccess, virtualHost, queueName, dispatcher, converter,
            type, prefetchCount, watchDogIntervalMillis
        )

        every { channel.basicAck(any(), any()) } throws IOException()

        val message = PendingRabbitMQMessage("message", 1L, 1L)
        runBlockingTest {
            val response = consumer.ackMessage(message)
            assertThat(response).isInstanceOf(Response.Failure::class.java)
            assertThat((response as Response.Failure).error).isInstanceOf(IOException::class.java)
        }

        verify {
            channel.basicAck(1, false)
        }
    }

    @Test
    fun testNackMessage() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val consumer = RabbitMQConsumer(
            rabbitMQAccess, virtualHost, queueName, dispatcher, converter,
            type, prefetchCount, watchDogIntervalMillis
        )

        val message = PendingRabbitMQMessage("message", 1L, 1L)
        runBlockingTest {
            val response = consumer.nackMessage(message)
            assertThat(response).isInstanceOf(Response.Success::class.java)
            assertThat((response as Response.Success).value).isEqualTo(message)
        }

        verify {
            channel.basicNack(1, false, true)
        }
    }

    @Test
    fun testNackMessage_error() {
        val connection = mockNewSuccessfulConnection()
        val channel = mockNewSuccessfulChannel(connection)
        val consumer = RabbitMQConsumer(
            rabbitMQAccess, virtualHost, queueName, dispatcher, converter,
            type, prefetchCount, watchDogIntervalMillis
        )
        every { channel.basicNack(any(), any(), any()) } throws IOException()

        val message = PendingRabbitMQMessage("message", 1L, 1L)
        runBlockingTest {
            val response = consumer.nackMessage(message)
            assertThat(response).isInstanceOf(Response.Failure::class.java)
            assertThat((response as Response.Failure).error).isInstanceOf(IOException::class.java)
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