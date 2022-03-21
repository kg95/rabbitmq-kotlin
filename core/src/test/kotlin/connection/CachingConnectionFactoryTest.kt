package connection

import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class CachingConnectionFactoryTest {

    @Test
    fun testCreateConnection_newConnection() {
        val rabbitConnectionFactory = mockk<ConnectionFactory>(relaxed = true)
        val connection = mockk<Connection>()

        every { rabbitConnectionFactory.host } returns "localhost"
        every { rabbitConnectionFactory.port } returns 5672
        every { rabbitConnectionFactory.newConnection() } returns connection

        val cachingConnectionFactory = CachingConnectionFactory(rabbitConnectionFactory)
        val returnedConnection = cachingConnectionFactory.createConnection()

        verify {
            rabbitConnectionFactory.newConnection()
        }

        assertThat(connection).isEqualTo(returnedConnection)
    }

    @Test
    fun testCreateConnection_cached() {
        val rabbitConnectionFactory = mockk<ConnectionFactory>(relaxed = true)

        every { rabbitConnectionFactory.host } returns "localhost"
        every { rabbitConnectionFactory.port } returns 5672


        val cachingConnectionFactory = CachingConnectionFactory(rabbitConnectionFactory)
        cachingConnectionFactory.createConnection()
        cachingConnectionFactory.createConnection()

        verify(exactly = 1) {
            rabbitConnectionFactory.newConnection()
        }
    }

    @Test
    fun testCreateConnection_notCached() {
        val rabbitConnectionFactory = mockk<ConnectionFactory>(relaxed = true)

        every { rabbitConnectionFactory.host } returns "localhost"
        every { rabbitConnectionFactory.port } returns 5672

        val cachingConnectionFactory = CachingConnectionFactory(rabbitConnectionFactory)

        cachingConnectionFactory.createConnection()

        every { rabbitConnectionFactory.host } returns "someOtherHos"
        every { rabbitConnectionFactory.port } returns 8888

        cachingConnectionFactory.createConnection()

        verify(exactly = 2) {
            rabbitConnectionFactory.newConnection()
        }
    }

    @Test
    fun testSetParams() {
        val rabbitConnectionFactory = ConnectionFactory().apply {
            username = "user1"
            password = "password1"
            host = "host1"
            port = 1
            virtualHost = "virtualhost1"
            isAutomaticRecoveryEnabled = true
        }

        val cachingConnectionFactory = CachingConnectionFactory(rabbitConnectionFactory)
        cachingConnectionFactory.apply {
            username = "user2"
            password = "password2"
            host = "host2"
            port = 2
            virtualHost = "virtualhost2"
            isAutomaticRecoveryEnabled = false
        }

        assertThat(rabbitConnectionFactory.username).isEqualTo("user2")
        assertThat(rabbitConnectionFactory.password).isEqualTo("password2")
        assertThat(rabbitConnectionFactory.host).isEqualTo("host2")
        assertThat(rabbitConnectionFactory.port).isEqualTo(2)
        assertThat(rabbitConnectionFactory.virtualHost).isEqualTo("virtualhost2")
        assertThat(rabbitConnectionFactory.isAutomaticRecoveryEnabled).isEqualTo(false)
    }
}
