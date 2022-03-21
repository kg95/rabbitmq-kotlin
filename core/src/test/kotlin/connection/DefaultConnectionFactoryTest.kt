package connection

import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test

class DefaultConnectionFactoryTest {

    @Test
    fun testCreateConnection() {
        val rabbitConnectionFactory = mockk<ConnectionFactory>(relaxed = true)
        val connection = mockk<Connection>()

        every { rabbitConnectionFactory.newConnection() } returns connection

        val defaultConnectionFactory = DefaultConnectionFactory(rabbitConnectionFactory)
        val returnedConnection = defaultConnectionFactory.createConnection()

        verify {
            rabbitConnectionFactory.newConnection()
        }

        Assertions.assertThat(connection).isEqualTo(returnedConnection)
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

        val defaultConnectionFactory = DefaultConnectionFactory(rabbitConnectionFactory)
        defaultConnectionFactory.apply {
            username = "user2"
            password = "password2"
            host = "host2"
            port = 2
            virtualHost = "virtualhost2"
            isAutomaticRecoveryEnabled = false
        }

        Assertions.assertThat(rabbitConnectionFactory.username).isEqualTo("user2")
        Assertions.assertThat(rabbitConnectionFactory.password).isEqualTo("password2")
        Assertions.assertThat(rabbitConnectionFactory.host).isEqualTo("host2")
        Assertions.assertThat(rabbitConnectionFactory.port).isEqualTo(2)
        Assertions.assertThat(rabbitConnectionFactory.virtualHost).isEqualTo("virtualhost2")
        Assertions.assertThat(rabbitConnectionFactory.isAutomaticRecoveryEnabled).isEqualTo(false)
    }
}