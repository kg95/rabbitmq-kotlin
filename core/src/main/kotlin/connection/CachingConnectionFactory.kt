package connection

import com.rabbitmq.client.Connection
import model.ConnectionCredentials

class CachingConnectionFactory(
    rabbitMQConnectionFactory: com.rabbitmq.client.ConnectionFactory = com.rabbitmq.client.ConnectionFactory()
): ConnectionFactory(rabbitMQConnectionFactory) {

    private val connectionCache: MutableMap<ConnectionCredentials, Connection> = mutableMapOf()

    override fun createConnection(): Connection {
        return findCurrentConnection() ?: newConnection()
    }

    private fun newConnection(): Connection {
        return rabbitMQConnectionFactory.newConnection().also {
            connectionCache[ConnectionCredentials(host, port)] = it
        }
    }

    private fun findCurrentConnection(): Connection? {
        return connectionCache[
                ConnectionCredentials(
                    rabbitMQConnectionFactory.host,
                    rabbitMQConnectionFactory.port
                )
        ]
    }
}
