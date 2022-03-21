package connection

import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory

abstract class ConnectionFactory {

    protected val rabbitMQConnectionFactory: ConnectionFactory = ConnectionFactory()

    var username: String
        get() = rabbitMQConnectionFactory.username
        set(value) {
            rabbitMQConnectionFactory.username = value
        }

    var password: String
        get() = rabbitMQConnectionFactory.password
        set(value) {
            rabbitMQConnectionFactory.password = value
        }

    var host: String
        get() = rabbitMQConnectionFactory.host
        set(value) {
            rabbitMQConnectionFactory.host = value
        }

    var port: Int
        get() = rabbitMQConnectionFactory.port
        set(value) {
            rabbitMQConnectionFactory.port = value
        }

    var virtualHost: String
        get() = rabbitMQConnectionFactory.virtualHost
        set(value) {
            rabbitMQConnectionFactory.virtualHost = value
        }

    var isAutomaticRecoveryEnabled: Boolean
        get() = rabbitMQConnectionFactory.isAutomaticRecoveryEnabled
        set(value) {
            rabbitMQConnectionFactory.isAutomaticRecoveryEnabled = value
        }

    open fun createConnection(): Connection = rabbitMQConnectionFactory.newConnection()
}
