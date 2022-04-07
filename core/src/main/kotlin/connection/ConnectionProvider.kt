package connection

import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import model.ConnectionProperties

open class ConnectionProvider {
    protected val connectionFactory: ConnectionFactory = ConnectionFactory()

    open fun newConnection(connectionProperties: ConnectionProperties): Connection {
        connectionFactory.apply {
            username = connectionProperties.username
            password = connectionProperties.password
            host = connectionProperties.host
            port = connectionProperties.port
            virtualHost = connectionProperties.virtualHost
            isAutomaticRecoveryEnabled = connectionProperties.isAutomaticRecoveryEnabled
        }
        return connectionFactory.newConnection()
    }
}
