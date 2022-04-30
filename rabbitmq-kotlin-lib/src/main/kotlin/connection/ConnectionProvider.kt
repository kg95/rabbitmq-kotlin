package io.github.kg95.rabbitmq.lib.connection

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import io.github.kg95.rabbitmq.lib.model.RabbitMQAccess

internal class ConnectionProvider(
    private val rabbitMQAccess: RabbitMQAccess,
    private val virtualHost: String
) {
    private val connectionFactory: ConnectionFactory = ConnectionFactory()
    private var connection: Connection

    init {
        connectionFactory.apply {
            username = rabbitMQAccess.username
            password = rabbitMQAccess.password
            host = rabbitMQAccess.host
            port = rabbitMQAccess.port
            virtualHost = this@ConnectionProvider.virtualHost
            isAutomaticRecoveryEnabled = false
        }
        connection = createConnection()
    }

    private fun createConnection() =
        connectionFactory.newConnection()

    fun createChannel(): Channel {
        if(!connection.isOpen){
            connection = createConnection()
        }
        return connection.createChannel()
    }

    fun close() {
        connection.close()
    }
}
