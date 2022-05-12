package io.github.kg95.rabbitmq.lib.connection

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import io.github.kg95.rabbitmq.lib.model.RabbitMqAccess

internal class ConnectionProvider(
    private val rabbitmqAccess: RabbitMqAccess,
    private val virtualHost: String
) {
    private val connectionFactory: ConnectionFactory = ConnectionFactory()
    private var connection: Connection

    init {
        connectionFactory.apply {
            username = rabbitmqAccess.username
            password = rabbitmqAccess.password
            host = rabbitmqAccess.host
            port = rabbitmqAccess.port
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
