package channel

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import connection.ConnectionProvider
import model.ConnectionProperties

internal class ConnectionManager(
    private val connectionProvider: ConnectionProvider,
    private val connectionProperties: ConnectionProperties
) {
    private var connection: Connection

    init {
        connection = createConnection()
    }

    private fun createConnection() =
        connectionProvider.newConnection(connectionProperties)

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
