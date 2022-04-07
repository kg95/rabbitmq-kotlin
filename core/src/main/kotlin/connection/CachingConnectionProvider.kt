package connection

import com.rabbitmq.client.Connection
import model.ConnectionProperties

class CachingConnectionProvider: ConnectionProvider() {
    private val connectionCache: MutableMap<ConnectionProperties, Connection> = mutableMapOf()

    override fun newConnection(connectionProperties: ConnectionProperties): Connection {
        return findConnection(connectionProperties)
            ?: super.newConnection(connectionProperties).also {
                connectionCache[connectionProperties] = it
            }
    }

    private fun findConnection(connectionProperties: ConnectionProperties) =
        connectionCache[connectionProperties]
}
