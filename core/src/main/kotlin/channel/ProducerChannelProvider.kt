package channel

import com.rabbitmq.client.ReturnListener
import com.rabbitmq.client.Channel
import com.rabbitmq.client.MessageProperties
import connection.ConnectionProvider
import model.ConnectionProperties

internal class ProducerChannelProvider(
    connectionProvider: ConnectionProvider,
    connectionProperties: ConnectionProperties,
    private val queueName: String,
    private val returnListener: ReturnListener
) {
    private val connectionManager: ConnectionManager
    private var channel: Channel

    init {
        connectionManager = ConnectionManager(connectionProvider, connectionProperties)
        channel = createChannel()
    }

    private fun createChannel(): Channel {
        return connectionManager.createChannel().apply {
            addReturnListener(returnListener)
        }
    }

    fun recreateChannel() {
        if (!channel.isOpen) {
            channel = createChannel()
        }
    }

    fun publish(message: ByteArray) =
            channel.basicPublish("", queueName, true, MessageProperties.PERSISTENT_BASIC, message)

    fun close() {
        connectionManager.close()
    }
}
