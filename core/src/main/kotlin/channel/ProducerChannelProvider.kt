package channel

import com.rabbitmq.client.ReturnListener
import com.rabbitmq.client.Channel
import com.rabbitmq.client.MessageProperties
import connection.ConnectionProvider
import model.ConnectionProperties

internal class ProducerChannelProvider(
    connectionProperties: ConnectionProperties,
    private val returnListener: ReturnListener
) {
    private val queueName: String = connectionProperties.queueName
    private val connectionProvider: ConnectionProvider
    private var channel: Channel

    init {
        connectionProvider = ConnectionProvider(connectionProperties)
        channel = createChannel()
    }

    private fun createChannel(): Channel {
        return connectionProvider.createChannel().apply {
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
        connectionProvider.close()
    }
}
