package channel

import com.rabbitmq.client.ReturnListener
import com.rabbitmq.client.Channel
import com.rabbitmq.client.MessageProperties
import com.rabbitmq.client.ShutdownListener
import connection.ConnectionProvider
import model.ConnectionProperties

internal class ProducerChannelProvider(
    connectionProperties: ConnectionProperties,
    private val queueName: String,
    private val returnListener: ReturnListener
) {
    private val connectionProvider: ConnectionProvider
    private var channel: Channel

    init {
        connectionProvider = ConnectionProvider(connectionProperties)
        channel = createChannel()
        channel.queueDeclarePassive(queueName)
    }

    private fun createChannel(): Channel {
        return connectionProvider.createChannel().apply {
            val shutDownListener = ShutdownListener {
                closeChannel(this@apply)
            }
            addShutdownListener(shutDownListener)
            addReturnListener(returnListener)
        }
    }

    private fun closeChannel(channel: Channel) {
        try {
            if(channel.isOpen) {
                channel.close()
            }
        } catch (e: Throwable) {
            return
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
