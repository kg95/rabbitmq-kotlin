package channel

import com.rabbitmq.client.ReturnListener
import com.rabbitmq.client.Channel
import com.rabbitmq.client.MessageProperties
import com.rabbitmq.client.ShutdownListener
import connection.ConnectionProvider
import model.ConnectionProperties

internal class ProducerChannelProvider(
    connectionProperties: ConnectionProperties,
    virtualHost: String,
    private val queueName: String,
    private val returnListener: ReturnListener
): AbstractChannelProvider(connectionProperties, virtualHost) {

    init {
        channel = createChannel()
        channel.queueDeclarePassive(queueName)
    }

    override fun createChannel(): Channel {
        return super.createChannel().apply {
            val shutDownListener = ShutdownListener {
                super.closeChannel(this@apply)
            }
            addShutdownListener(shutDownListener)
            addReturnListener(returnListener)
        }
    }

    fun publish(message: ByteArray) =
            channel.basicPublish("", queueName, true, MessageProperties.PERSISTENT_BASIC, message)
}
