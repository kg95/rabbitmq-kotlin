package channel

import com.rabbitmq.client.ReturnListener
import com.rabbitmq.client.Channel
import com.rabbitmq.client.MessageProperties
import connection.ConnectionFactory
import model.Queue
import model.RabbitMqAccess

class ProducerChannelProvider(
    connectionFactory: ConnectionFactory,
    rabbitMqAccess: RabbitMqAccess,
    queue: Queue,
    private val returnListener: ReturnListener
): AbstractChannelProvider(connectionFactory, rabbitMqAccess, queue) {

    init {
        this.connectionFactory.isAutomaticRecoveryEnabled = false
        connection = createConnection()
        channel = createChannel()
    }

    override fun createChannel(): Channel {
        return super.createChannel().apply {
            addReturnListener(returnListener)
        }
    }

    fun recreateChannel() {
        if (!channel.isOpen) {
            createChannel()
        }
    }

    fun publish(message: ByteArray) =
            channel.basicPublish("", queue.queueName, true, MessageProperties.PERSISTENT_BASIC, message)
}
