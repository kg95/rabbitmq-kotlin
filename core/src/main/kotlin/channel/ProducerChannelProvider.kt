package channel

import com.rabbitmq.client.ReturnListener
import exception.RabbitMqMessageReturnedException
import java.io.IOException
import com.rabbitmq.client.Channel
import com.rabbitmq.client.MessageProperties
import com.rabbitmq.client.ShutdownListener
import com.rabbitmq.client.ShutdownSignalException
import connection.ConnectionFactory
import model.Queue
import model.RabbitMqAccess
import org.slf4j.Logger
import util.getLogger

class ProducerChannelProvider(
    connectionFactory: ConnectionFactory,
    rabbitMqAccess: RabbitMqAccess,
    queue: Queue,
): AbstractChannelProvider(connectionFactory, rabbitMqAccess, queue) {

    override val logger: Logger = getLogger(ProducerChannelProvider::class.java)

    init {
        this.connectionFactory.isAutomaticRecoveryEnabled = false
        connection = createConnection()
        channel = createChannel()
    }

    override fun createChannel(): Channel {
        return super.createChannel().apply {
            val onReturn = ReturnListener { replyCode, replyText, exchange, routingKey, _, _ ->
                val message =
                    "Message returned from exchange: $exchange with routingKey: $routingKey ," +
                            " replyCode: $replyCode replyText: $replyText, attempting to republish"
                logger.error(message, RabbitMqMessageReturnedException(message))
            }
            val onShutDown = ShutdownListener {
                logger.warn("Channel was shut down: {}", it.message)
            }
            addReturnListener(onReturn)
            addShutdownListener(onShutDown)
        }
    }

    fun publish(message: ByteArray) {
        try {
            if(!channel.isOpen){
                channel = createChannel()
            }
            channel.basicPublish("", queue.queueName, true, MessageProperties.PERSISTENT_BASIC, message)
        } catch (e: IOException) {
            logger.error("Error during publish: {}", e.message)
        } catch (e: ShutdownSignalException) {
            logger.warn("Channel unexpectedly shut down during publish: {}", e.message)
        }
    }
}
