package channel

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ShutdownSignalException
import connection.ConnectionFactory
import model.Queue
import model.RabbitMqAccess
import util.getLogger
import java.io.IOException

abstract class AbstractChannelProvider(
    protected val connectionFactory: ConnectionFactory,
    private val rabbitMqAccess: RabbitMqAccess,
    protected val queue: Queue,
) {
    protected open val logger = getLogger(AbstractChannelProvider::class.java)

    protected lateinit var connection: Connection
    protected lateinit var channel: Channel

    init {
        connectionFactory.apply {
            username = rabbitMqAccess.username
            password = rabbitMqAccess.password
            host = rabbitMqAccess.host
            port = rabbitMqAccess.port
            virtualHost = queue.virtualHost
        }
    }

    protected fun createConnection(): Connection {
        return connectionFactory.createConnection()
    }

    protected open fun createChannel(): Channel {
        if(!connection.isOpen){
            connection = createConnection()
        }
        return connection.createChannel()
    }

    open fun close() {
        connection.close()
    }
}
