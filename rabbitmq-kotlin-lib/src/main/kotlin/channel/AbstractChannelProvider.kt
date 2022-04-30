package io.github.kg95.rabbitmq.lib.channel

import com.rabbitmq.client.Channel
import io.github.kg95.rabbitmq.lib.connection.ConnectionProvider
import io.github.kg95.rabbitmq.lib.model.RabbitMQAccess

internal abstract class AbstractChannelProvider(
    rabbitMQAccess: RabbitMQAccess,
    virtualHost: String
) {
    private val connectionProvider = ConnectionProvider(rabbitMQAccess, virtualHost)
    protected lateinit var channel: Channel

    protected open fun createChannel(): Channel {
        return connectionProvider.createChannel()
    }

    protected fun closeChannel(channel: Channel) {
        try {
            if(channel.isOpen) {
                channel.close()
            }
        } catch (e: Throwable) {
            return
        }
    }

    open fun recreateChannel() {
        if(!channel.isOpen) {
            channel = createChannel()
        }
    }

    fun channelIsOpen() = channel.isOpen

    open fun close() {
        connectionProvider.close()
    }
}