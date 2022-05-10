package io.github.kg95.rabbitmq.generator

import java.io.Serializable

open class VirtualHostConfig: Serializable {
    var vhost: String = "/"
    val queueConfig: MutableList<QueueConfig> = mutableListOf()

    fun queueConfig(call: QueueConfig.() -> Unit) {
        QueueConfig().apply {
            call()
        }.also {
            queueConfig.add(it)
        }
    }

    fun queueConfig(name: String, type: Class<*>) {
        queueConfig.add(
            QueueConfig().apply {
                this.name = name
                this.type = type
            }
        )
    }
}
