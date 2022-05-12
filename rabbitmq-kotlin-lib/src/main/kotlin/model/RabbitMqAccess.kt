package io.github.kg95.rabbitmq.lib.model

data class RabbitMqAccess(
    val username: String = "guest",
    val password: String = "guest",
    val host: String = "localhost",
    val port: Int = 5672
)
