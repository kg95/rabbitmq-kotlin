package model

data class RabbitMQAccess(
    val username: String,
    val password: String,
    val host: String,
    val port: Int
)
