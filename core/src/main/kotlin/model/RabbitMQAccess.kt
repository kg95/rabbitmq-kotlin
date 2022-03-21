package model

data class RabbitMqAccess(
    val username: String,
    val password: String,
    val host: String,
    val port: Int,
)
