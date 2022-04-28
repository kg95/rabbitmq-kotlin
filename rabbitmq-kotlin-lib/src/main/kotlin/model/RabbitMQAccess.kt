package model

data class RabbitMQAccess(
    val username: String = "guest",
    val password: String = "guest",
    val host: String = "localhost",
    val port: Int = 5672
)