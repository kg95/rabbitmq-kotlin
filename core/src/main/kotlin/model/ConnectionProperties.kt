package model

data class ConnectionProperties(
    val username: String,
    val password: String,
    val host: String,
    val port: Int,
    val virtualHost: String
)
