package io.github.kg95.rabbitmq.lib.exception

class RabbitMqException(message: String, cause: Throwable?): RuntimeException(message, cause)
