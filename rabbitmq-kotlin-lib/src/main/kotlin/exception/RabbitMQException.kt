package io.github.kg95.rabbitmq.lib.exception

class RabbitMQException(message: String, cause: Throwable?): RuntimeException(message, cause)
