package exception

class RabbitMQException(message: String, cause: Throwable?): RuntimeException(message, cause)
