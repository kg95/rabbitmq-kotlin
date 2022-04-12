package exception

import java.io.IOException

class RabbitMQException(message: String, cause: Throwable?): IOException(message, cause)
