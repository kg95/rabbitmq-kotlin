package exception

import java.io.IOException

class RabbitMQException(message: String): IOException(message)
