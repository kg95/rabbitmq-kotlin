package util

import com.rabbitmq.client.ShutdownSignalException
import exception.RabbitMQException
import java.io.IOException
import java.net.ConnectException
import java.util.concurrent.TimeoutException

internal fun convertToRabbitMQException(e: Throwable): RabbitMQException {
    return when(e) {
        is ConnectException -> {
            val message = "Failed to connect to rabbitmq message broker. Ensure that the broker " +
                    "is running and your ConnectionProperties are set correctly"
            RabbitMQException(message, e)
        }
        is TimeoutException -> RabbitMQException("TimeoutException during rabbitmq operation", e)
        is IOException -> {
            var message = "IOException during rabbitmq operation"
            when(e.cause) {
                null -> RabbitMQException(message, e)
                is ShutdownSignalException -> {
                    message += ", channel got shut down"
                    RabbitMQException(message, e.cause)
                }
                else -> {
                    message += ", unknown nested exception"
                    RabbitMQException(message, e.cause)
                }
            }
        }
        else -> RabbitMQException("Unknown exception during rabbitmq operation", e)
    }
}