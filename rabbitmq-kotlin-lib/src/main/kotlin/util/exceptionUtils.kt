package io.github.kg95.rabbitmq.lib.util

import com.rabbitmq.client.ShutdownSignalException
import io.github.kg95.rabbitmq.lib.exception.RabbitMqException
import java.io.IOException
import java.net.ConnectException
import java.util.concurrent.TimeoutException

internal fun convertToRabbitMqException(e: Throwable): RabbitMqException {
    return when(e) {
        is ConnectException -> {
            val message = "Failed to connect to rabbitmq message broker. Ensure that the broker " +
                    "is running and your ConnectionProperties are set correctly"
            RabbitMqException(message, e)
        }
        is TimeoutException -> RabbitMqException("TimeoutException during rabbitmq operation", e)
        is IOException -> {
            var message = "IOException during rabbitmq operation"
            when(e.cause) {
                null -> RabbitMqException(message, e)
                is ShutdownSignalException -> {
                    message += ", channel got shut down"
                    RabbitMqException(message, e.cause)
                }
                else -> {
                    message += ", unknown nested exception"
                    RabbitMqException(message, e.cause)
                }
            }
        }
        else -> RabbitMqException("Unknown exception during rabbitmq operation", e)
    }
}