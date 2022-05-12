package io.github.kg95.rabbitmq.lib.model

data class PendingRabbitMqMessage<T>(val value: T, val deliveryTag: Long, val channelVersion: Long)
