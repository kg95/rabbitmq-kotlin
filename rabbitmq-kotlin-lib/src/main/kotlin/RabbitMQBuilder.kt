package io.github.kg95.rabbitmq.lib

import io.github.kg95.rabbitmq.lib.converter.Converter
import io.github.kg95.rabbitmq.lib.converter.DefaultConverter
import io.github.kg95.rabbitmq.lib.model.ConsumerOptions
import io.github.kg95.rabbitmq.lib.model.ProducerOptions
import io.github.kg95.rabbitmq.lib.model.RabbitMQAccess

class RabbitMQBuilder(
    var rabbitMQAccess: RabbitMQAccess = RabbitMQAccess(),
    var converter: Converter = DefaultConverter(),
    var consumerOptions: ConsumerOptions = ConsumerOptions(),
    var producerOptions: ProducerOptions = ProducerOptions()
) {
    fun <T: Any> consumer(
        virtualHost: String,
        queueName: String,
        type: Class<T>,
    ): RabbitMQConsumer<T> {
        return RabbitMQConsumer(
            rabbitMQAccess,
            virtualHost,
            queueName,
            consumerOptions.dispatcher,
            converter,
            type,
            consumerOptions.prefetchCount,
            consumerOptions.watchDogIntervalMillis
        )
    }

    fun <T: Any> producer(
        virtualHost: String,
        queueName: String,
        type: Class<T>
    ): RabbitMQProducer<T> {
        return RabbitMQProducer(
            rabbitMQAccess,
            virtualHost,
            queueName,
            converter,
            type,
            producerOptions.publishAttemptCount,
            producerOptions.publishAttemptDelayMillis
        )
    }
}
