package io.github.kg95.rabbitmq.lib

import io.github.kg95.rabbitmq.lib.converter.Converter
import io.github.kg95.rabbitmq.lib.converter.DefaultConverter
import io.github.kg95.rabbitmq.lib.model.ConsumerOptions
import io.github.kg95.rabbitmq.lib.model.ProducerOptions
import io.github.kg95.rabbitmq.lib.model.RabbitMqAccess

class RabbitMqBuilder(
    var rabbitmqAccess: RabbitMqAccess = RabbitMqAccess(),
    var converter: Converter = DefaultConverter(),
    var consumerOptions: ConsumerOptions = ConsumerOptions(),
    var producerOptions: ProducerOptions = ProducerOptions()
) {
    fun <T: Any> consumer(
        virtualHost: String,
        queueName: String,
        type: Class<T>,
    ): RabbitMqConsumer<T> {
        return RabbitMqConsumer(
            rabbitmqAccess,
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
    ): RabbitMqProducer<T> {
        return RabbitMqProducer(
            rabbitmqAccess,
            virtualHost,
            queueName,
            converter,
            type,
            producerOptions.publishAttemptCount,
            producerOptions.publishAttemptDelayMillis
        )
    }
}
