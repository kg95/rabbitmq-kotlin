package builder

import consumer.RabbitMQConsumer
import converter.Converter
import converter.DefaultConverter
import model.ConsumerOptions
import model.ProducerOptions
import model.RabbitMQAccess
import producer.RabbitMQProducer

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
