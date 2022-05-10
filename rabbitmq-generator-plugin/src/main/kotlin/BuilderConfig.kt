package io.github.kg95.rabbitmq.generator

import java.io.Serializable

open class BuilderConfig: Serializable {
    var username: String = "guest"
    var password: String = "guest"
    var host: String = "localhost"
    var port: Int = 5672
    var customConverterClass: Class<*>? = null
    var consumerPrefetchCount: Int = 1000
    var consumerWatchDogIntervalMillis: Long = 10000
    var producerPublishAttemptCount: Int = 1
    var producerPublishAttemptDelayMillis: Long = 1000
}
