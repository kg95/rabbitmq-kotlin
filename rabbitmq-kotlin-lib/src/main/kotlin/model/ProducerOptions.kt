package io.github.kg95.rabbitmq.lib.model

private const val DEFAULT_PUBLISH_ATTEMPT_COUNT = 1
private const val DEFAULT_PUBLISH_ATTEMPT_DELAY_MILLIS = 1000L

data class ProducerOptions(
    val publishAttemptCount: Int = DEFAULT_PUBLISH_ATTEMPT_COUNT,
    val publishAttemptDelayMillis: Long = DEFAULT_PUBLISH_ATTEMPT_DELAY_MILLIS
)
