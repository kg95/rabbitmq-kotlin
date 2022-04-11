package model

private const val DEFAULT_PUBLISH_ATTEMPT_COUNT = 1
private const val DEFAULT_PUBLISH_ATTEMPT_DELAY_MILLIS = 1000L

data class ProducerProperties(
    val publishAttemptCount: Int = DEFAULT_PUBLISH_ATTEMPT_COUNT,
    val publishAttemptDelayMillis: Long = DEFAULT_PUBLISH_ATTEMPT_DELAY_MILLIS,
)
