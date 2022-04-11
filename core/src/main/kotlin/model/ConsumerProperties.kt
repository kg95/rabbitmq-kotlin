package model

private const val DEFAULT_ACK_ATTEMPT_COUNT = 1
private const val DEFAULT_ACK_ATTEMPT_DELAY_MILLIS = 1000L

data class ConsumerProperties(
    val ackAttemptCount: Int = DEFAULT_ACK_ATTEMPT_COUNT,
    val ackAttemptDelayMillis: Long = DEFAULT_ACK_ATTEMPT_DELAY_MILLIS,
)
