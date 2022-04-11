package model

private const val DEFAULT_PREFETCH_COUNT = 1000
private const val DEFAULT_WATCH_DOG_INTERVAL_MILLIS = 5000L

data class ConsumerChannelProperties(
    val prefetchCount: Int = DEFAULT_PREFETCH_COUNT,
    val watchDogIntervalMillis: Long = DEFAULT_WATCH_DOG_INTERVAL_MILLIS
)
