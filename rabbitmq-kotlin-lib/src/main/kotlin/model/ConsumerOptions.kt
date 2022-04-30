package io.github.kg95.rabbitmq.lib.model

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers

private const val DEFAULT_PREFETCH_COUNT = 1000
private const val DEFAULT_WATCH_DOG_INTERVAL_MILLIS = 10000L

data class ConsumerOptions(
    val dispatcher: CoroutineDispatcher = Dispatchers.Default,
    val prefetchCount: Int = DEFAULT_PREFETCH_COUNT,
    val watchDogIntervalMillis: Long = DEFAULT_WATCH_DOG_INTERVAL_MILLIS
)