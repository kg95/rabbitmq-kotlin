package io.github.kg95.rabbitmq.lib.model

sealed class Response<T> {
    data class Success<R>(val value: R): Response<R>()
    data class Failure<S>(val error: Throwable): Response<S>()
}
