package io.github.kg95.rabbitmq.lib.converter

class DefaultConverter: Converter {
    override fun <T> toByteArray(value: T, type: Class<T>): ByteArray {
        if(type == String::class.java) {
            return (value as String).toByteArray()
        }
        if(type == ByteArray::class.java) {
            return value as ByteArray
        }
        val message = "${type.name} is not supported by this converter. Consider using a different" +
                "converter or implement your own."
        throw IllegalStateException(message)
    }

    override fun <T> toObject(value: ByteArray, type: Class<T>): T {
        if(type == String::class.java) {
            return String(value) as T
        }
        if(type == ByteArray::class.java) {
            return value as T
        }
        val message = "${type.name} is not supported by this converter. Consider using a different" +
                "converter or implement your own."
        throw IllegalStateException(message)
    }
}
