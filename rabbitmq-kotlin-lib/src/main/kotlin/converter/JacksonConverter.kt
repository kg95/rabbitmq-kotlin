package io.github.kg95.rabbitmq.lib.converter

import com.fasterxml.jackson.databind.ObjectMapper

class JacksonConverter(
    private val objectMapper: ObjectMapper = ObjectMapper()
): Converter {

    override fun <T> toByteArray(value: T, type: Class<T>): ByteArray {
        return try {
            objectMapper.writeValueAsString(value).toByteArray()
        } catch (e: Throwable) {
            throw RuntimeException(
                "Failed to convert value of type ${type.name} to byte array", e
            )
        }
    }

    override fun <T> toObject(value: ByteArray, type: Class<T>): T {
        return try {
            objectMapper.readValue(String(value), type)
        } catch (e: Throwable) {
            throw RuntimeException(
                "Failed to convert byte array to value of type ${type.name}", e
            )
        }
    }
}
