package converter

import com.fasterxml.jackson.databind.ObjectMapper
import exception.ConverterException

class JacksonConverter: Converter {

    private val objectMapper = ObjectMapper()

    override fun <T> toByteArray(value: T, type: Class<T>): ByteArray {
        try {
            return objectMapper.writeValueAsString(value).toByteArray()
        } catch (e: Throwable) {
            throw ConverterException(
                "Failed to convert value of type ${type.name} to byte array, ${e.message}"
            )
        }
    }

    override fun <T> toObject(value: ByteArray, type: Class<T>): T {
        try {
            return objectMapper.readValue(String(value), type)
        } catch (e: Throwable) {
            throw ConverterException(
                "Failed to convert byte array to value of type ${type.name}, ${e.message}"
            )
        }
    }
}
