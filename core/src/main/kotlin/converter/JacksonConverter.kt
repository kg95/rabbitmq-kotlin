package converter

import com.fasterxml.jackson.databind.ObjectMapper
import exception.ConverterException

class JacksonConverter(
    private val objectMapper: ObjectMapper = ObjectMapper()
): Converter {

    override fun <T> toByteArray(value: T, type: Class<T>): ByteArray? {
        return try {
            objectMapper.writeValueAsString(value).toByteArray()
        } catch (e: Throwable) {
            null
        }
    }

    override fun <T> toObject(value: ByteArray, type: Class<T>): T? {
        return try {
            return objectMapper.readValue(String(value), type)
        } catch (e: Throwable) {
            null
        }
    }
}
