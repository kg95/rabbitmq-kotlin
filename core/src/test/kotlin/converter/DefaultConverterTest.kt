package converter

import exception.ConverterException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class DefaultConverterTest {

    @Test
    fun testConversion_string() {
        val converter = DefaultConverter()
        val originalString = "testString"
        val convertedString = converter.toByteArray(originalString, String::class.java)
        assertThat(converter.toObject(convertedString, String::class.java)).isEqualTo(originalString)
    }

    @Test
    fun testConversion_bytearray() {
        val converter = DefaultConverter()
        val originalByteArray = "testArray".toByteArray()
        val convertedByteArray = converter.toByteArray(originalByteArray, ByteArray::class.java)
        assertThat(converter.toObject(convertedByteArray, ByteArray::class.java)).isEqualTo(originalByteArray)
    }

    @Test
    fun testToByteArray_notSupportedType() {
        val testBoolean = true
        assertThrows<ConverterException> {
            DefaultConverter().toByteArray(testBoolean, Boolean::class.java)
        }
    }

    @Test
    fun testToObject_notSupportedType() {
        val testByteArray = true.toString().toByteArray()
        assertThrows<ConverterException> {
            DefaultConverter().toObject(testByteArray, Boolean::class.java)
        }
    }
}