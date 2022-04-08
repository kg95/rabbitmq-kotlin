package converter

import com.fasterxml.jackson.databind.ObjectMapper
import exception.ConverterException
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException
import java.util.UUID

class JacksonConverterTest {

    private class TestClass(
        val testString: String = "testString",
        val testInt: Int = 1,
        val testBoolean: Boolean = true
    )

    @Test
    fun testConversion_standardTypes() {
        val converter = JacksonConverter()
        val originalString = "testString"
        val originalInt = 1
        val originalDouble = 1.0
        val originalBoolean = true

        val convertedString = converter.toByteArray(originalString, String::class.java)
        val convertedInt = converter.toByteArray(originalInt, Int::class.java)
        val convertedDouble = converter.toByteArray(originalDouble, Double::class.java)
        val convertedBoolean = converter.toByteArray(originalBoolean, Boolean::class.java)

        assertThat(converter.toObject(convertedString, String::class.java)).isEqualTo(originalString)
        assertThat(converter.toObject(convertedInt, Int::class.java)).isEqualTo(originalInt)
        assertThat(converter.toObject(convertedDouble, Double::class.java)).isEqualTo(originalDouble)
        assertThat(converter.toObject(convertedBoolean, Boolean::class.java)).isEqualTo(originalBoolean)
    }


    @Test
    fun testConversion_uuid() {
        val converter = JacksonConverter()
        val originalUUID = UUID.randomUUID()
        val convertedUUID = converter.toByteArray(originalUUID, UUID::class.java)
        assertThat(converter.toObject(convertedUUID, UUID::class.java)).isEqualTo(originalUUID)
    }

    @Test
    fun testConversion_objects() {
        val converter = JacksonConverter()
        val originalObject = TestClass()
        val convertedObject = converter.toByteArray(originalObject, TestClass::class.java)
        val returnedObject = converter.toObject(convertedObject, TestClass::class.java)
        assertThat(returnedObject.testString).isEqualTo(originalObject.testString)
        assertThat(returnedObject.testInt).isEqualTo(originalObject.testInt)
        assertThat(returnedObject.testBoolean).isEqualTo(originalObject.testBoolean)
    }

    @Test
    fun testToByteArray_notSupportedType() {
        val objectMapper = mockk<ObjectMapper>()
        every { objectMapper.writeValueAsString(any()) } throws IOException()

        val converter = JacksonConverter(objectMapper)
        assertThrows<ConverterException> {
            converter.toByteArray("testString", String::class.java)
        }
    }

    @Test
    fun testToObject_notSupportedType() {
        val objectMapper = mockk<ObjectMapper>()
        val testByteArray = "testString".toByteArray()
        every { objectMapper.readValue(testByteArray, String::class.java) } throws IOException()

        val converter = JacksonConverter(objectMapper)
        assertThrows<ConverterException>{
            converter.toObject(testByteArray, String::class.java)
        }
    }
}
