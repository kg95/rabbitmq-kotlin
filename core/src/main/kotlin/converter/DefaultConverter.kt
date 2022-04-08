package converter

import exception.ConverterException

class DefaultConverter: Converter {
    override fun <T> toByteArray(value: T, type: Class<T>): ByteArray {
        if(type == String::class.java) {
            return (value as String).toByteArray()
        }
        if(type == ByteArray::class.java) {
            return value as ByteArray
        }
        throw ConverterException("${type.name} is not supported by this converter")
    }

    override fun <T> toObject(value: ByteArray, type: Class<T>): T {
        if(type == String::class.java) {
            return String(value) as T
        }
        if(type == ByteArray::class.java) {
            return value as T
        }
        throw ConverterException("${type.name} is not supported by this converter")
    }
}
