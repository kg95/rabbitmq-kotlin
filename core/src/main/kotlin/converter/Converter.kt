package converter

interface Converter {

    fun <T> toByteArray(value: T, type: Class<T>): ByteArray?

    fun <T> toObject(value: ByteArray, type: Class<T>): T?
}
