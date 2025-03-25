@file:Suppress("unused")
package me.hchome.kactor

import kotlin.reflect.KClass
import kotlin.reflect.KType

/**
 * Represents an attribute map of an [Actor] in the actor scope. From Jetbrains Ktor Project
 */
interface Attributes {

    /**
     * Returns the value for the given key or throws [NoSuchElementException] if no value is found
     */
    suspend operator fun <T : Any> get(key: AttributeKey<T>): T = getOrNull(key)
        ?: throw NoSuchElementException("Attribute ${key.name} not found")

    /**
     * Returns the value for the given key or `null` if no value is found
     */
    suspend operator fun <T : Any> set(key: AttributeKey<T>, value: T) = put(key, value)

    /**
     * Puts the given value under the given key. If the key already has a value, it is replaced and the old value is returned
     */
    suspend fun <T : Any> put(key: AttributeKey<T>, value: T)

    /**
     * Removes the value for the given key and returns it or `null` if no value is found
     */
    suspend fun <T : Any> remove(key: AttributeKey<T>): T?

    /**
     * Returns `true` if the given key is present in the map
     */
    suspend operator fun contains(key: AttributeKey<*>): Boolean

    /**
     * Returns the value for the given key or `null` if no value is found
     */
    suspend fun <T : Any> getOrNull(key: AttributeKey<T>): T?

    /**
     * Returns all values in the map
     */
    val allKeys: Set<AttributeKey<*>>
}

/**
 * Creates an [AttributeKey] with the given name and type.
 *
 * @param name The name of the attribute
 * @return The created [AttributeKey]
 */
@JvmSynthetic
inline fun <reified T> AttributeKey(name: String): AttributeKey<T> where T : Any =
    AttributeKey(name, TypeInfo.typeOf<T>())


/**
 * Creates an [AttributeKey] with the given name and type. From Jetbrains Ktor Project
 *
 * @param name The name of the attribute
 * @param type The type of the attribute
 * @return The created [AttributeKey]
 */
data class AttributeKey<T : Any> @JvmOverloads constructor(
    val name: String,
    val type: TypeInfo = TypeInfo.typeOf<Any>()
) {
    init {
        require(name.isNotBlank()) { "Name is required" }
    }
}

/**
 * Represents the type information of a value. From Jetbrains Ktor Project
 *
 * @property type The type of the value
 * @property kotlinType The Kotlin type of the value
 */
class TypeInfo(val type: KClass<*>, val kotlinType: KType? = null) {

    override fun hashCode(): Int {
        return kotlinType?.hashCode() ?: type.hashCode()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is TypeInfo) return false

        return if (kotlinType != null || other.kotlinType != null) {
            kotlinType == other.kotlinType
        } else {
            type == other.type
        }
    }

    companion object {
        @JvmStatic
        inline fun <reified T : Any> typeOf(): TypeInfo = TypeInfo(
            T::class, try {
                kotlin.reflect.typeOf<T>()
            } catch (_: Throwable) {
                null
            }
        )
    }
}
