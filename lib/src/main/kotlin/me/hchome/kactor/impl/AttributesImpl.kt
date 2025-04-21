package me.hchome.kactor.impl

import me.hchome.kactor.AttributeKey
import me.hchome.kactor.Attributes
import java.util.concurrent.ConcurrentHashMap

/**
 * Attributes implementation for actor context
 * From jetbrains Ktor project
 */
@Suppress("UNCHECKED_CAST")
internal class AttributesImpl(ref: Attributes? = null) : Attributes {
    private val attributes = ConcurrentHashMap<AttributeKey<*>, Any?>()

    init {
        if (ref != null) {
            this.attributes.putAll(ref.allKeys.associateWith { ref.get(it) })
        }
    }

    override val allKeys: Set<AttributeKey<*>>
        get() = attributes.keys.toSet()

    override fun <T : Any> put(key: AttributeKey<T>, value: T) {
        attributes.put(key, value)
    }

    override fun contains(key: AttributeKey<*>): Boolean {
        return attributes.contains(key)
    }

    override fun <T : Any> getOrNull(key: AttributeKey<T>): T? {
        return attributes[key] as? T
    }

    override fun <T : Any> remove(key: AttributeKey<T>): T? {
        return attributes.remove(key) as? T
    }

    override fun clear() {
        return attributes.clear()
    }

    override fun snapshot(): Attributes {
        return AttributesImpl().also { it.attributes.putAll(attributes) }
    }

    override fun recover(attributes: Attributes) {
        this.attributes.putAll(attributes.allKeys.associateWith { attributes[it] })
    }
}