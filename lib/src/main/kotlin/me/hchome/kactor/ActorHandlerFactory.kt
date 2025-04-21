package me.hchome.kactor

import kotlin.reflect.KClass
import kotlin.reflect.full.createInstance

/**
 * Actor handler factory, response for create actor handler
 *
 * @see ActorHandlerFactory.getBean
 */
interface ActorHandlerFactory {

    /**
     * Get an actor handler by class
     *
     * @param kClass actor handler class
     * @return actor handler
     */
    fun <T> getBean(kClass: KClass<T>): T where T : ActorHandler = getBean(kClass, *emptyArray<Any>())

    /**
     * Get an actor handler by class and arguments
     *
     * @param kClass actor handler class
     * @param args arguments
     * @return actor handler
     */
    fun <T> getBean(kClass: KClass<T>, vararg args: Any): T where T : ActorHandler
}

inline fun <reified T> ActorHandlerFactory.getBean(): T where T : ActorHandler = getBean(T::class)

object DefaultActorHandlerFactory : ActorHandlerFactory {
    override fun <T : ActorHandler> getBean(kClass: KClass<T>, vararg args: Any): T {
        if (args.isEmpty()) {
            val o = kClass.objectInstance
            if (o != null) return o
            return kClass.createInstance()
        }
        val constructor = kClass.constructors.firstOrNull {
            it.parameters.size == args.size
        } ?: error("No matching constructor for ${kClass.simpleName}")
        return constructor.call(*args)
    }
}