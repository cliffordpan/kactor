package me.hchome.kactor

import kotlinx.coroutines.CoroutineDispatcher
import kotlin.reflect.KClass

/**
 * Actor Registry
 */
interface ActorHandlerRegistry {

    fun <T> register(
        dispatcher: CoroutineDispatcher? = null,
        config: ActorConfig = ActorConfig.DEFAULT,
        factory: ActorHandlerFactory? = null,
        kClass: KClass<T>
    ) where T : ActorHandler

    operator fun <T> get(kClass: KClass<T>): ActorHandlerConfigHolder where T : ActorHandler

    operator fun <T> contains(kClass: KClass<T>): Boolean where T : ActorHandler

}

inline fun <reified T> ActorHandlerRegistry.register(
    dispatcher: CoroutineDispatcher? = null,
    config: ActorConfig = ActorConfig.DEFAULT,
    factory: ActorHandlerFactory? = null,
) where  T : ActorHandler {
    register(dispatcher, config, factory, T::class)
}

data class ActorHandlerConfigHolder(
    val dispatcher: CoroutineDispatcher,
    val config: ActorConfig,
    val factory: ActorHandlerFactory
)