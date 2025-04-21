package me.hchome.kactor.impl

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import me.hchome.kactor.ActorConfig
import me.hchome.kactor.ActorHandler
import me.hchome.kactor.ActorHandlerConfigHolder
import me.hchome.kactor.ActorHandlerFactory
import me.hchome.kactor.ActorHandlerRegistry
import me.hchome.kactor.DefaultActorHandlerFactory
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass

internal class ActorHandlerRegistryImpl(
    private val defaultDispatcher: CoroutineDispatcher = Dispatchers.Default,
    private val defaultFactory: ActorHandlerFactory = DefaultActorHandlerFactory
) : ActorHandlerRegistry {

    private val registry: MutableMap<KClass<out ActorHandler>, ActorHandlerConfigHolder> =
        ConcurrentHashMap<KClass<out ActorHandler>, ActorHandlerConfigHolder>()

    override fun <T> register(
        dispatcher: CoroutineDispatcher?,
        config: ActorConfig,
        factory: ActorHandlerFactory?,
        kClass: KClass<T>
    ) where T : ActorHandler {
        val dispatcher = dispatcher ?: defaultDispatcher
        val factory = factory ?: defaultFactory
        registry[kClass] = ActorHandlerConfigHolder(dispatcher, config, factory)
    }

    override fun <T> get(kClass: KClass<T>): ActorHandlerConfigHolder where T : ActorHandler {
        return registry[kClass] ?: throw IllegalArgumentException("No handler registered for $kClass")
    }

    override fun <T> contains(kClass: KClass<T>): Boolean where T : ActorHandler  {
        return registry.containsKey(kClass)
    }
}