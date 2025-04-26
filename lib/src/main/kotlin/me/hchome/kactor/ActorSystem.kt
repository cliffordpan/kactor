@file:Suppress("unused")

package me.hchome.kactor

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
import me.hchome.kactor.impl.ActorSystemImpl
import me.hchome.kactor.impl.LocalActorRegistry
import kotlin.reflect.KClass
import kotlin.time.Duration

/**
 * An actor system is a container for actors. It is responsible for creating, destroying, and sending messages to actors.
 * @see Actor
 */
@JvmDefaultWithCompatibility
interface ActorSystem : Supervisor, ActorHandlerRegistry, DisposableHandle {
    val notifications: Flow<ActorSystemNotificationMessage>

    /**
     * Check if an actor exists
     * @param actorRef actor reference
     */
    operator fun contains(actorRef: ActorRef): Boolean

    /**
     * create an actor
     * @param id actor id
     * @param parent parent actor reference
     * @param kClass actor handler class
     * @return actor reference
     */
    fun <T> actorOf(
        id: String? = null,
        parent: ActorRef = ActorRef.EMPTY,
        kClass: KClass<T>
    ): ActorRef where T : ActorHandler = runBlocking {
        actorOfSuspend(id, parent, kClass)
    }

    /**
     * create an actor
     * @param id actor id
     * @param parent parent actor reference
     * @return actor reference
     */
    suspend fun <T> actorOfSuspend(
        id: String? = null,
        parent: ActorRef = ActorRef.EMPTY,
        kClass: KClass<T>
    ): ActorRef where T : ActorHandler

    /**
     * create a service actor
     * @param kClass actor handler class
     * @return actor reference
     */
    fun <T> serviceOf(
        kClass: KClass<T>
    ): ActorRef where T : ActorHandler = runBlocking {
        serviceOfSuspend(kClass)
    }

    suspend fun <T> serviceOfSuspend(
        kClass: KClass<T>
    ): ActorRef where T : ActorHandler

    /**
     * get all services
     */
    fun getServices(): Set<ActorRef>

    /**
     * destroy an actor
     * @param actorRef actor reference
     */
    fun destroyActor(actorRef: ActorRef)

    /**
     * send a message to an actor
     * @param actorRef actor reference
     * @param sender sender actor reference
     * @param message message
     */
    fun send(actorRef: ActorRef, sender: ActorRef, message: Any)

    /**
     * send a message to an actor
     * @param actorRef actor reference
     * @param message message
     */
    fun send(actorRef: ActorRef, message: Any) = send(actorRef, ActorRef.EMPTY, message)


    /**
     * ask a status from an actor
     * @param actorRef actor reference
     * @param sender sender actor reference
     * @param message message
     * @param timeout timeout
     * @return deferred result
     */
    fun <T : Any> ask(actorRef: ActorRef, sender: ActorRef, message: Any, timeout: Duration = Duration.INFINITE): Deferred<T>

    /**
     * get a service actor reference
     * @param kClass actor handler class
     * @return actor reference
     */
    fun getService(kClass: KClass<out ActorHandler>): ActorRef


    fun notifySystem(
        sender: ActorRef,
        receiver: ActorRef,
        message: String,
        notificationType: ActorSystemNotificationMessage.NotificationType,
        throwable: Throwable? = null
    )

    companion object {

        /**
         * Create an actor system
         *
         * <p>
         *     Planned to support a clustering actor system in the future.
         * </p>
         *
         * @param dispatcher coroutine dispatcher
         * @param factory actor handler factory
         * @param registry actor registry, currently only support local registry
         * @return actor system
         */
        @JvmStatic
        fun createOrGet(
            dispatcher: CoroutineDispatcher = Dispatchers.Default,
            factory: ActorHandlerFactory = DefaultActorHandlerFactory,
            registry: ActorRegistry = LocalActorRegistry(),
            restartStrategy: RestartStrategy = RestartStrategy.OneForOne
        ): ActorSystem {
            return ActorSystemImpl(dispatcher, factory, registry, restartStrategy)
        }
    }
}

suspend inline fun <reified T> ActorSystem.actorOfSuspend(id: String? = null, parent: ActorRef = ActorRef.EMPTY): ActorRef where T : ActorHandler =
    actorOfSuspend(id, parent, T::class)

suspend inline fun <reified T> ActorSystem.serviceOfSuspend(): ActorRef where T : ActorHandler =
    serviceOfSuspend(T::class)

inline fun <reified T> ActorSystem.actorOf(id: String? = null, parent: ActorRef = ActorRef.EMPTY): ActorRef where T : ActorHandler =
    actorOf(id, parent, T::class)

inline fun <reified T> ActorSystem.serviceOf(): ActorRef where T : ActorHandler = serviceOf(T::class)

inline fun <reified T> ActorSystem.getService(): ActorRef where T : ActorHandler = getService(T::class)
