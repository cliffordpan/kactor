@file:Suppress("unused")

package me.hchome.kactor

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlin.reflect.KClass
import kotlin.time.Duration


/**
 *
 */
interface ActorHandlerFactory {
    fun <T> getBean(kClass: KClass<T>): T where T : ActorHandler
}

inline fun <reified T> ActorHandlerFactory.getBean(): T where T : ActorHandler = getBean(T::class)


/**
 * Actor context is a container for actor information and methods. Use the context to do the operations
 * on the actor.
 *
 * @see ActorContext
 * @see ActorHandler
 */
interface ActorContext : Attributes {
    /**
     * Actor reference
     * @see ActorRef
     */
    val ref: ActorRef

    /**
     * Parent actor reference
     * @see ActorRef
     */
    val parent: ActorRef

    /**
     * Children actor references
     * @see ActorRef
     */
    val children: Set<ActorRef>

    /**
     * Service actor references
     * @see ActorRef
     */
    val services: Set<ActorRef>

    /**
     * Check if an actor has a parent
     * @see ActorRef
     */
    val hasParent: Boolean get() = parent != ActorRef.EMPTY

    /**
     * Check if an actor has children
     * @see ActorRef
     */
    val hasChildren: Boolean get() = children.isNotEmpty()

    /**
     * Check if an actor has services
     * @see ActorRef
     */
    val hasServices: Boolean get() = services.isNotEmpty()

    /**
     * Check if an actor has a service
     * @see ActorRef
     */
    fun <T> hasService(kClass: KClass<T>): Boolean where T : ActorHandler = getService(kClass) != ActorRef.EMPTY

    /**
     * Get a service actor reference
     * @see ActorRef
     */
    fun <T> getService(kClass: KClass<T>): ActorRef where T : ActorHandler

    /**
     * Check if an actor is a child of target actor
     * @see ActorRef
     */
    fun isChild(childRef: ActorRef): Boolean = children.contains(childRef)

    /**
     * Check if an actor used to be a child of target actor
     * @see ActorRef
     */
    fun isFormalChild(childRef: ActorRef): Boolean {
        return !isChild(childRef) && childRef != ActorRef.EMPTY && childRef.actorId.isNotEmpty()
                && childRef.actorId.startsWith(ref.actorId)
    }

    /**
     * Check if an actor is a parent of target actor
     * @see ActorRef
     */
    fun isParent(parentRef: ActorRef): Boolean {
        return parent == parentRef && parent != ActorRef.EMPTY
    }

    /**
     * Send a message to a service actor
     */
    fun <T : ActorHandler> sendService(kClass: KClass<T>, message: Any)

    /**
     * Send a message to all children actors
     */
    fun sendChildren(message: Any)

    /**
     * Send a message to a child actor
     */
    fun sendChild(childRef: ActorRef, message: Any)

    /**
     * Send a message to the parent actor
     */
    fun sendParent(message: Any)

    /**
     * Send a message to the self-actor
     */
    fun sendSelf(message: Any)

    /**
     * Stop a child actor
     */
    fun stopChild(childRef: ActorRef)

    /**
     * Stop the self-actor
     */
    fun stopSelf()

    /**
     * Stop all children actors
     */
    fun stopChildren()

    /**
     * Create a child actor
     * @see ActorRef
     */
    fun <T> createChild(
        dispatcher: CoroutineDispatcher? = null,
        id: String? = null,
        config: ActorConfig = ActorConfig.DEFAULT,
        kClass: KClass<T>,
    ): ActorRef where T : ActorHandler

    /**
     * Create a new actor
     * @see ActorRef
     */
    fun <T> createNew(
        dispatcher: CoroutineDispatcher? = null, id: String? = null, config: ActorConfig = ActorConfig.DEFAULT,
        kClass: KClass<T>,
    ): ActorRef where T : ActorHandler

    /**
     * Schedule a task
     */
    fun schedule(id: String, period: Duration, initDelay: Duration = Duration.ZERO, block: suspend () -> Unit):Boolean

    /**
     * cancel a schedule task
     */
    fun cancelSchedule(id: String): Boolean
}

inline fun <reified T : ActorHandler> ActorContext.sendService(message: Any) = sendService(T::class, message)

/**
 * Actor reference
 * @see Actor
 */
data class ActorRef(val handler: KClass<out ActorHandler>, val actorId: String) {
    companion object {
        @JvmStatic
        val EMPTY = ActorRef(ActorHandler::class, "")
    }
}

data class ActorConfig(
    val capacity: Int,
    val onBufferOverflow: BufferOverflow
) {
    companion object {
        @JvmStatic
        val DEFAULT = ActorConfig(100, BufferOverflow.SUSPEND)
    }
}

/**
 * An actor is a business logic object that can receive messages and send messages to other actors.
 * @see ActorSystem
 */
interface Actor {
    fun send(message: Any, sender: ActorRef = ActorRef.EMPTY)
}

/**
 * An actor system is a container for actors. It is responsible for creating, destroying, and sending messages to actors.
 * @see Actor
 */
@JvmDefaultWithCompatibility
interface ActorSystem : DisposableHandle {
    companion object {
        val NOTIFICATIONS: Flow<ActorSystemNotificationMessage> = MutableSharedFlow(
            onBufferOverflow = BufferOverflow.DROP_OLDEST,
            extraBufferCapacity = 100
        )
    }

    fun <T> actorOf(
        dispatcher: CoroutineDispatcher? = null,
        id: String? = null,
        parent: ActorRef = ActorRef.EMPTY,
        config: ActorConfig = ActorConfig.DEFAULT,
        kClass: KClass<T>,
    ): ActorRef where T : ActorHandler

    fun <T> serviceOf(
        dispatcher: CoroutineDispatcher? = null,
        config: ActorConfig = ActorConfig.DEFAULT,
        kClass: KClass<T>
    ): ActorRef where T : ActorHandler

    fun getServices(): Set<ActorRef>

    fun destroyActor(actorRef: ActorRef)

    fun send(actorRef: ActorRef, sender: ActorRef, message: Any)

    fun send(actorRef: ActorRef, message: Any) = send(actorRef, ActorRef.EMPTY, message)

    fun <Handler : ActorHandler> getService(kClass: KClass<Handler>): ActorRef

}

inline fun <reified Handler : ActorHandler> ActorSystem.actorOf(
    dispatcher: CoroutineDispatcher? = null,
    id: String? = null,
    parent: ActorRef = ActorRef.EMPTY,
    config: ActorConfig = ActorConfig.DEFAULT
): ActorRef = actorOf(dispatcher, id, parent, config, Handler::class)

inline fun <reified Handler : ActorHandler> ActorSystem.serviceOf(
    dispatcher: CoroutineDispatcher? = null,
    config: ActorConfig = ActorConfig.DEFAULT
): ActorRef = serviceOf(dispatcher, config, Handler::class)

inline fun <reified Handler : ActorHandler> ActorSystem.getService(): ActorRef? = getService(Handler::class)

/**
 * Actor handler - business logic for an actor
 */
interface ActorHandler {
    suspend fun onMessage(message: Any, sender: ActorRef) {}
    suspend fun onException(exception: Throwable, sender: ActorRef) {
        throw exception
    }
}

/**
 * Notification message for the actor system
 */
data class ActorSystemNotificationMessage(
    val sender: ActorRef,
    val receiver: ActorRef,
    val level: MessageLevel,
    val message: String,
    val exception: Throwable? = null
) {
    /**
     * Notification type
     */
    enum class NotificationType {
        ACTOR_CREATED, ACTOR_DESTROYED, ACTOR_EXCEPTION, ACTOR_FATAL, ACTOR_MESSAGE, MESSAGE_UNDELIVERED
    }

    /**
     * Notification level
     */
    enum class MessageLevel {
        INFO, WARN, ERROR
    }
}

/**
 * Actor system exception
 */
class ActorSystemException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)



