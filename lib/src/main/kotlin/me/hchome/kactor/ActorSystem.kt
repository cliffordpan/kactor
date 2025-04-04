@file:Suppress("unused")

package me.hchome.kactor

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.Flow
import kotlin.reflect.KClass
import kotlin.reflect.full.createInstance
import kotlin.time.Duration


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
    fun <T> getBean(kClass: KClass<T>): T where T : ActorHandler = getBean(kClass, emptyArray<Any>())

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
     * Check if an actor has a child
     * @see ActorRef
     */
    operator fun contains(childRef: ActorRef): Boolean = children.contains(childRef)


    /**
     * Check system has the reference
     */
    fun hasActor(ref: ActorRef): Boolean = getActor(ref) != ActorRef.EMPTY

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
     * Get an actor reference
     * @see ActorRef
     */
    fun getActor(ref: ActorRef): ActorRef

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
     * Send a message to any actor
     */
    fun sendActor(ref: ActorRef, message: Any)

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
    fun schedule(
        id: String,
        period: Duration,
        initDelay: Duration = Duration.ZERO,
        block: suspend ActorHandler.(ActorContext) -> Unit
    ): Job

    /**
     * create a run task
     */
    fun task(initDelay: Duration = Duration.ZERO, block: suspend ActorHandler.(ActorContext) -> Unit): Job
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

        /**
         * Create an actor reference
         * @param Handler actor handler class
         * @param id actor id
         * @return actor reference
         */
        inline fun <reified Handler : ActorHandler> withId(id: String) = ActorRef(Handler::class, id)

        /**
         * Create an actor reference
         * @param Handler actor handler class
         * @return actor reference
         */
        inline fun <reified Handler : ActorHandler> ref() = ActorRef(Handler::class, "${Handler::class}")
    }
}

/**
 * Actor configuration
 * @see Actor
 */
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
    val notifications: Flow<ActorSystemNotificationMessage>

    /**
     * Check if an actor exists
     * @param actorRef actor reference
     */
    operator fun contains(actorRef: ActorRef): Boolean


    /**
     * Get an actor reference
     * @param actorRef actor reference
     */
    operator fun get(actorRef: ActorRef): ActorRef

    /**
     * create an actor
     * @param dispatcher coroutine dispatcher
     * @param id actor id
     * @param parent parent actor reference
     * @param config actor configuration
     * @param kClass actor handler class
     * @return actor reference
     */
    fun <T> actorOf(
        dispatcher: CoroutineDispatcher? = null,
        id: String? = null,
        parent: ActorRef = ActorRef.EMPTY,
        config: ActorConfig = ActorConfig.DEFAULT,
        kClass: KClass<T>,
    ): ActorRef where T : ActorHandler

    /**
     * create a service actor
     * @param dispatcher coroutine dispatcher
     * @param config actor configuration
     * @param kClass actor handler class
     * @return actor reference
     */
    fun <T> serviceOf(
        dispatcher: CoroutineDispatcher? = null,
        config: ActorConfig = ActorConfig.DEFAULT,
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
     * get a service actor reference
     * @param kClass actor handler class
     * @return actor reference
     */
    fun <Handler : ActorHandler> getService(kClass: KClass<Handler>): ActorRef

    object DefaultActorHandlerFactory : ActorHandlerFactory {
        override fun <T : ActorHandler> getBean(kClass: KClass<T>, vararg args: Any): T {
            if (args.isEmpty()) {
                val o = kClass.objectInstance
                if (o != null) return o
                return kClass.createInstance()
            }
            return kClass.constructors.first().call(*args)
        }
    }

    companion object {
        @JvmStatic
        fun createOrGet(
            dispatcher: CoroutineDispatcher? = null,
            factory: ActorHandlerFactory = DefaultActorHandlerFactory
        ): ActorSystem {
            return ActorSystemImpl(dispatcher, factory)
        }
    }
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

inline fun <reified Handler : ActorHandler> ActorSystem.getService(): ActorRef = getService(Handler::class)

/**
 * Actor handler - business logic for an actor
 */
interface ActorHandler {
    suspend fun onMessage(message: Any, sender: ActorRef) {}
    suspend fun onException(exception: Throwable, sender: ActorRef) {
        throw exception
    }

    // life-cycle functions
    suspend fun preStart() {}
    fun preDestroy() {}
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



