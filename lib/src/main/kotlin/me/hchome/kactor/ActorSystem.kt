@file:Suppress("unused")

package me.hchome.kactor

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
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
    val hasParent: Boolean get() = parent.isNotEmpty()

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
    fun hasActor(ref: ActorRef): Boolean = getActor(ref).isNotEmpty()

    /**
     * Check if an actor has a service
     * @see ActorRef
     */
    fun <T> hasService(kClass: KClass<T>): Boolean where T : ActorHandler = getService(kClass).isNotEmpty()

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
        return !isChild(childRef) && childRef.isNotEmpty() && childRef.actorId.isNotEmpty()
                && childRef.actorId.startsWith(ref.actorId)
    }

    /**
     * Check if an actor is a parent of target actor
     * @see ActorRef
     */
    fun isParent(parentRef: ActorRef): Boolean {
        return parent == parentRef && parent.isNotEmpty()
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
     * Get a child actor reference
     * @see ActorRef
     */
    fun <T> getChild(id: String, kClass: KClass<T>): ActorRef where T : ActorHandler

    /**
     * Send a message to the parent actor
     */
    fun sendParent(message: Any)

    /**
     * Send a message to the self-actor
     */
    fun sendSelf(message: Any)

    /**
     * Send a message to the actor (not self) and wait for a response
     */
    fun <T : Any> ask(message: Any, ref: ActorRef, timeout: Duration = Duration.INFINITE): Deferred<T>

    /**
     * Ask a child
     */
    fun <T : Any, H : ActorHandler> askChild(message: Any, id: String, kClass: KClass<H>, timeout: Duration = Duration.INFINITE): Deferred<T>

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
        block: suspend ActorHandler.() -> Unit
    ): Job

    /**
     * create a run task
     */
    fun task(initDelay: Duration = Duration.ZERO, block: suspend ActorHandler.() -> Unit): Job
}

inline fun <reified T : ActorHandler> ActorContext.sendService(message: Any) = sendService(T::class, message)

inline fun <reified T : ActorHandler> ActorContext.sendChild(id: String, message: Any) = sendChild(getChild<T>(id), message)

inline fun <reified T : ActorHandler> ActorContext.createChild(
    dispatcher: CoroutineDispatcher? = null,
    id: String? = null,
    config: ActorConfig = ActorConfig.DEFAULT,
) = createChild(dispatcher, id, config, T::class)

inline fun <reified T : ActorHandler> ActorContext.createNew(
    dispatcher: CoroutineDispatcher? = null,
    id: String? = null,
    config: ActorConfig = ActorConfig.DEFAULT,
) = createNew(dispatcher, id, config, T::class)

inline fun <reified T : ActorHandler> ActorContext.getService() = getService(T::class)

inline fun <reified T : ActorHandler> ActorContext.getChild(id: String) = getChild(id, T::class)

inline fun <T : Any, reified H : ActorHandler> ActorContext.askChild(message: Any, id: String, timeout: Duration = Duration.INFINITE): Deferred<T> =
    askChild(message, id, H::class, timeout)

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
 * Check if an actor reference is empty
 */

fun ActorRef?.isEmpty(): Boolean = this == null || this == ActorRef.EMPTY || this.actorId.isBlank()

/**
 * Check if an actor reference is not empty
 */
fun ActorRef?.isNotEmpty(): Boolean = !this.isEmpty()

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
    /**
     * Send a message to the actor
     * @param message message
     * @param sender sender actor reference
     */
    fun send(message: Any, sender: ActorRef = ActorRef.EMPTY)

    /**
     * Ask a status from an actor
     *
     * @param message message
     * @param sender sender actor reference
     * @return deferred result
     */
    fun <T : Any> ask(message: Any, sender: ActorRef = ActorRef.EMPTY, callback: CompletableDeferred<in T>)
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
    ): ActorRef where T : ActorHandler = runBlocking {
        actorOfSuspend(dispatcher, id, parent, config, kClass)
    }

    /**
     * create an actor
     * @param dispatcher coroutine dispatcher
     * @param id actor id
     * @param parent parent actor reference
     * @param config actor configuration
     * @param kClass actor handler class
     * @return actor reference
     */
    suspend fun <T> actorOfSuspend(
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
    ): ActorRef where T : ActorHandler = runBlocking {
        serviceOfSuspend(dispatcher, config, kClass)
    }

    suspend fun <T> serviceOfSuspend(
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
    fun <Handler : ActorHandler> getService(kClass: KClass<Handler>): ActorRef

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

suspend inline fun <reified Handler : ActorHandler> ActorSystem.actorOfSuspend(
    dispatcher: CoroutineDispatcher? = null,
    id: String? = null,
    parent: ActorRef = ActorRef.EMPTY,
    config: ActorConfig = ActorConfig.DEFAULT
): ActorRef = actorOfSuspend(dispatcher, id, parent, config, Handler::class)

inline fun <reified Handler : ActorHandler> ActorSystem.serviceOf(
    dispatcher: CoroutineDispatcher? = null,
    config: ActorConfig = ActorConfig.DEFAULT
): ActorRef = serviceOf(dispatcher, config, Handler::class)

suspend inline fun <reified Handler : ActorHandler> ActorSystem.serviceOfSuspend(
    dispatcher: CoroutineDispatcher? = null,
    config: ActorConfig = ActorConfig.DEFAULT
): ActorRef = serviceOfSuspend(dispatcher, config, Handler::class)

inline fun <reified Handler : ActorHandler> ActorSystem.getService(): ActorRef = getService(Handler::class)

/**
 * Actor handler - business logic for an actor
 */
interface ActorHandler {

    /**
     * Ask handler
     */
    suspend fun onAsk(message: Any, sender: ActorRef, callback: CompletableDeferred<in Any>) {}

    /**
     * Receive handler
     */
    suspend fun onMessage(message: Any, sender: ActorRef) {}

    /**
     * exception handler
     */
    suspend fun onException(exception: Throwable, sender: ActorRef) {
        throw exception
    }

    // life-cycle functions
    /**
     * Before the actor receiving and processing messages
     */
    suspend fun preStart() {}

    /**
     * After the actor stopped receiving messages
     */
    suspend fun postStop() {}

    /**
     * Before the actor system destroys the actor
     */
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
        ACTOR_CREATED, ACTOR_DESTROYED, ACTOR_EXCEPTION, ACTOR_FATAL, ACTOR_MESSAGE, MESSAGE_UNDELIVERED, ACTOR_TIMEOUT
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



