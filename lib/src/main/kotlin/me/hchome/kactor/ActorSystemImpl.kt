@file:Suppress("unused")

package me.hchome.kactor

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext
import kotlin.properties.Delegates
import kotlin.reflect.KClass
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

private typealias ActorHandlerScope = suspend ActorHandler.(ActorContext, Any, ActorRef) -> Unit

private class ActorContextHolder(val context: ActorContext) : AbstractCoroutineContextElement(ActorContextHolder) {
    companion object Key : CoroutineContext.Key<ActorContextHolder>
}

private fun createActor(
    dispatcher: CoroutineDispatcher,
    handler: ActorHandler,
    actorSystem: ActorSystem,
    id: String,
    capacity: Int,
    onBufferOverflow: BufferOverflow,
    singleton: Boolean
): BaseActor = BaseActor(dispatcher, handler, actorSystem, id, capacity, onBufferOverflow, singleton)

private class BaseActor(
    dispatcher: CoroutineDispatcher,
    val handler: ActorHandler,
    val actorSystem: ActorSystem,
    val id: String,
    capacity: Int,
    onBufferOverflow: BufferOverflow,
    val singleton: Boolean
) : Actor, DisposableHandle, CoroutineScope {

    private val mailbox = Channel<MessageWrapper>(capacity, onBufferOverflow, ::undeliveredMessageHandler)
    private val job = SupervisorJob()

    private val handlerScope: ActorHandlerScope = object : ActorHandlerScope {
        override suspend fun invoke(h: ActorHandler, context: ActorContext, message: Any, sender: ActorRef) {
            withContext(SupervisorJob(job) + ActorContextHolder(context)) {
                try {
                    h.onMessage(message, sender)
                } catch (e: Throwable) {
                    h.onException(e, sender)
                    (actorSystem as ActorSystemImpl).notifySystem(
                        sender, context.ref, "Exception occurred: $message",
                        ActorSystemNotificationMessage.NotificationType.ACTOR_EXCEPTION, e
                    )
                }
            }
        }
    }

    val ref: ActorRef by lazy { ActorRef(handler::class, id) }

    var parent: ActorRef by Delegates.vetoable(ActorRef.EMPTY) { _, old, new ->
        old == ActorRef.EMPTY && new != ActorRef.EMPTY
    }

    val childrenRefs: MutableSet<ActorRef> = mutableSetOf<ActorRef>()


    override val coroutineContext: CoroutineContext = dispatcher + job

    val hasChildren: Boolean
        get() = childrenRefs.isNotEmpty()

    val hasParent: Boolean
        get() = parent != ActorRef.EMPTY

    init {
        processingMessage()
    }

    override fun send(message: Any, sender: ActorRef) {
        mailbox.trySend(MessageWrapper(message, sender)).getOrThrow()
    }

    fun addChild(child: ActorRef) {
        childrenRefs.add(child)
    }

    fun removeChild(child: ActorRef) {
        childrenRefs.remove(child)
    }

    private fun undeliveredMessageHandler(wrapper: MessageWrapper) {
        val (message, sender) = wrapper
        val formattedMessage = "Undelivered message: $message"
        (actorSystem as ActorSystemImpl).notifySystem(
            sender, ref, formattedMessage,
            ActorSystemNotificationMessage.NotificationType.MESSAGE_UNDELIVERED
        )
    }

    private fun processingMessage() {
        launch(job) {
            val context = ActorContextImpl(this@BaseActor, actorSystem)

            mailbox.consumeEach {
                val (message, sender) = it
                try {
                    handlerScope(handler, context, message, sender)
                } catch (e: Throwable) {
                    fetalHandling(e, message, sender)
                }
            }
        }
    }

    private fun fetalHandling(e: Throwable, message: Any, sender: ActorRef) {
        (actorSystem as ActorSystemImpl).notifySystem(
            sender, ref, "Fetal message: $message",
            notificationType = ActorSystemNotificationMessage.NotificationType.ACTOR_FETAL, e
        )
        mailbox.close(e)
        this.cancel("Fetal message: $message", e)
        actorSystem.destroyActor(ref)
    }


    private data class ActorContextImpl(private val self: BaseActor, private val system: ActorSystem) : ActorContext,
        Attributes by AttributesImpl() {

        override val services: Set<ActorRef>
            get() = system.getServices()

        override val ref: ActorRef
            get() = self.ref

        override val parent: ActorRef
            get() = self.parent
        override val children: Set<ActorRef>
            get() = self.childrenRefs.toSet()

        override fun <T : ActorHandler> sendService(kClass: KClass<T>, message: Any) {
            val ref = ActorRef(kClass, "$kClass")
            system.send(ref, self.ref, message)
        }

        override fun sendChildren(message: Any) {
            if (self.singleton) {
                return
            }
            self.childrenRefs.forEach {
                system.send(it, self.ref, message)
            }
        }

        override fun sendChild(childRef: ActorRef, message: Any) {
            if (self.singleton) {
                return
            }
            self.childrenRefs.firstOrNull { it == childRef }?.also {
                system.send(it, self.ref, message)
            }
        }

        override fun sendParent(message: Any) {
            if (self.hasParent) {
                system.send(self.parent, self.ref, message)
            } else {
                (system as ActorSystemImpl).notifySystem(
                    self.ref, ActorRef.EMPTY,
                    "Send a message to an empty parent: $message",
                    ActorSystemNotificationMessage.NotificationType.MESSAGE_UNDELIVERED
                )
            }
        }

        override fun sendSelf(message: Any) {
            system.send(self.ref, self.ref, message)
        }

        override fun stopChild(childRef: ActorRef) {
            system.destroyActor(childRef)
        }

        override fun stopSelf() {
            system.destroyActor(self.ref)
        }


        override fun stopChildren() {
            self.childrenRefs.forEach {
                system.destroyActor(it)
            }
        }

        override fun createChild(
            dispatcher: CoroutineDispatcher?,
            id: String?,
            config: ActorConfig,
            handlerCreator: ActorHandlerCreator
        ): ActorRef {
            if(self.singleton) throw ActorSystemException("Can't create a child for a singleton actor")
            return system.actorOf(dispatcher, id, self.ref, config, handlerCreator)
        }

       override fun createNew(
           dispatcher: CoroutineDispatcher?,
           id: String?,
           config: ActorConfig,
           handlerCreator: ActorHandlerCreator
       ): ActorRef {
           return system.actorOf(dispatcher, id, ActorRef.EMPTY, config, handlerCreator)
       }
    }

    private data class MessageWrapper(val message: Any, val sender: ActorRef)

    @OptIn(DelicateCoroutinesApi::class)
    override fun dispose() {
        if (!mailbox.isClosedForSend) {
            mailbox.close()
        }
        if (this.coroutineContext.isActive) {
            this.cancel()
        }
    }
}


/**
 * Actor system implementation
 */
private class ActorSystemImpl private constructor(dispatcher: CoroutineDispatcher?) :
    ActorSystem, CoroutineScope, DisposableHandle {
    private val job = SupervisorJob()
    override val coroutineContext: CoroutineContext = Dispatchers.Default + job
    private val defaultActorDispatcher: CoroutineDispatcher = dispatcher ?: Dispatchers.IO
    private val mutex = Mutex()
    private val actors = mutableMapOf<ActorRef, BaseActor>()

    override fun dispose() {
        for (actor in actors.values) {
            actor.dispose()
        }
        actors.clear()
        coroutineContext.cancel()
    }


    override fun actorOf(
        dispatcher: CoroutineDispatcher?,
        id: String?,
        parent: ActorRef,
        config: ActorConfig,
        handler: ActorHandlerCreator
    ): ActorRef = actorOf(dispatcher, id, false, parent, config, handler)

    override fun serviceOf(
        dispatcher: CoroutineDispatcher?,
        config: ActorConfig,
        handler: ActorHandlerCreator
    ): ActorRef = actorOf(dispatcher, null, true, ActorRef.EMPTY, config, handler)

    override fun getServices(): Set<ActorRef> {
        return actors.entries.filter { it.value.singleton }.map { it.key }.toSet()
    }

    @OptIn(ExperimentalUuidApi::class)
    private fun actorOf(
        dispatcher: CoroutineDispatcher?,
        id: String?,
        singleton: Boolean,
        parent: ActorRef,
        config: ActorConfig,
        handler: ActorHandlerCreator
    ): ActorRef = runBlocking {
        val h = handler()
        var actorId = if (id.isNullOrBlank() && !singleton) {
            "actor-${Uuid.random()}"
        } else if (id.isNullOrBlank()) {
            "${h::class}"
        } else {
            id
        }
        val actorDispatcher = dispatcher ?: defaultActorDispatcher
        mutex.withLock {
            val parentActor = if (parent != ActorRef.EMPTY) {
                actors[parent] ?: throw ActorSystemException("Parent actor not found")
            } else null

            if (parentActor != null) {
                actorId = "${parentActor.ref.actorId}/$actorId"
            }

            if (parentActor != null && parentActor.singleton) {
                throw ActorSystemException("Parent actor is a singleton actor")
            }

            val ref = ActorRef(h::class, actorId)
            if (actors.contains(ref)) {
                if (singleton) {
                    return@runBlocking ref
                } else {
                    throw ActorSystemException("Actor with id $actorId already exists")
                }
            }

            val actor = createActor(
                actorDispatcher, h, this@ActorSystemImpl, actorId, config.capacity,
                config.onBufferOverflow, singleton
            )
            actor.parent = parent
            actors[actor.ref] = actor
            if (parentActor != null) {
                actors[parent]?.addChild(actor.ref)
            }
            this@ActorSystemImpl.notifySystem(
                actor.ref, ActorRef.EMPTY, "Actor created",
                ActorSystemNotificationMessage.NotificationType.ACTOR_CREATED
            )
            actor.ref
        }
    }

    override fun destroyActor(actorRef: ActorRef) {
        val actor = actors[actorRef] ?: return
        if (actor.hasChildren) {
            val children = actor.childrenRefs.toSet() // copy a
            for (child in children) {
                destroyActor(child)
            }
        }
        if (actor.hasParent) {
            actors[actor.parent]?.removeChild(actorRef)
        }
        actor.dispose()
        runBlocking {
            mutex.withLock {
                actors.remove(actor.ref)
            }
        }
        notifySystem(
            actor.ref,
            ActorRef.EMPTY,
            "Actor destroyed",
            ActorSystemNotificationMessage.NotificationType.ACTOR_DESTROYED
        )
    }

    override fun send(actorRef: ActorRef, sender: ActorRef, message: Any) {
        val actor = actors[actorRef] ?: throw ActorSystemException("Actor not found")
        actor.send(message, sender)
    }

    companion object {
        private var INSTANCE: ActorSystemImpl? = null
        fun createOrGet(dispatcher: CoroutineDispatcher?): ActorSystem = runBlocking {
            (INSTANCE as? ActorSystem) ?: run {
                INSTANCE = ActorSystemImpl(dispatcher)
                INSTANCE as ActorSystem
            }
        }

        fun destroy() {
            INSTANCE?.dispose()
        }
    }
}


fun ActorSystem.Companion.createOrGet(dispatcher: CoroutineDispatcher? = null): ActorSystem {
    return ActorSystemImpl.createOrGet(dispatcher)
}

fun ActorSystem.Companion.destroy() {
    ActorSystemImpl.destroy()
}

private fun ActorSystemImpl.notifySystem(
    sender: ActorRef,
    receiver: ActorRef,
    message: String,
    notificationType: ActorSystemNotificationMessage.NotificationType,
    throwable: Throwable? = null
) {
    val level = when (notificationType) {
        ActorSystemNotificationMessage.NotificationType.MESSAGE_UNDELIVERED,
        ActorSystemNotificationMessage.NotificationType.ACTOR_EXCEPTION,
            -> ActorSystemNotificationMessage.MessageLevel.WARN

        ActorSystemNotificationMessage.NotificationType.ACTOR_MESSAGE,
        ActorSystemNotificationMessage.NotificationType.ACTOR_CREATED,
        ActorSystemNotificationMessage.NotificationType.ACTOR_DESTROYED -> ActorSystemNotificationMessage.MessageLevel.INFO

        ActorSystemNotificationMessage.NotificationType.ACTOR_FETAL -> ActorSystemNotificationMessage.MessageLevel.ERROR
    }
    val notification = ActorSystemNotificationMessage(sender, receiver, level, message, throwable)
    launch {
        (ActorSystem.NOTIFICATIONS as MutableSharedFlow<ActorSystemNotificationMessage>)
            .emit(notification)
    }
}

/**
 * context function get actor context from coroutine context
 */
suspend fun ActorHandler.context(): ActorContext = coroutineContext[ActorContextHolder]?.context ?: throw ActorSystemException("No context")

/**
 * Attributes implementation for actor context
 * From jetbrains Ktor project
 */
@Suppress("UNCHECKED_CAST")
private class AttributesImpl() : Attributes {
    private val attributes = mutableMapOf<AttributeKey<*>, Any?>()
    private val mutex: Mutex = Mutex()

    override val allKeys: Set<AttributeKey<*>>
        get() = attributes.keys.toSet()

    override suspend fun <T : Any> put(key: AttributeKey<T>, value: T) {
        mutex.withLock { attributes.put(key, value) }
    }

    override suspend fun contains(key: AttributeKey<*>): Boolean {
        return mutex.withLock { attributes.contains(key) }
    }

    override suspend fun <T : Any> getOrNull(key: AttributeKey<T>): T? {
        return mutex.withLock { attributes[key] as? T }
    }

    override suspend fun <T : Any> remove(key: AttributeKey<T>): T? {
        return mutex.withLock { attributes.remove(key) as? T }
    }
}