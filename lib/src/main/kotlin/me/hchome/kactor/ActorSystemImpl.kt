@file:Suppress("unused")

package me.hchome.kactor

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.delay
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
import kotlin.time.Duration
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

private typealias ActorHandlerScope = suspend ActorHandler.(Any, ActorRef) -> Unit

private class ActorContextHolder(val context: ActorContext) : AbstractCoroutineContextElement(ActorContextHolder) {
    companion object Key : CoroutineContext.Key<ActorContextHolder>
}

private fun <T> createActor(
    dispatcher: CoroutineDispatcher,
    kClass: KClass<T>,
    factory: ActorHandlerFactory,
    actorSystem: ActorSystem,
    id: String,
    capacity: Int,
    onBufferOverflow: BufferOverflow,
    singleton: Boolean
): BaseActor<T> where T : ActorHandler =
    BaseActor<T>(dispatcher, kClass, factory, actorSystem, id, capacity, onBufferOverflow, singleton)

private class BaseActor<T>(
    dispatcher: CoroutineDispatcher,
    kClass: KClass<T>,
    factory: ActorHandlerFactory,
    val actorSystem: ActorSystem,
    val id: String,
    capacity: Int,
    onBufferOverflow: BufferOverflow,
    val singleton: Boolean
) : Actor, DisposableHandle, CoroutineScope where T : ActorHandler {

    private val mailbox = Channel<MessageWrapper>(capacity, onBufferOverflow, ::undeliveredMessageHandler)


    private val handlerScope: ActorHandlerScope = object : ActorHandlerScope {
        override suspend fun invoke(h: ActorHandler, message: Any, sender: ActorRef) {
            withContext(SupervisorJob(job) + holder) {
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

    val ref: ActorRef by lazy { ActorRef(kClass, id) }

    var parent: ActorRef by Delegates.vetoable(ActorRef.EMPTY) { _, old, new ->
        old == ActorRef.EMPTY && new != ActorRef.EMPTY
    }

    val childrenRefs: MutableSet<ActorRef> = mutableSetOf<ActorRef>()
    private val jobs: MutableMap<String, Job> = mutableMapOf()
    private val context = ActorContextImpl(this@BaseActor, actorSystem)
    private val holder = ActorContextHolder(context)
    private val job = SupervisorJob()
    private val mutex = Mutex()
    override val coroutineContext: CoroutineContext = dispatcher + job


    val hasChildren: Boolean
        get() = childrenRefs.isNotEmpty()

    val hasParent: Boolean
        get() = parent != ActorRef.EMPTY

    val handler: T by lazy {
        factory.getBean(kClass)
    }

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

    fun hasJob(id:String) :Boolean {
        return id in jobs
    }

    suspend fun task(id:String, initDelay: Duration = Duration.ZERO, block: suspend ActorHandler.(ActorContext) -> Unit): Boolean {
        if (id in jobs.keys) {
            return false
        }
        val job = launch(SupervisorJob(job) + holder) {
            delay(initDelay)
            block(handler, context)
        }
        mutex.withLock {
            jobs[id] = job
        }
        return true
    }

    suspend fun schedule(
        id: String,
        period: Duration,
        initDelay: Duration = Duration.ZERO,
        block: suspend ActorHandler.(ActorContext) -> Unit
    ): Boolean {
        if (id in jobs.keys) {
            false
        }
        val job = launch(SupervisorJob(job) + holder) {
            delay(initDelay)
            while (isActive) {
                block(handler, context)
                delay(period)
            }
        }
        mutex.withLock {
            jobs[id] = job
        }
        return true
    }

    suspend fun cancelSchedule(id: String): Boolean {
        val job = jobs[id] ?: return false
        job.cancel()
        mutex.withLock {
            jobs.remove(id)
        }
        return true
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
            mailbox.consumeEach {
                val (message, sender) = it
                try {
                    handlerScope(handler, message, sender)
                } catch (e: Throwable) {
                    fatalHandling(e, message, sender)
                }
            }
        }
    }

    private fun fatalHandling(e: Throwable, message: Any, sender: ActorRef) {
        (actorSystem as ActorSystemImpl).notifySystem(
            sender, ref, "Fatal message: $message",
            notificationType = ActorSystemNotificationMessage.NotificationType.ACTOR_FATAL, e
        )
        if (singleton) {
            mailbox.close(e)
            this.cancel("Fatal message: $message", e)
            actorSystem.destroyActor(ref)
        }
    }


    private data class ActorContextImpl(private val self: BaseActor<*>, private val system: ActorSystem) : ActorContext,
        Attributes by AttributesImpl() {

        override fun <T : ActorHandler> getService(kClass: KClass<T>): ActorRef = system.getService(kClass)

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

        override fun <T : ActorHandler> createChild(
            dispatcher: CoroutineDispatcher?,
            id: String?,
            config: ActorConfig,
            kClass: KClass<T>
        ): ActorRef {
            if (self.singleton) throw ActorSystemException("Can't create a child for a singleton actor")
            return system.actorOf(dispatcher, id, self.ref, config, kClass)
        }

        override fun <T : ActorHandler> createNew(
            dispatcher: CoroutineDispatcher?,
            id: String?,
            config: ActorConfig,
            kClass: KClass<T>
        ): ActorRef {
            return system.actorOf(dispatcher, id, ActorRef.EMPTY, config, kClass)
        }

        override suspend fun schedule(
            id: String,
            period: Duration,
            initDelay: Duration,
            block: suspend ActorHandler.(ActorContext) -> Unit
        ): Boolean {
            return self.schedule(id, period, initDelay, block)
        }

        override suspend fun cancelSchedule(id: String): Boolean {
            return self.cancelSchedule(id)
        }

        override fun hasJob(id: String): Boolean {
            return self.hasJob(id)
        }

        override suspend fun task(
            id: String,
            initDelay: Duration,
            block: suspend ActorHandler.(ActorContext) -> Unit
        ): Boolean {
            return self.task(id, initDelay, block)
        }
    }

    private data class MessageWrapper(val message: Any, val sender: ActorRef)

    @OptIn(DelicateCoroutinesApi::class)
    override fun dispose() {
        if (!mailbox.isClosedForSend) {
            mailbox.close()
        }
        if (this.jobs.isNotEmpty()) {
            this.jobs.values.forEach { it.cancel() }
            this.jobs.clear()
        }
        if (this.coroutineContext.isActive) {
            this.cancel()
        }
    }
}


/**
 * Actor system implementation
 */
internal class ActorSystemImpl(dispatcher: CoroutineDispatcher?, private val handlerFactory: ActorHandlerFactory) :
    ActorSystem, CoroutineScope, DisposableHandle {
    private val job = SupervisorJob()
    override val coroutineContext: CoroutineContext = Dispatchers.Default + job
    private val defaultActorDispatcher: CoroutineDispatcher = dispatcher ?: Dispatchers.IO
    private val mutex = Mutex()
    private val actors = mutableMapOf<ActorRef, BaseActor<*>>()
    override val notifications: MutableSharedFlow<ActorSystemNotificationMessage>
        get() = MutableSharedFlow<ActorSystemNotificationMessage>(0, 10, BufferOverflow.DROP_OLDEST)

    override fun dispose() {
        for (actor in actors.values) {
            actor.dispose()
        }
        actors.clear()
        coroutineContext.cancel()
    }

    override fun <T : ActorHandler> actorOf(
        dispatcher: CoroutineDispatcher?,
        id: String?,
        parent: ActorRef,
        config: ActorConfig,
        kClass: KClass<T>
    ): ActorRef = actorOf(dispatcher, id, false, parent, config, kClass)

    override fun <T : ActorHandler> serviceOf(
        dispatcher: CoroutineDispatcher?,
        config: ActorConfig,
        kClass: KClass<T>
    ): ActorRef = actorOf(dispatcher, null, true, ActorRef.EMPTY, config, kClass)

    override fun getServices(): Set<ActorRef> {
        return actors.entries.filter { it.value.singleton }.map { it.key }.toSet()
    }

    override fun <Handler : ActorHandler> getService(kClass: KClass<Handler>): ActorRef {
        return actors.entries.firstOrNull {
            it.key.handler == kClass &&
                    it.key.actorId == "$kClass" &&
                    it.value.singleton &&
                    kClass.isInstance(it.value.handler)
        }?.key ?: ActorRef.EMPTY
    }


    override fun contains(actorRef: ActorRef): Boolean {
        return this.actors.contains(actorRef)
    }

    override fun get(actorRef: ActorRef): ActorRef {
        return this.actors[actorRef]?.ref ?: ActorRef.EMPTY
    }

    private fun <T : ActorHandler> handler(kClass: KClass<T>): T = handlerFactory.getBean(kClass)

    @OptIn(ExperimentalUuidApi::class)
    private fun <T : ActorHandler> actorOf(
        dispatcher: CoroutineDispatcher?,
        id: String?,
        singleton: Boolean,
        parent: ActorRef,
        config: ActorConfig,
        kClass: KClass<T>
    ): ActorRef = runBlocking {
        var actorId = if (id.isNullOrBlank() && !singleton) {
            "actor-${Uuid.random()}"
        } else if (id.isNullOrBlank()) {
            "$kClass"
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

            val ref = ActorRef(kClass, actorId)
            if (actors.contains(ref)) {
                if (singleton) {
                    return@runBlocking ref
                } else {
                    throw ActorSystemException("Actor with id $actorId already exists")
                }
            }

            val actor = createActor(
                actorDispatcher, kClass, handlerFactory, this@ActorSystemImpl, actorId, config.capacity,
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

        ActorSystemNotificationMessage.NotificationType.ACTOR_FATAL -> ActorSystemNotificationMessage.MessageLevel.ERROR
    }
    val notification = ActorSystemNotificationMessage(sender, receiver, level, message, throwable)
    launch {
        notifications.emit(notification)
    }
}

/**
 * context function get actor context from coroutine context
 */
suspend fun ActorHandler.context(): ActorContext =
    coroutineContext[ActorContextHolder]?.context ?: throw ActorSystemException("No context")

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