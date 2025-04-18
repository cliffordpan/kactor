@file:Suppress("unused")

package me.hchome.kactor

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext
import kotlin.properties.Delegates
import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

private typealias ActorHandlerScope = suspend ActorHandler.(Any, ActorRef) -> Unit
private typealias AskActorHandlerScope = suspend ActorHandler.(Any, ActorRef, CompletableDeferred<in Any>) -> Unit

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
    private val dispatcher: CoroutineDispatcher,
    kClass: KClass<T>,
    factory: ActorHandlerFactory,
    val actorSystem: ActorSystem,
    val id: String,
    capacity: Int,
    onBufferOverflow: BufferOverflow,
    val singleton: Boolean
) : Actor, DisposableHandle, CoroutineScope where T : ActorHandler {

    private val mailbox = Channel<MessageWrapper>(capacity, onBufferOverflow, ::undeliveredMessageHandler)

    private val handlerScope: ActorHandlerScope = { h: ActorHandler, message: Any, sender: ActorRef ->
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

    private val askHandlerScope: AskActorHandlerScope = { h: ActorHandler, message: Any, sender: ActorRef, cb ->
        try {
            h.onAsk(message, sender, cb)
        } catch (e: Throwable) {
            h.onException(e, sender)
            (actorSystem as ActorSystemImpl).notifySystem(
                sender, context.ref, "Exception occurred: $message",
                ActorSystemNotificationMessage.NotificationType.ACTOR_EXCEPTION, e
            )
        }
    }

    val ref: ActorRef by lazy { ActorRef(kClass, id) }

    var parent: ActorRef by Delegates.vetoable(ActorRef.EMPTY) { _, old, new ->
        old.isEmpty() && new.isNotEmpty()
    }

    val childrenRefs: MutableSet<ActorRef> = mutableSetOf<ActorRef>()
    private val context = ActorContextImpl(this@BaseActor, actorSystem)
    private val holder = ActorContextHolder(context)
    private val job = SupervisorJob()
    private val mutex = Mutex()
    override val coroutineContext: CoroutineContext
        get() = dispatcher + job + holder


    val hasChildren: Boolean
        get() = childrenRefs.isNotEmpty()

    val hasParent: Boolean
        get() = parent.isNotEmpty()

    val handler: T by lazy {
        factory.getBean(kClass)
    }

    init {
        processingMessage()
    }

    override fun send(message: Any, sender: ActorRef) {
        mailbox.trySend(SetStatusMessageWrapperImpl(message, sender)).getOrThrow()
    }

    override fun <T : Any> ask(message: Any, sender: ActorRef, callback: CompletableDeferred<in T>) {
        try {
            mailbox.trySend(GetStatusMessageWrapperImpl(message, sender, callback)).getOrThrow()
        } catch (e: Throwable) {
            callback.completeExceptionally(e)
        }
    }

    fun addChild(child: ActorRef) {
        childrenRefs.add(child)
    }

    fun removeChild(child: ActorRef) {
        childrenRefs.remove(child)
    }

    fun task(
        initDelay: Duration = Duration.ZERO,
        block: suspend ActorHandler.() -> Unit
    ): Job = launch {
        delay(initDelay)
        block(handler)
    }

    fun schedule(
        id: String,
        period: Duration,
        initDelay: Duration = Duration.ZERO,
        block: suspend ActorHandler.() -> Unit
    ): Job = launch {
        delay(initDelay)
        while (isActive) {
            block(handler)
            delay(period)
        }
    }

    private fun undeliveredMessageHandler(wrapper: MessageWrapper) {
        val message = wrapper.message
        val sender = wrapper.sender
        val formattedMessage = "Undelivered message: $message"
        (actorSystem as ActorSystemImpl).notifySystem(
            sender, ref, formattedMessage,
            ActorSystemNotificationMessage.NotificationType.MESSAGE_UNDELIVERED
        )
    }

    @Suppress("UNCHECKED_CAST")
    private fun processingMessage() {
        launch {
            handler.preStart()
            mailbox.consumeEach { wrapper ->
                val message = wrapper.message
                val sender = wrapper.sender
                try {
                    when (wrapper) {
                        is SetStatusMessageWrapperImpl -> {
                            val (message, sender) = wrapper
                            handlerScope(handler, message, sender)
                        }

                        is GetStatusMessageWrapperImpl<*> -> {
                            val (message, sender, cb) = wrapper
                            askHandlerScope(handler, message, sender, cb as CompletableDeferred<in Any>)
                        }
                    }
                } catch (e: Throwable) {
                    fatalHandling(e, message, sender)
                }
            }
            handler.postStop()
        }
    }

    private fun fatalHandling(e: Throwable, message: Any, sender: ActorRef) {
        (actorSystem as ActorSystemImpl).notifySystem(
            sender, ref, "Fatal message: $message",
            notificationType = ActorSystemNotificationMessage.NotificationType.ACTOR_FATAL, e
        )
        mailbox.close(e)
        this.cancel("Fatal message: $message", e)
        actorSystem.destroyActor(ref)
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
            if (ref in system) {
                system.send(ref, self.ref, message)
                return
            } else {
                (system as ActorSystemImpl).notifySystem(
                    self.ref, ActorRef.EMPTY,
                    "Target service $kClass is not found: $message",
                    ActorSystemNotificationMessage.NotificationType.ACTOR_EXCEPTION
                )
            }
        }

        override fun sendChildren(message: Any) {
            if (self.singleton) {
                (system as ActorSystemImpl).notifySystem(
                    self.ref, ActorRef.EMPTY,
                    "Target children shouldn't be a service: $message",
                    ActorSystemNotificationMessage.NotificationType.ACTOR_EXCEPTION
                )
                return
            }
            if (self.childrenRefs.isEmpty()) {
                (system as ActorSystemImpl).notifySystem(
                    self.ref, ActorRef.EMPTY,
                    "Send a message to an empty children: $message",
                    ActorSystemNotificationMessage.NotificationType.MESSAGE_UNDELIVERED
                )
                return
            }
            self.childrenRefs.forEach {
                system.send(it, self.ref, message)
            }
        }

        override fun <T : ActorHandler> getChild(id: String, kClass: KClass<T>): ActorRef {
            return children.firstOrNull { it.actorId.endsWith(id) && it.handler == kClass } ?: ActorRef.EMPTY
        }

        override fun <T : Any, H: ActorHandler> askChild(message: Any, id: String, kClass: KClass<H>, timeout: Duration): Deferred<T> {
            var ref: ActorRef = getChild(id, kClass)
            if (ref.isEmpty()) {
                ref = createChild(id = id, kClass = kClass)
            }
            return system.ask<T>(ref, self.ref, message, timeout)
        }

        override fun sendChild(childRef: ActorRef, message: Any) {
            if (self.singleton) {
                return
            }
            self.childrenRefs.firstOrNull { it == childRef }?.also {
                system.send(it, self.ref, message)
            } ?: run {
                (system as ActorSystemImpl).notifySystem(
                    self.ref, ActorRef.EMPTY,
                    "Send a message to an empty child $childRef: $message",
                    ActorSystemNotificationMessage.NotificationType.MESSAGE_UNDELIVERED
                )
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

        override fun schedule(
            id: String,
            period: Duration,
            initDelay: Duration,
            block: suspend ActorHandler.() -> Unit
        ): Job {
            return self.schedule(id, period, initDelay, block)
        }

        override fun task(
            initDelay: Duration,
            block: suspend ActorHandler.() -> Unit
        ): Job {
            return self.task(initDelay, block)
        }


        override fun getActor(ref: ActorRef): ActorRef {
            return system[ref]
        }

        override fun sendActor(ref: ActorRef, message: Any) {
            if (getActor(ref).isNotEmpty()) {
                system.send(ref, self.ref, message)
            } else {
                (system as ActorSystemImpl).notifySystem(
                    self.ref, ActorRef.EMPTY,
                    "Send a message to an empty actor $ref: $message",
                    ActorSystemNotificationMessage.NotificationType.MESSAGE_UNDELIVERED
                )
            }
        }

        override fun <T : Any> ask(message: Any, ref: ActorRef, timeout: Duration): Deferred<T> {
            if (ref == self.ref) {
                throw ActorSystemException("Can't ask self")
            } else if (ref == ActorRef.EMPTY) {
                throw ActorSystemException("Can't ask empty actor")
            }
            return system.ask<T>(ref, self.ref, message)
        }
    }

    private interface MessageWrapper {
        val message: Any
        val sender: ActorRef
    }

    private data class SetStatusMessageWrapperImpl(
        override val message: Any,
        override val sender: ActorRef
    ) : MessageWrapper

    private data class GetStatusMessageWrapperImpl<T>(
        override val message: Any,
        override val sender: ActorRef,
        val callback: CompletableDeferred<in T>
    ) : MessageWrapper

    @OptIn(DelicateCoroutinesApi::class)
    override fun dispose() {
        handler.preDestroy()
        job.cancel()
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
internal class ActorSystemImpl(dispatcher: CoroutineDispatcher?, private val handlerFactory: ActorHandlerFactory) :
    ActorSystem, CoroutineScope, DisposableHandle {
    private val job = SupervisorJob()
    override val coroutineContext: CoroutineContext = Dispatchers.Default + job
    private val defaultActorDispatcher: CoroutineDispatcher = dispatcher ?: Dispatchers.IO
    private val mutex = Mutex()
    private val actors = mutableMapOf<ActorRef, BaseActor<*>>()
    internal val _notifications = MutableSharedFlow<ActorSystemNotificationMessage>(0, 10, BufferOverflow.DROP_OLDEST)
    override val notifications: Flow<ActorSystemNotificationMessage>
        get() = _notifications

    override fun dispose() {
        for (actor in actors.values) {
            actor.dispose()
        }
        actors.clear()
        coroutineContext.cancel()
    }

    override suspend fun <T : ActorHandler> actorOfSuspend(
        dispatcher: CoroutineDispatcher?,
        id: String?,
        parent: ActorRef,
        config: ActorConfig,
        kClass: KClass<T>
    ): ActorRef = actorOfSuspend(dispatcher, id, false, parent, config, kClass)

    override suspend fun <T : ActorHandler> serviceOfSuspend(
        dispatcher: CoroutineDispatcher?,
        config: ActorConfig,
        kClass: KClass<T>
    ): ActorRef = actorOfSuspend(dispatcher, null, true, ActorRef.EMPTY, config, kClass)

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


    override operator fun contains(actorRef: ActorRef): Boolean {
        return this.actors.contains(actorRef)
    }

    override operator fun get(actorRef: ActorRef): ActorRef {
        return this.actors[actorRef]?.ref ?: ActorRef.EMPTY
    }

    private fun <T : ActorHandler> handler(kClass: KClass<T>): T = handlerFactory.getBean(kClass)

    private fun <T : ActorHandler> actorOf(
        dispatcher: CoroutineDispatcher?,
        id: String?,
        singleton: Boolean,
        parent: ActorRef,
        config: ActorConfig,
        kClass: KClass<T>
    ): ActorRef = runBlocking {
        actorOfSuspend(dispatcher, id, singleton, parent, config, kClass)
    }

    @OptIn(ExperimentalUuidApi::class)
    private suspend fun <T : ActorHandler> actorOfSuspend(
        dispatcher: CoroutineDispatcher?,
        id: String?,
        singleton: Boolean,
        parent: ActorRef,
        config: ActorConfig,
        kClass: KClass<T>
    ): ActorRef {
        val parentActor = if (parent.isNotEmpty()) {
            actors[parent] ?: throw ActorSystemException("Parent actor not found")
        } else null
        var actorId = buildActorId(parent, id, singleton, kClass)
        val actorDispatcher = dispatcher ?: defaultActorDispatcher
        return mutex.withLock {
            if (parentActor != null && parentActor.singleton) {
                throw ActorSystemException("Parent actor is a singleton actor")
            }

            val ref = ActorRef(kClass, actorId)
            if (actors.contains(ref)) {
                if (singleton) {
                    return ref
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

    override fun <T : Any> ask(actorRef: ActorRef, sender: ActorRef, message: Any, timeout: Duration): Deferred<T> = async {
        val actor = actors[actorRef] ?: throw ActorSystemException("Actor not found")
        val deferred = CompletableDeferred<T>()
        try {
            withTimeout(timeout) {
                actor.ask<T>(message, sender, deferred)
                deferred.await()
            }
        } catch (e: TimeoutCancellationException) {
            notifySystem(
                sender,
                actorRef,
                "Actor timeout",
                ActorSystemNotificationMessage.NotificationType.ACTOR_TIMEOUT,
            )
            throw ActorSystemException("Actor timeout")
        }
    }

    @OptIn(ExperimentalUuidApi::class)
    private fun buildActorId(
        parent: ActorRef?,
        id: String?,
        singleton: Boolean,
        kClass: KClass<*>
    ): String {
        val baseId = when {
            id.isNullOrBlank() && !singleton -> "actor-${Uuid.random()}"
            id.isNullOrBlank() -> "$kClass"
            else -> id
        }
        return if (parent != null && parent.isNotEmpty()) {
            "${parent.actorId}/$baseId"
        } else {
            baseId
        }
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
        ActorSystemNotificationMessage.NotificationType.ACTOR_TIMEOUT,
        ActorSystemNotificationMessage.NotificationType.ACTOR_EXCEPTION,
            -> ActorSystemNotificationMessage.MessageLevel.WARN

        ActorSystemNotificationMessage.NotificationType.ACTOR_MESSAGE,
        ActorSystemNotificationMessage.NotificationType.ACTOR_CREATED,
        ActorSystemNotificationMessage.NotificationType.ACTOR_DESTROYED -> ActorSystemNotificationMessage.MessageLevel.INFO

        ActorSystemNotificationMessage.NotificationType.ACTOR_FATAL -> ActorSystemNotificationMessage.MessageLevel.ERROR
    }
    val notification = ActorSystemNotificationMessage(sender, receiver, level, message, throwable)
    launch {
        _notifications.emit(notification)
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

    override suspend fun clear() {
        return mutex.withLock { attributes.clear() }
    }
}