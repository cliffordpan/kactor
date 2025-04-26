@file:Suppress("unused")

package me.hchome.kactor.impl

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.withTimeout
import me.hchome.kactor.ActorHandler
import me.hchome.kactor.ActorHandlerFactory
import me.hchome.kactor.ActorHandlerRegistry
import me.hchome.kactor.ActorRef
import me.hchome.kactor.ActorRegistry
import me.hchome.kactor.ActorSystem
import me.hchome.kactor.ActorSystemException
import me.hchome.kactor.ActorSystemNotificationMessage
import me.hchome.kactor.Attributes
import me.hchome.kactor.RestartStrategy
import me.hchome.kactor.RestartStrategy.AllForOne
import me.hchome.kactor.RestartStrategy.Backoff
import me.hchome.kactor.RestartStrategy.Escalate
import me.hchome.kactor.RestartStrategy.OneForOne
import me.hchome.kactor.RestartStrategy.Resume
import me.hchome.kactor.RestartStrategy.Stop
import me.hchome.kactor.isNotEmpty
import kotlin.coroutines.CoroutineContext
import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * Actor system implementation
 */
internal class ActorSystemImpl(
    dispatcher: CoroutineDispatcher,
    handlerFactory: ActorHandlerFactory,
    actorRegistry: ActorRegistry,
    val restartStrategy: RestartStrategy
) :
    ActorSystem,
    ActorHandlerRegistry by ActorHandlerRegistryImpl(dispatcher, handlerFactory),
    ActorRegistry by actorRegistry,
    CoroutineScope,
    DisposableHandle {
    private val job = SupervisorJob()
    override val coroutineContext: CoroutineContext = Dispatchers.Default + job
    private val mutex = Mutex()
    internal val _notifications = MutableSharedFlow<ActorSystemNotificationMessage>(0, 10, BufferOverflow.DROP_OLDEST)
    override val notifications: Flow<ActorSystemNotificationMessage>
        get() = _notifications

    override fun dispose() {
        clear()
        coroutineContext.cancel()
    }

    override suspend fun <T : ActorHandler> actorOfSuspend(
        id: String?,
        parent: ActorRef,
        kClass: KClass<T>
    ): ActorRef = actorOfSuspend(id, false, parent, kClass)

    override suspend fun <T> serviceOfSuspend(
        kClass: KClass<T>
    ): ActorRef where T : ActorHandler = actorOfSuspend(null, true, ActorRef.Companion.EMPTY, kClass)

    override fun getServices(): Set<ActorRef> {
        return all().filter { it.singleton }.map { it.ref }.toSet()
    }

    override fun getService(kClass: KClass<out ActorHandler>): ActorRef {
        return all().firstOrNull {
            it.ref.handler == kClass &&
                    it.ref.actorId == "$kClass" &&
                    it.singleton &&
                    kClass.isInstance(it.ref.handler)
        }?.ref ?: ActorRef.Companion.EMPTY
    }

    override suspend fun recover(child: ActorRef, attributes: Attributes) {
        this[child].recover(attributes)
    }

    override suspend fun snapshot(child: ActorRef): Attributes {
        return this[child].snapshot()
    }

    override suspend fun supervise(
        child: ActorRef,
        singleton: Boolean,
        cause: Throwable
    ) {
        when (restartStrategy) {
            is OneForOne, is AllForOne, is Escalate -> {
                val attributes = snapshot(child)
                destroyActor(child)
                val new = if (singleton) {
                    serviceOfSuspend(child.handler)
                } else {
                    actorOfSuspend(child.rawId, ActorRef.EMPTY, child.handler)
                }
                recover(new, attributes)
            }

            is Resume -> {}
            is Stop -> destroyActor(child)
            is Backoff -> {
                val (init, max) = restartStrategy
                val attributes = snapshot(child)
                delay(init)
                destroyActor(child)
                delay(max)
                val new = if (singleton) {
                    serviceOfSuspend(child.handler)
                } else {
                    actorOfSuspend(child.rawId, ActorRef.EMPTY, child.handler)
                }
                recover(new, attributes)
            }
        }
    }

    private fun <T> actorOfSuspend(
        id: String?,
        singleton: Boolean,
        parent: ActorRef,
        kClass: KClass<T>
    ): ActorRef where T : ActorHandler {
        val parentActor = if (parent.isNotEmpty()) this[parent] else null
        var actorId = buildActorId(parent, id, singleton, kClass)

        val config = this[kClass]
        val actorDispatcher = config.dispatcher
        if (parentActor != null && parentActor.singleton) {
            throw ActorSystemException("Parent actor is a singleton actor")
        }

        val ref = ActorRef(kClass, actorId)
        if (contains(ref)) {
            if (singleton) {
                return ref
            } else {
                throw ActorSystemException("Actor with id $actorId already exists")
            }
        }

        val actor = createActor(
            actorDispatcher, kClass, config.factory, this@ActorSystemImpl, actorId, config.config,
            singleton, parentActor
        )
        this[actor.ref] = actor
        this@ActorSystemImpl.notifySystem(
            actor.ref, ActorRef.Companion.EMPTY, "Actor created",
            ActorSystemNotificationMessage.NotificationType.ACTOR_CREATED
        )
        return actor.ref
    }

    override fun destroyActor(actorRef: ActorRef) {
        val actor = this[actorRef]
        this.remove(actorRef)
        notifySystem(
            actor.ref,
            ActorRef.Companion.EMPTY,
            "Actor destroyed",
            ActorSystemNotificationMessage.NotificationType.ACTOR_DESTROYED
        )
    }

    override fun send(actorRef: ActorRef, sender: ActorRef, message: Any) {
        this[actorRef].send(message, sender)
    }

    override fun <T : Any> ask(actorRef: ActorRef, sender: ActorRef, message: Any, timeout: Duration): Deferred<T> = async {
        val actor = this@ActorSystemImpl[actorRef]
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
        if (!id.isNullOrBlank() && id.contains('/')) {
            throw ActorSystemException("Actor id cannot contain '/'")
        }
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

    override fun notifySystem(
        sender: ActorRef,
        receiver: ActorRef,
        message: String,
        notificationType: ActorSystemNotificationMessage.NotificationType,
        throwable: Throwable?
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
        _notifications.tryEmit(notification)
    }
}
