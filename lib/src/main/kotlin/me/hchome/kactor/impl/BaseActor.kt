@file:Suppress("unused")
package me.hchome.kactor.impl

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import me.hchome.kactor.Actor
import me.hchome.kactor.ActorConfig
import me.hchome.kactor.ActorContext
import me.hchome.kactor.ActorHandler
import me.hchome.kactor.ActorHandlerFactory
import me.hchome.kactor.ActorRef
import me.hchome.kactor.ActorSystem
import me.hchome.kactor.ActorSystemException
import me.hchome.kactor.ActorSystemNotificationMessage
import me.hchome.kactor.Attributes
import me.hchome.kactor.RestartStrategy.*
import me.hchome.kactor.Supervisor
import me.hchome.kactor.TaskInfo
import me.hchome.kactor.isNotEmpty
import kotlin.collections.forEach
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.uuid.ExperimentalUuidApi

private typealias ActorHandlerScope = suspend ActorHandler.(Any, ActorRef) -> Unit
private typealias AskActorHandlerScope = suspend ActorHandler.(Any, ActorRef, CompletableDeferred<in Any>) -> Unit


internal class BaseActor(
    private val dispatcher: CoroutineDispatcher,
    private val kClass: KClass<out ActorHandler>,
    factory: ActorHandlerFactory,
    val actorSystem: ActorSystem,
    val id: String,
    val actorConfig: ActorConfig,
    override val singleton: Boolean,
    val parentActor: Actor? = null
) : Actor,
    Supervisor,
    CoroutineScope,
    DisposableHandle {
    private val priority: Channel<MessageWrapper> = Channel(32, onBufferOverflow = BufferOverflow.SUSPEND, onUndeliveredElement = ::undeliveredMessageHandler)
    private val normal: Channel<MessageWrapper> = Channel(64, onBufferOverflow = BufferOverflow.SUSPEND, onUndeliveredElement = ::undeliveredMessageHandler)
    private val mailbox =
        Channel<MessageWrapper>(actorConfig.capacity, actorConfig.onBufferOverflow, ::undeliveredMessageHandler)

    private val handlerScope: ActorHandlerScope = { h: ActorHandler, message: Any, sender: ActorRef ->
        context(context) {
            h.onMessage(message, sender)
        }
    }

    private val taskExceptionHandler = CoroutineExceptionHandler { ctx, e ->
        actorSystem.notifySystem(
            ref, ref, "Exception occurred [${ref}] task: $e",
            ActorSystemNotificationMessage.NotificationType.ACTOR_TASK_EXCEPTION, e
        )
        val info = ctx[TaskInfo] ?: return@CoroutineExceptionHandler
        context(context) {
            handler.onTaskException(info, e)
        }
    }

    private val askHandlerScope: AskActorHandlerScope = { h: ActorHandler, message: Any, sender: ActorRef, cb ->
        context(context) {
            h.onAsk(message, sender, cb)
        }
    }

    private val uncaughtExceptionHandler = CoroutineExceptionHandler { _, e ->
        launch { unCaughtFatalHandling(e) }
    }

    override val ref: ActorRef
        get() = ActorRef(kClass, id)

    val parent: ActorRef
        get() = parentActor?.ref ?: ActorRef.EMPTY

    val childrenRefs: MutableSet<ActorRef> = mutableSetOf()
    private val context = ActorContextImpl(this@BaseActor, actorSystem)
    private val holder = ActorContextHolder(context)

    override val coroutineContext: CoroutineContext
        get() = dispatcher + SupervisorJob() + holder + uncaughtExceptionHandler


    val hasParent: Boolean
        get() = parent.isNotEmpty()

    override fun contains(ref: ActorRef): Boolean = ref in childrenRefs

    val handler: ActorHandler by lazy {
        factory.getBean(kClass)
    }

    init {
        if (parentActor != null && parentActor is BaseActor) {
            parentActor.addChild(this.ref)
        }
        processingMessage()
    }

    override fun sendPrioritized(message: Any, sender: ActorRef) {
        val result = priority.trySend(SetStatusMessageWrapperImpl(message, sender))
        if (!result.isSuccess) {
            val e = result.exceptionOrNull()
            actorSystem.notifySystem(
                sender, this.ref, "Failed to send",
                ActorSystemNotificationMessage.NotificationType.ACTOR_FATAL, e
            )
            throw IllegalStateException("Failed to send message to actor ${this::class.simpleName}: $message", e)
        }
    }

    override fun send(message: Any, sender: ActorRef) {
        val result = normal.trySend(SetStatusMessageWrapperImpl(message, sender))
        if (!result.isSuccess) {
            val e = result.exceptionOrNull()
            actorSystem.notifySystem(
                sender, this.ref, "Failed to send",
                ActorSystemNotificationMessage.NotificationType.ACTOR_FATAL, e
            )
            throw IllegalStateException("Failed to send message to actor ${this::class.simpleName}: $message", e)
        }
    }

    override fun <T : Any> ask(message: Any, sender: ActorRef, callback: CompletableDeferred<in T>) {
        val result = normal.trySend(GetStatusMessageWrapperImpl(message, sender, callback))
        if (!result.isSuccess) {
            val e = result.exceptionOrNull()
            actorSystem.notifySystem(
                sender, this.ref, "Failed to ask",
                ActorSystemNotificationMessage.NotificationType.ACTOR_FATAL, e
            )
            if (e != null) {
                callback.completeExceptionally(e)
            }
        }
    }

    fun addChild(child: ActorRef) {
        childrenRefs.add(child)
    }

    fun removeChild(child: ActorRef) {
        childrenRefs.remove(child)
    }

    override fun recover(attributes: Attributes) {
        context.recover(attributes)
    }

    override fun snapshot(): Attributes {
        return context.snapshot()
    }

    @OptIn(ExperimentalUuidApi::class)
    fun task(
        initDelay: Duration = Duration.ZERO,
        block: suspend ActorHandler.(String) -> Unit
    ): Job {
        val taskInfo = TaskInfo.Task(initDelay, block)
        return launch(taskInfo + taskExceptionHandler) {
            delay(initDelay)
            block(handler, taskInfo.id)
        }
    }

    @OptIn(ExperimentalUuidApi::class)
    fun schedule(
        period: Duration,
        initDelay: Duration = Duration.ZERO,
        block: suspend ActorHandler.(String) -> Unit
    ): Job {
        val taskInfo = TaskInfo.Schedule(period, initDelay, block)
        return launch(taskInfo + taskExceptionHandler) {
            delay(initDelay)
            while (isActive) {
                block(handler, taskInfo.id)
                delay(period)
            }
        }
    }

    private fun undeliveredMessageHandler(wrapper: MessageWrapper) {
        val message = wrapper.message
        val sender = wrapper.sender
        val formattedMessage = "Undelivered message: $message"
        actorSystem.notifySystem(
            sender, ref, formattedMessage,
            ActorSystemNotificationMessage.NotificationType.MESSAGE_UNDELIVERED
        )
    }

    @Suppress("UNCHECKED_CAST")
    private fun processingMessage() {
        launch {
            while (isActive) {
                val wrapper = priority.receiveCatching().getOrNull() ?: normal.receive()
                mailbox.send(wrapper)
            }
        }
        launch {
            context(context) {
                try {
                    handler.preStart()
                } catch (e: Throwable) {
                    fatalHandling(e, "Start actor failed", ref)
                    return@launch
                }
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
                try {
                    handler.postStop()
                } catch (e: Throwable) {
                    fatalHandling(e, "Stop actor failed", ref)
                    return@launch
                }
            }
        }
    }

    override suspend fun recover(child: ActorRef, attributes: Attributes) {
        actorSystem.recover(ref, attributes)
    }

    override suspend fun snapshot(child: ActorRef): Attributes? {
        return try {
            actorSystem.snapshot(ref)
        } catch (_: Throwable) {
            null
        }
    }

    override suspend fun supervise(
        child: ActorRef,
        singleton: Boolean,
        cause: Throwable,
    ) {
        val config = this.actorConfig
        when (config.restartStrategy) {
            is OneForOne -> {
                val attributes = snapshot(child)
                actorSystem.destroyActor(child)
                val ref = actorSystem.actorOfSuspend(child.rawId, ref, child.handler)
                if (attributes != null) {
                    recover(ref, attributes)
                }
            }

            is AllForOne -> {
                val children = this.childrenRefs.toSet()
                children.forEach {
                    actorSystem.destroyActor(it)
                }
                childrenRefs.clear()
                children.forEach {
                    val attr = snapshot(it)
                    val childRef = actorSystem.actorOfSuspend(it.rawId, ref, it.handler)
                    if (attr != null) {
                        recover(childRef, attr)
                    }
                }
            }

            is Resume -> {}
            is Stop -> actorSystem.destroyActor(child)
            is Escalate -> fatalHandling(cause, "Bubble up the supervisor", ref)
            is Backoff -> {
                val (init, max) = config.restartStrategy
                delay(init)
                actorSystem.destroyActor(child)
                delay(max)
                actorSystem.actorOfSuspend(child.rawId, ref, child.handler)
            }
        }

    }

    private suspend fun unCaughtFatalHandling(e: Throwable) {
        fatalHandling(e, "Uncaught fatal message", ref)
    }

    private suspend fun fatalHandling(e: Throwable, message: Any, sender: ActorRef) {
        actorSystem.notifySystem(
            sender, ref, "Fatal message: $message",
            notificationType = ActorSystemNotificationMessage.NotificationType.ACTOR_FATAL, e
        )
        if (parent.isNotEmpty() && parentActor != null) {
            parentActor.supervise(ref, singleton, e)
        } else {
            actorSystem.supervise(ref, singleton, e)
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
        if(!priority.isClosedForSend) {
            priority.close()
        }

        if(!normal.isClosedForSend) {
            normal.close()
        }

        // stop mailbox
        if (!mailbox.isClosedForSend) {
            mailbox.close()
        }
        // notify handler cleanup
        context(context) {
            handler.preDestroy()
        }

        // just in case
        if (this.isActive) {
            this.cancel()
        }

        if (childrenRefs.isNotEmpty()) {
            val children = childrenRefs.toSet() // copy a
            for (child in children) {
                actorSystem.destroyActor(child)
            }
        }

        if (parentActor != null && parentActor is BaseActor) {
            parentActor.removeChild(this.ref)
        }
    }
}

private class ActorContextHolder(val context: ActorContext) : AbstractCoroutineContextElement(ActorContextHolder) {
    companion object Key : CoroutineContext.Key<ActorContextHolder>
}

/**
 * context function get actor context from coroutine context
 */
suspend fun ActorHandler.context(): ActorContext =
    currentCoroutineContext()[ActorContextHolder]?.context ?: throw ActorSystemException("No context")

/**
 * Create actor
 * @param dispatcher coroutine dispatcher
 * @param kClass actor handler class
 * @param factory actor handler factory
 * @param actorSystem actor system
 * @param id actor id
 * @param singleton singleton actor flag
 * @return actor instance
 */
internal fun createActor(
    dispatcher: CoroutineDispatcher,
    kClass: KClass<out ActorHandler>,
    factory: ActorHandlerFactory,
    actorSystem: ActorSystem,
    id: String,
    actorConfig: ActorConfig,
    singleton: Boolean,
    parentActor: Actor?
): BaseActor = BaseActor(dispatcher, kClass, factory, actorSystem, id, actorConfig, singleton, parentActor)