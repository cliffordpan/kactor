package me.hchome.kactor.impl

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
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
import me.hchome.kactor.isNotEmpty
import kotlin.collections.forEach
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext
import kotlin.reflect.KClass
import kotlin.time.Duration

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

    private val mailbox = Channel<MessageWrapper>(actorConfig.capacity, actorConfig.onBufferOverflow, ::undeliveredMessageHandler)

    private val handlerScope: ActorHandlerScope = { h: ActorHandler, message: Any, sender: ActorRef ->
        try {
            h.onMessage(message, sender)
        } catch (e: Throwable) {
            fatalHandling(e, message, sender)
        }
    }

    private val askHandlerScope: AskActorHandlerScope = { h: ActorHandler, message: Any, sender: ActorRef, cb ->
        try {
            h.onAsk(message, sender, cb)
        } catch (e: Throwable) {
            fatalHandling(e, message, sender)
        }
    }

    override val ref: ActorRef
        get() = ActorRef(kClass, id)

    val parent: ActorRef
        get() = parentActor?.ref ?: ActorRef.EMPTY

    val childrenRefs: MutableSet<ActorRef> = mutableSetOf<ActorRef>()
    private val context = ActorContextImpl(this@BaseActor, actorSystem)
    private val holder = ActorContextHolder(context)
    private val job = SupervisorJob()
    override val coroutineContext: CoroutineContext
        get() = dispatcher + job + holder


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

    override fun recover(attributes: Attributes) {
        context.recover(attributes)
    }

    override fun snapshot(): Attributes {
        return context.snapshot()
    }

    fun task(
        initDelay: Duration = Duration.ZERO,
        block: suspend ActorHandler.() -> Unit
    ): Job = launch {
        delay(initDelay)
        block(handler)
    }

    fun schedule(
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
        actorSystem.notifySystem(
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

    override suspend fun recover(child: ActorRef, attributes: Attributes) {
        actorSystem.recover(ref, attributes)
    }

    override suspend fun snapshot(child: ActorRef): Attributes {
        return actorSystem.snapshot(ref)
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
                recover(ref, attributes)
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
                    recover(childRef, attr)
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
        handler.preDestroy()
        job.cancel()
        if (!mailbox.isClosedForSend) {
            mailbox.close()
        }

        if (this.coroutineContext.isActive) {
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
    coroutineContext[ActorContextHolder]?.context ?: throw ActorSystemException("No context")

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