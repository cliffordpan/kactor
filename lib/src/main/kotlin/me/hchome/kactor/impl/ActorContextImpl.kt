package me.hchome.kactor.impl

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import me.hchome.kactor.ActorContext
import me.hchome.kactor.ActorHandler
import me.hchome.kactor.ActorRef
import me.hchome.kactor.ActorSystem
import me.hchome.kactor.ActorSystemException
import me.hchome.kactor.ActorSystemNotificationMessage
import me.hchome.kactor.Attributes
import kotlin.reflect.KClass
import kotlin.time.Duration

internal data class ActorContextImpl(private val self: BaseActor, private val system: ActorSystem) : ActorContext,
    Attributes by AttributesImpl() {

    override fun getService(kClass: KClass<out ActorHandler>): ActorRef = system.getService(kClass)

    override val services: Set<ActorRef>
        get() = system.getServices()

    override val ref: ActorRef
        get() = self.ref

    override val parent: ActorRef
        get() = self.parent
    override val children: Set<ActorRef>
        get() = self.childrenRefs.toSet()

    override fun <T : ActorHandler> sendService(kClass: KClass<out T>, message: Any) {
        val ref = ActorRef.ofService(kClass)
        if (ref !in system) {
            system.notifySystem(
                self.ref, ActorRef.Companion.EMPTY,
                "Target service $kClass is not found: $message",
                ActorSystemNotificationMessage.NotificationType.ACTOR_EXCEPTION
            )
            return
        }
        system.send(ref, self.ref, message)
    }

    override fun hasActor(ref: ActorRef): Boolean = ref in system

    override fun sendChildren(message: Any) {
        if (self.singleton) {
            system.notifySystem(
                self.ref, ActorRef.Companion.EMPTY,
                "Target children shouldn't be a service: $message",
                ActorSystemNotificationMessage.NotificationType.ACTOR_EXCEPTION
            )
            return
        }
        if (self.childrenRefs.isEmpty()) {
            system.notifySystem(
                self.ref, ActorRef.Companion.EMPTY,
                "Send a message to an empty children: $message",
                ActorSystemNotificationMessage.NotificationType.MESSAGE_UNDELIVERED
            )
            return
        }
        self.childrenRefs.forEach {
            system.send(it, self.ref, message)
        }
    }

    override fun getChild(id: String): ActorRef {
        return children.firstOrNull { it.actorId.endsWith(id) } ?: ActorRef.Companion.EMPTY
    }

    override fun sendChild(childRef: ActorRef, message: Any) {
        if (self.singleton) {
            return
        }
        self.childrenRefs.firstOrNull { it == childRef }?.also {
            system.send(it, self.ref, message)
        } ?: run {
            system.notifySystem(
                self.ref, ActorRef.Companion.EMPTY,
                "Send a message to an empty child $childRef: $message",
                ActorSystemNotificationMessage.NotificationType.MESSAGE_UNDELIVERED
            )
        }
    }

    override fun sendParent(message: Any) {
        if (self.hasParent) {
            system.send(self.parent, self.ref, message)
        } else {
            system.notifySystem(
                self.ref, ActorRef.Companion.EMPTY,
                "Send a message to an empty parent: $message",
                ActorSystemNotificationMessage.NotificationType.MESSAGE_UNDELIVERED
            )
        }
    }

    override fun stopActor(ref: ActorRef) {
        system.destroyActor(ref)
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

    override suspend fun <T : ActorHandler> newChild(
        id: String?,
        kClass: KClass<T>,
    ): ActorRef {
        if (self.singleton) throw ActorSystemException("Can't create a child for a singleton actor")
        return system.actorOfSuspend(id, self.ref, kClass)
    }

    override suspend fun <T : ActorHandler> newActor(
        id: String?,
        kClass: KClass<T>
    ): ActorRef {
        return system.actorOfSuspend(id, ActorRef.Companion.EMPTY, kClass)
    }

    override fun schedule(
        period: Duration,
        initDelay: Duration,
        block: suspend ActorHandler.(String) -> Unit
    ): Job {
        return self.schedule(period, initDelay, block)
    }

    override fun task(
        initDelay: Duration,
        block: suspend ActorHandler.(String) -> Unit
    ): Job {
        return self.task(initDelay, block)
    }

    override fun sendActor(ref: ActorRef, message: Any) {
        if (ref in system) {
            system.send(ref, self.ref, message)
        } else {
            system.notifySystem(
                self.ref, ActorRef.Companion.EMPTY,
                "Send a message to an empty actor $ref: $message",
                ActorSystemNotificationMessage.NotificationType.MESSAGE_UNDELIVERED
            )
        }
    }

    override fun <T : Any> ask(message: Any, ref: ActorRef, timeout: Duration): Deferred<T> {
        if (ref == self.ref) {
            throw ActorSystemException("Can't ask self")
        } else if (ref == ActorRef.Companion.EMPTY) {
            throw ActorSystemException("Can't ask empty actor")
        }
        if (ref in system) {
            return system.ask<T>(ref, self.ref, message)
        } else {
            throw ActorSystemException("Actor not in system")
        }
    }

    override suspend fun <T : ActorHandler> newService(kClass: KClass<T>): ActorRef = system.serviceOfSuspend(kClass)
}

