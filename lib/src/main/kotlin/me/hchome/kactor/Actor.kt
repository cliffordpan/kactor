package me.hchome.kactor

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.DisposableHandle

/**
 * An actor is a business logic object that can receive messages and send messages to other actors.
 * @see ActorSystem
 */
interface Actor : Supervisor, DisposableHandle {

    val ref: ActorRef

    val singleton: Boolean

    operator fun contains(ref: ActorRef): Boolean

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

    /**
     * Recover actor attributes
     * @param attributes attributes
     */
    fun recover(attributes: Attributes)

    /**
     * Snapshot actor attributes
     * @return attributes
     */
    fun snapshot(): Attributes
}
