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
     * Sends a prioritized message to the actor. Unlike the regular `send` method, this method
     * ensures that the message is handled with higher priority in the actor's message processing queue.
     *
     * @param message The message to be sent to the actor.
     * @param sender The reference to the sender actor. Defaults to an empty actor reference if not provided.
     */
    fun sendPrioritized(message: Any, sender: ActorRef = ActorRef.EMPTY)

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
