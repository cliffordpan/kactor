package me.hchome.kactor

import kotlinx.coroutines.CompletableDeferred

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
