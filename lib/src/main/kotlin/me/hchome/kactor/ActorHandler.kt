package me.hchome.kactor

import kotlinx.coroutines.CompletableDeferred

/**
 * Actor handler - business logic for an actor
 */
interface ActorHandler {

    /**
     * Ask handler
     */
    context(context:ActorContext)
    suspend fun onAsk(message: Any, sender: ActorRef, callback: CompletableDeferred<in Any>) {}

    /**
     * Receive handler
     */
    context(context:ActorContext)
    suspend fun onMessage(message: Any, sender: ActorRef) {}

    /**
     * exception handler
     */
    context(context:ActorContext)
    suspend fun onException(exception: Throwable, sender: ActorRef) {
        throw exception
    }

    /**
     * Task exception handler
     */
    context(context:ActorContext)
    fun onTaskException(taskInfo: TaskInfo, exception: Throwable) {}

    /**
     * Before the actor receiving and processing messages
     */
    context(context:ActorContext)
    suspend fun preStart() {}

    /**
     * After the actor stopped receiving messages
     */
    context(context:ActorContext)
    suspend fun postStop() {}

    /**
     * Before the actor system destroys the actor
     */
    context(context:ActorContext)
    fun preDestroy() {}
}
