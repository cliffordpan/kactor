package me.hchome.kactor

import kotlin.time.Duration

/**
 * Actors can be restarted by the supervisor strategy when they crash.
 * [OneForOne] restarts the child actor by default.
 * [AllForOne] restarts all children when one of them crashes.
 * [Backoff] restarts the child actor with an exponential backoff delay.
 * [Resume] Not restarts the child actor but send to exception handling.
 * [Stop] stops the child actor.
 * [Escalate] escalates the failure to the supervisor
 *
 */
sealed interface RestartStrategy {
    object OneForOne : RestartStrategy
    object AllForOne : RestartStrategy
    data class Backoff(val initialDelay: Duration, val maxDelay: Duration) : RestartStrategy
    object Resume : RestartStrategy
    object Stop : RestartStrategy
    object Escalate : RestartStrategy
}

interface Supervisor {
    /**
     * Handles the failure of the child actor.
     * @param child The child actor that failed
     * @param singleton Whether the child actor is a singleton
     * @param cause The cause of the failure
     */
    suspend fun supervise(
        child: ActorRef,
        singleton: Boolean,
        cause: Throwable,
  )

    /**
     * recover the child actor attributes
     */
    suspend fun recover(child: ActorRef, attributes: Attributes)

    /**
     * snapshot the child actor attributes
     */
    suspend fun snapshot(child: ActorRef): Attributes
}