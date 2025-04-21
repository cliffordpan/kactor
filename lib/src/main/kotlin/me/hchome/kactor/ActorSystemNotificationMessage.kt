package me.hchome.kactor

/**
 * Notification message for the actor system
 */
data class ActorSystemNotificationMessage(
    val sender: ActorRef,
    val receiver: ActorRef,
    val level: MessageLevel,
    val message: String,
    val exception: Throwable? = null
) {
    /**
     * Notification type
     */
    enum class NotificationType {
        ACTOR_CREATED, ACTOR_DESTROYED, ACTOR_EXCEPTION, ACTOR_FATAL, ACTOR_MESSAGE, MESSAGE_UNDELIVERED, ACTOR_TIMEOUT
    }

    /**
     * Notification level
     */
    enum class MessageLevel {
        INFO, WARN, ERROR
    }
}
