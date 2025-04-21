package me.hchome.kactor

import kotlin.reflect.KClass

/**
 * Actor reference
 * @see Actor
 */
data class ActorRef(
    /**
     * the actor handler class
     */
    val handler: KClass<out ActorHandler>,
    /**
     * the actor id
     */
    val actorId: String,
) {
    /**
     * the actor node id
     */
    var node: String = ""
        internal set

    /**
     * the actor is local or not
     */
    val isLocal: Boolean
        get() = node.isBlank()

    /**
     * the actor is remote or not
     */
    val isRemote: Boolean
        get() = !isLocal

    val rawId: String
        get() = actorId.substringAfterLast('/')

    val hasParent: Boolean
        get() = actorId.contains('/')

    val lastParentId: String
        get() = actorId.substringBeforeLast('/').substringAfterLast('/')

    companion object {
        @JvmStatic
        val EMPTY = ActorRef(ActorHandler::class, "")
    }
}

/**
 * Check if an actor reference is empty
 */

fun ActorRef?.isEmpty(): Boolean = this == null || this == ActorRef.EMPTY || this.actorId.isBlank()

/**
 * Check if an actor reference is not empty
 */
fun ActorRef?.isNotEmpty(): Boolean = !this.isEmpty()