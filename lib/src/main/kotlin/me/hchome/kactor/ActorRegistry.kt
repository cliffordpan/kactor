package me.hchome.kactor

interface ActorRegistry {

    fun all(): Set<Actor>

    operator fun set(ref: ActorRef, actor: Actor)

    operator fun get(ref: ActorRef): Actor

    operator fun contains(ref: ActorRef): Boolean

    fun remove(ref: ActorRef): Actor?

    fun clear()
}