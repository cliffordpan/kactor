package me.hchome.kactor.impl

import me.hchome.kactor.Actor
import me.hchome.kactor.ActorRef
import me.hchome.kactor.ActorRegistry
import me.hchome.kactor.isEmpty
import java.util.concurrent.ConcurrentHashMap

internal class LocalActorRegistry: ActorRegistry{
    private val actors = ConcurrentHashMap<ActorRef, Actor>()


    override fun all(): Set<Actor> {
        return actors.values.toSet()
    }

    override fun set(ref: ActorRef, actor: Actor) {
        if(actors.containsKey(ref)) throw IllegalArgumentException("Actor already exists")
        if(ref.isEmpty()) throw IllegalArgumentException("ActorRef is empty")
        actors[ref] = actor
    }

    override fun get(ref: ActorRef): Actor {
        if(!actors.containsKey(ref)) throw IllegalArgumentException("Actor does not exist")
        return actors[ref]!!
    }

    override fun contains(ref: ActorRef): Boolean {
        return actors.containsKey(ref)
    }

    override fun remove(ref: ActorRef): Actor? {
//        if(!actors.containsKey(ref)) throw IllegalArgumentException("Actor does not exist")
        return actors.remove(ref)
    }

    override fun clear() {
        for (actor in actors.values) {
            actor.dispose()
        }
        actors.clear()
    }
}