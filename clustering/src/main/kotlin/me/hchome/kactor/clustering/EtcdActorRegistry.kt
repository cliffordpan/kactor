package me.hchome.kactor.clustering

import io.etcd.jetcd.Client
import me.hchome.kactor.Actor
import me.hchome.kactor.ActorRef
import me.hchome.kactor.ActorRegistry

class EtcdActorRegistry: ActorRegistry {
    private val client = Client.builder().endpoints("http://localhost:2379").build()

    override fun all(): Set<Actor> {
        TODO("Not yet implemented")
    }

    override fun set(ref: ActorRef, actor: Actor) {
        TODO("Not yet implemented")
    }

    override fun get(ref: ActorRef): Actor {
        TODO("Not yet implemented")
    }

    override fun contains(ref: ActorRef): Boolean {
        TODO("Not yet implemented")
    }

    override fun remove(ref: ActorRef): Actor {
        TODO("Not yet implemented")
    }

    override fun clear() {
        TODO("Not yet implemented")
    }
}