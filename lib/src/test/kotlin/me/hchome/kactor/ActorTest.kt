package me.hchome.kactor

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.time.Duration.Companion.milliseconds

data class TestSignal<T>(val completing: CompletableDeferred<in T>) where T : Any

val AttrKey = AttributeKey<Int>("testKey")
val CallBackKey = AttributeKey<CompletableDeferred<String>>("callbackKey")
val CountKey = AttributeKey<Int>("countKey")

@Suppress("UNCHECKED_CAST")
class TestActor : ActorHandler {

    val scope = CoroutineScope(Dispatchers.IO)

    override suspend fun onMessage(message: Any, sender: ActorRef) {
        val context = context()
        println("$message -- $sender")
        when (message) {
            is TestSignal<*> -> {
                val next = Random.Default.nextInt(1, 5000)
                context[AttrKey] = next
                context[CallBackKey] = message.completing as CompletableDeferred<String>
                println("${context.ref} set signal - $next")
            }

            else -> {
                println("${context.ref} attr: ${context[AttrKey]}")

                if (context.hasChildren && !context.isChild(sender) && !context.isFormalChild(sender)) {
                    context[CountKey] = context.children.size

                    context.children.forEach { child ->
                        val next = Random.Default.nextInt(1, 5000)
                        scope.launch {
                            delay(next.milliseconds)
                            context.sendChild(child, "${context.ref} follow message to $child: $message - next: $next ms")
                        }
                    }
                } else if (context.isChild(sender) || context.isFormalChild(sender)) {
                    println("Response from children: $sender - $message")
                    context[CountKey]--
                    if (context[CountKey] == 0) {
                        context[CallBackKey].complete("Done children ${context.ref}")
                        context.stopSelf()
                    }
                } else {
                    println("${context.ref}: No children")
                    context[CallBackKey].complete("Done ${context.ref}")
                    context.stopSelf()
                }
            }
        }
    }
}


class TestActor2 : ActorHandler {
    private val scope = CoroutineScope(Dispatchers.IO)
    override suspend fun onMessage(message: Any, sender: ActorRef) {
        val context = context()

        scope.launch {
            if (context.isParent(sender)) {
                println("From parent: $message")
                val next = Random.Default.nextInt(1, 5000)
                delay(next.milliseconds)
                context.sendParent("Child job done - $next ms")
            } else {
                println("From other")
            }
            context.stopSelf()
        }
    }

}


class ActorTest {


    @Test
    fun test(): Unit = runBlocking {
        val actorRef = SYSTEM.actorOf<TestActor>()
        val actorRef2 = SYSTEM.actorOf<TestActor>()
        SYSTEM.actorOf<TestActor2>(parent = actorRef)
        SYSTEM.actorOf<TestActor2>(parent = actorRef)
        val future1 = CompletableDeferred<String>()
        val future2 = CompletableDeferred<String>()
        SYSTEM.send(actorRef, TestSignal(future1))
        SYSTEM.send(actorRef2, TestSignal(future2))

        SYSTEM.send(actorRef, "Hello to 1")
        SYSTEM.send(actorRef2, "Hello to 2")

        val job1 = launch {
            println(future1.await())
            println("***")
        }
        val job2 = launch {
            println(future2.await())
            println("***==")
        }

        joinAll(job1, job2)
    }

    companion object {

        lateinit var SYSTEM: ActorSystem

        @JvmStatic
        @BeforeAll
        fun createSystem() {
            SYSTEM = ActorSystem.createOrGet(Dispatchers.IO)
        }

        @AfterAll
        @JvmStatic
        fun cleanup() {
            SYSTEM.dispose()
        }
    }
}