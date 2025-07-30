package me.hchome.kactor

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import me.hchome.kactor.impl.context
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import kotlin.coroutines.CoroutineContext
import kotlin.random.Random
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

data class TestSignal<T>(val completing: CompletableDeferred<in T>) where T : Any

val AttrKey = AttributeKey<Int>("testKey")
val CallBackKey = AttributeKey<CompletableDeferred<String>>("callbackKey")
val CountKey = AttributeKey<Int>("countKey")

@Suppress("UNCHECKED_CAST")
class TestActor : ActorHandler, CoroutineScope by CoroutineScope(Dispatchers.IO) {

    val scope = CoroutineScope(Dispatchers.IO)

    context(context: ActorContext)
    override suspend fun onMessage(message: Any, sender: ActorRef) {
        launch(this@TestActor.coroutineContext + currentCoroutineContext()) {
            delay(1000)
            println("Task111: ${context.ref}")
        }
        println("==============ref: ${ref.actorId}")
        println("Main: ${context.ref}")
        context.task {
            println("Task: ${context.ref}")
        }
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

    context(context: ActorContext)
    override suspend fun onMessage(message: Any, sender: ActorRef) {

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

class TestActor3 : ActorHandler {

    context(context: ActorContext)
    override suspend fun onAsk(message: Any, sender: ActorRef, callback: CompletableDeferred<in Any>) {
        when (message) {
            "Hello" -> callback.complete("Hello to you too")
            "recover" -> {
                println("ask recovered value")
                callback.complete(context[TEST_ATTR])
            }

            else -> callback.complete("I don't know what you say")
        }
    }

    context(context: ActorContext)
    override suspend fun onMessage(message: Any, sender: ActorRef) {
        when (message) {
            "start" -> context.newChild<TestActor3>(id = "c1")

            "do" -> {
                val childRef = context.getChild("c1")
                val rs: String = context.ask<String>("Hello", childRef).await()
                println(rs)
            }

            "failed" -> {
                context[TEST_ATTR] = Random(10).nextInt(1, 100)
                println(context[TEST_ATTR])
                throw Exception("Failed")
            }

            "recover" -> {
                val childRef = context.getChild("c1")
                println(context.ask<Int>("recover", childRef).await())
            }

            "failed child" -> {
                val childRef = context.getChild("c1")
                context.sendChild(childRef, "failed")
            }

            "job" -> {
                println("I'm restarted")
            }
        }
    }

    companion object {
        val TEST_ATTR = AttributeKey<Int>("testAttr")
    }
}


class A : CoroutineScope {
    private val job = SupervisorJob()
    override val coroutineContext: CoroutineContext
        get() = job + Dispatchers.Default

    fun task1() = launch {
        println("task1: start")
        delay(500)
        println("task1: throwing exception")
        throw RuntimeException("Something went wrong in task1")
    }

    fun task2() = launch {
        println("task2: start")
        repeat(10) { i ->
            delay(300)
            println("task2: still running $i")
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


    @Test
    fun test2(): Unit = runBlocking {
        val actorRef = SYSTEM.actorOfSuspend<TestActor3>()
        SYSTEM.send(actorRef, "start")
        SYSTEM.send(actorRef, "do")
        delay(5.seconds)
    }

    @Test
    fun test3(): Unit = runBlocking {
        val actorRef = SYSTEM.actorOfSuspend<TestActor3>()
        SYSTEM.send(actorRef, "failed")
        SYSTEM.send(actorRef, "job")

        delay(5.seconds)
    }

    @Test
    fun test4(): Unit = runBlocking {
        val actorRef = SYSTEM.actorOfSuspend<TestActor3>()
        SYSTEM.send(actorRef, "start")
        SYSTEM.send(actorRef, "failed child")
        delay(5.seconds)
    }

    @Test
    fun test5(): Unit = runBlocking {
        val actorRef = SYSTEM.actorOfSuspend<TestActor3>()
        SYSTEM.send(actorRef, "start")
        SYSTEM.send(actorRef, "failed child")
        SYSTEM.send(actorRef, "recover")
        delay(5.seconds)
    }

    @Test
    fun test6(): Unit = runBlocking {
        val a = A()
        a.task1()
        a.task2()

        delay(4000) // 等待所有输出
        println("main: done")
    }

    companion object : CoroutineScope by CoroutineScope(Dispatchers.Default) {

        lateinit var SYSTEM: ActorSystem

        @JvmStatic
        @BeforeAll
        fun createSystem() {
            SYSTEM = ActorSystem.createOrGet(Dispatchers.IO)
            SYSTEM.register<TestActor>()
            SYSTEM.register<TestActor2>()
            SYSTEM.register<TestActor3>()

            launch {
                SYSTEM.notifications.collect { notification ->
                    println(notification)
                }
            }
        }

        @AfterAll
        @JvmStatic
        fun cleanup() {
            SYSTEM.dispose()
            cancel()
        }
    }
}