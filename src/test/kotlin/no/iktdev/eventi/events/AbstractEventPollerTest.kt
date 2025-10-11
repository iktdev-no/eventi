package no.iktdev.eventi.events

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import no.iktdev.eventi.EventDispatcherTest
import no.iktdev.eventi.EventDispatcherTest.DerivedEvent
import no.iktdev.eventi.EventDispatcherTest.OtherEvent
import no.iktdev.eventi.EventDispatcherTest.TriggerEvent
import no.iktdev.eventi.TestBase
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.stores.EventStore
import no.iktdev.eventi.testUtil.wipe
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import sun.rmi.transport.DGCAckHandler.received
import java.time.Duration
import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class AbstractEventPollerTest : TestBase() {
    val dispatcher = EventDispatcher(eventStore)
    val queue = SequenceDispatchQueue(maxConcurrency = 8)

    val poller = object : AbstractEventPoller(eventStore, queue, dispatcher) {}

    @BeforeEach
    fun setup() {
        EventTypeRegistry.wipe()
        EventListenerRegistry.wipe()
        eventStore.clear()
        // Verifiser at det er tomt

        EventTypeRegistry.register(listOf(
            DerivedEvent::class.java,
            TriggerEvent::class.java,
            OtherEvent::class.java
        ))
    }

    @Test
    fun `pollOnce should dispatch all new referenceIds and update lastSeenTime`() = runTest {
        val dispatched = ConcurrentHashMap.newKeySet<UUID>()
        val completionMap = mutableMapOf<UUID, CompletableDeferred<Unit>>()

        EventListenerRegistry.registerListener(object : EventListener() {
            override fun onEvent(event: Event, context: List<Event>): Event? {
                dispatched += event.referenceId
                completionMap[event.referenceId]?.complete(Unit)
                return null
            }
        })

        val referenceIds = (1..10).map { UUID.randomUUID() }

        referenceIds.forEach { refId ->
            val e = EventDispatcherTest.TriggerEvent().usingReferenceId(refId)
            eventStore.persist(e) // persistedAt settes automatisk her
            completionMap[refId] = CompletableDeferred()
        }

        poller.pollOnce()

        completionMap.values.awaitAll()

        assertEquals(referenceIds.toSet(), dispatched)
    }

    @Test
    fun `pollOnce should increase backoff when no events and reset when events arrive`() = runTest {
        val testPoller = object : AbstractEventPoller(eventStore, queue, dispatcher) {
            fun currentBackoff(): Duration = backoff
        }

        testPoller.pollOnce()
        val afterFirst = testPoller.currentBackoff()

        testPoller.pollOnce()
        val afterSecond = testPoller.currentBackoff()

        assertTrue(afterSecond > afterFirst)

        val e = TriggerEvent().usingReferenceId(UUID.randomUUID())
        eventStore.persist(e)

        testPoller.pollOnce()
        val afterReset = testPoller.currentBackoff()

        assertEquals(Duration.ofSeconds(2), afterReset)
    }

    @Test
    fun `pollOnce should group and dispatch exactly 3 events for one referenceId`() = runTest {
        val refId = UUID.randomUUID()
        val received = mutableListOf<Event>()
        val done = CompletableDeferred<Unit>()

        // Wipe alt før test
        EventTypeRegistry.wipe()
        EventListenerRegistry.wipe()
        eventStore.clear() // sørg for at InMemoryEventStore støtter dette

        EventTypeRegistry.register(listOf(TriggerEvent::class.java))

        object : EventListener() {
            override fun onEvent(event: Event, context: List<Event>): Event? {
                received += event
                if (received.size == 3) done.complete(Unit)
                return null
            }
        }

        repeat(3) {
            eventStore.persist(TriggerEvent().usingReferenceId(refId))
        }

        poller.pollOnce()

        done.await()

        assertEquals(3, received.size)
        assertTrue(received.all { it.referenceId == refId })
    }


    @Test
    fun `pollOnce should ignore events before lastSeenTime`() = runTest {
        val refId = UUID.randomUUID()
        val ignored = TriggerEvent().usingReferenceId(refId)

        val testPoller = object : AbstractEventPoller(eventStore, queue, dispatcher) {
            init {
                lastSeenTime = LocalDateTime.now().plusSeconds(1)
            }
        }

        eventStore.persist(ignored)

        testPoller.pollOnce()

        assertFalse(queue.isProcessing(refId))
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `poller handles manually injected duplicate event`() = runTest {
        EventTypeRegistry.register(listOf(MarcoEvent::class.java, EchoEvent::class.java))
        val channel = Channel<Event>(Channel.UNLIMITED)
        val handled = mutableListOf<Event>()


        // Setup
        object : EventListener() {

            override fun onEvent(event: Event, context: List<Event>): Event? {
                if (event !is EchoEvent)
                    return null
                handled += event
                channel.trySend(event)
                return MarcoEvent(true).derivedOf(event)
            }
        }

        val poller = object : AbstractEventPoller(eventStore, queue, dispatcher) {
        }

        // Original event
        val original = EchoEvent(data = "Hello")
        eventStore.persist(original)

        // Act
        poller.pollOnce()
        withContext(Dispatchers.Default.limitedParallelism(1)) {
            withTimeout(Duration.ofMinutes(1).toMillis()) {
                repeat(1) { channel.receive() }
            }
        }

        // Manual replay with new eventId, same referenceId
        val duplicateEvent = EchoEvent("Test me").usingReferenceId(original.referenceId)

        eventStore.persist(duplicateEvent)

        // Act
        poller.pollOnce()

        withContext(Dispatchers.Default.limitedParallelism(1)) {
            withTimeout(Duration.ofMinutes(1).toMillis()) {
                repeat(1) { channel.receive() }
            }
        }



        // Assert
        assertEquals(2, handled.size)
        assertTrue(handled.any { it.eventId == original.eventId })
    }




}