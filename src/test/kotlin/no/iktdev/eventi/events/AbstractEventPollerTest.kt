package no.iktdev.eventi.events

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.test.runTest
import no.iktdev.eventi.EventDispatcherTest
import no.iktdev.eventi.EventDispatcherTest.DerivedEvent
import no.iktdev.eventi.EventDispatcherTest.OtherEvent
import no.iktdev.eventi.EventDispatcherTest.TriggerEvent
import no.iktdev.eventi.TestBase
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.testUtil.wipe
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class AbstractEventPollerTest : TestBase() {
    val dispatcher = EventDispatcher(store)
    val queue = SequenceDispatchQueue(maxConcurrency = 8)

    val poller = object : AbstractEventPoller(store, queue, dispatcher) {}

    @BeforeEach
    fun setup() {
        EventTypeRegistry.wipe()
        EventListenerRegistry.wipe()
        store.clear()
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
            store.save(e) // persistedAt settes automatisk her
            completionMap[refId] = CompletableDeferred()
        }

        poller.pollOnce()

        completionMap.values.awaitAll()

        assertEquals(referenceIds.toSet(), dispatched)
    }

    @Test
    fun `pollOnce should increase backoff when no events and reset when events arrive`() = runTest {
        val testPoller = object : AbstractEventPoller(store, queue, dispatcher) {
            fun currentBackoff(): Duration = backoff
        }

        testPoller.pollOnce()
        val afterFirst = testPoller.currentBackoff()

        testPoller.pollOnce()
        val afterSecond = testPoller.currentBackoff()

        assertTrue(afterSecond > afterFirst)

        val e = TriggerEvent().usingReferenceId(UUID.randomUUID())
        store.save(e)

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
        store.clear() // sørg for at InMemoryEventStore støtter dette

        EventTypeRegistry.register(listOf(TriggerEvent::class.java))

        object : EventListener() {
            override fun onEvent(event: Event, context: List<Event>): Event? {
                received += event
                if (received.size == 3) done.complete(Unit)
                return null
            }
        }

        repeat(3) {
            store.save(TriggerEvent().usingReferenceId(refId))
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

        val testPoller = object : AbstractEventPoller(store, queue, dispatcher) {
            init {
                lastSeenTime = LocalDateTime.now().plusSeconds(1)
            }
        }

        store.save(ignored)

        testPoller.pollOnce()

        assertFalse(queue.isProcessing(refId))
    }






}