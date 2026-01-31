package no.iktdev.eventi.events

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import no.iktdev.eventi.EventDispatcherTest.DerivedEvent
import no.iktdev.eventi.EventDispatcherTest.OtherEvent
import no.iktdev.eventi.EventDispatcherTest.TriggerEvent
import no.iktdev.eventi.MyTime
import no.iktdev.eventi.TestBase
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.testUtil.wipe
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

@DisplayName("""
EventPollerImplementation
Når polleren leser nye events fra EventStore og samarbeider med SequenceDispatchQueue
Hvis nye events ankommer, køen er travel, eller duplikater dukker opp
Så skal polleren dispatch'e riktig, oppdatere lastSeenTime og unngå duplikater
""")
class EventPollerImplementationTest : TestBase() {

    val dispatcher = EventDispatcher(eventStore)
    val queue = SequenceDispatchQueue(maxConcurrency = 8)
    val poller = object : EventPollerImplementation(eventStore, queue, dispatcher) {}

    @BeforeEach
    fun setup() {
        EventTypeRegistry.wipe()
        EventListenerRegistry.wipe()
        eventStore.clear()

        EventTypeRegistry.register(
            listOf(
                DerivedEvent::class.java,
                TriggerEvent::class.java,
                OtherEvent::class.java
            )
        )
    }

    @Test
    @DisplayName("""
    Når polleren finner nye referenceId-er med events
    Hvis pollOnce kjøres
    Så skal alle referenceId-er dispatch'es og lastSeenTime oppdateres
    """)
    fun pollOnceDispatchesAllNewReferenceIdsAndUpdatesLastSeenTime() = runTest {
        val dispatched = ConcurrentHashMap.newKeySet<UUID>()
        val completionMap = mutableMapOf<UUID, CompletableDeferred<Unit>>()

        EventListenerRegistry.registerListener(
            object : EventListener() {
                override fun onEvent(event: Event, history: List<Event>): Event? {
                    dispatched += event.referenceId
                    completionMap[event.referenceId]?.complete(Unit)
                    return null
                }
            }
        )

        val referenceIds = (1..10).map { UUID.randomUUID() }

        referenceIds.forEach { refId ->
            val e = TriggerEvent().usingReferenceId(refId)
            eventStore.persist(e)
            completionMap[refId] = CompletableDeferred()
        }

        poller.pollOnce()
        completionMap.values.awaitAll()

        assertEquals(referenceIds.toSet(), dispatched)
    }

    @Test
    @DisplayName("""
    Når polleren ikke finner nye events
    Hvis pollOnce kjøres flere ganger
    Så skal backoff øke, og resettes når nye events ankommer
    """)
    fun pollOnceIncreasesBackoffWhenNoEventsAndResetsWhenEventsArrive() = runTest {
        val testPoller = object : EventPollerImplementation(eventStore, queue, dispatcher) {
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
    @DisplayName("""
    Når flere events med samme referenceId ligger i EventStore
    Hvis pollOnce kjøres
    Så skal polleren gruppere og dispatch'e alle tre i én batch
    """)
    fun pollOnceGroupsAndDispatchesExactlyThreeEventsForOneReferenceId() = runTest {
        val refId = UUID.randomUUID()
        val received = mutableListOf<Event>()
        val done = CompletableDeferred<Unit>()

        EventTypeRegistry.wipe()
        EventListenerRegistry.wipe()
        eventStore.clear()

        EventTypeRegistry.register(listOf(TriggerEvent::class.java))

        object : EventListener() {
            override fun onEvent(event: Event, history: List<Event>): Event? {
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
    @DisplayName("""
    Når polleren har en lastSeenTime i fremtiden
    Hvis events ankommer med eldre timestamp
    Så skal polleren ignorere dem
    """)
    fun pollOnceIgnoresEventsBeforeLastSeenTime() = runTest {
        val refId = UUID.randomUUID()
        val ignored = TriggerEvent().usingReferenceId(refId)

        val testPoller = object : EventPollerImplementation(eventStore, queue, dispatcher) {
            init {
                lastSeenTime = MyTime.utcNow().plusSeconds(1)
            }
        }

        eventStore.persist(ignored)
        testPoller.pollOnce()

        assertFalse(queue.isProcessing(refId))
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    @DisplayName("""
    Når en duplikat-event injiseres manuelt i EventStore
    Hvis polleren kjører igjen
    Så skal begge events prosesseres, men uten å produsere duplikate derived events
    """)
    fun pollerHandlesManuallyInjectedDuplicateEvent() = runTest {
        EventTypeRegistry.register(listOf(MarcoEvent::class.java, EchoEvent::class.java))
        val channel = Channel<Event>(Channel.UNLIMITED)
        val handled = mutableListOf<Event>()

        object : EventListener() {
            override fun onEvent(event: Event, history: List<Event>): Event? {
                if (event !is EchoEvent) return null
                handled += event
                channel.trySend(event)
                return MarcoEvent(true).derivedOf(event)
            }
        }

        val poller = object : EventPollerImplementation(eventStore, queue, dispatcher) {}

        val original = EchoEvent("Hello")
        eventStore.persist(original)

        poller.pollOnce()

        withContext(Dispatchers.Default.limitedParallelism(1)) {
            withTimeout(Duration.ofMinutes(1).toMillis()) {
                repeat(1) { channel.receive() }
            }
        }

        val duplicateEvent = EchoEvent("Test me").usingReferenceId(original.referenceId)
        eventStore.persist(duplicateEvent)

        poller.pollOnce()

        withContext(Dispatchers.Default.limitedParallelism(1)) {
            withTimeout(Duration.ofMinutes(1).toMillis()) {
                repeat(1) { channel.receive() }
            }
        }

        assertEquals(2, handled.size)
        assertTrue(handled.any { it.eventId == original.eventId })
    }
}
