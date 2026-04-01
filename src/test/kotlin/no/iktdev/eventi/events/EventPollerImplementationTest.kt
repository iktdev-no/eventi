package no.iktdev.eventi.events

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.test.advanceUntilIdle
import no.iktdev.eventi.EventDispatcherTest.DerivedEvent
import no.iktdev.eventi.EventDispatcherTest.OtherEvent
import no.iktdev.eventi.EventDispatcherTest.TriggerEvent
import no.iktdev.eventi.MyTime
import no.iktdev.eventi.TestBase
import no.iktdev.eventi.lifecycle.LifecycleStore
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.registry.EventListenerRegistry
import no.iktdev.eventi.registry.EventTypeRegistry
import no.iktdev.eventi.testUtil.TestSequenceDispatchQueue
import no.iktdev.eventi.testUtil.wipe
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration.Companion.milliseconds

@DisplayName("""
EventPollerImplementation
Når polleren leser nye events fra EventStore og samarbeider med SequenceDispatchQueue
Hvis nye events ankommer, køen er travel, eller duplikater dukker opp
Så skal polleren dispatch'e riktig, oppdatere lastSeenTime og unngå duplikater
""")
class EventPollerImplementationTest : TestBase() {

    val lifecycleStore = LifecycleStore()
    private val dispatcher = EventDispatcher(eventStore, lifecycleStore)

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
        val testDispatcher = StandardTestDispatcher(testScheduler)
        val queue = TestSequenceDispatchQueue(maxConcurrency = 8, dispatcher = testDispatcher, lifecycleStore)
        val poller = object : EventPollerImplementation(eventStore, queue, dispatcher, lifecycleStore) {}

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
        val testDispatcher = StandardTestDispatcher(testScheduler)
        val queue = TestSequenceDispatchQueue(maxConcurrency = 8, dispatcher = testDispatcher, lifecycleStore)

        val testPoller = object : EventPollerImplementation(eventStore, queue, dispatcher, lifecycleStore) {
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
        val testDispatcher = StandardTestDispatcher(testScheduler)
        val queue = TestSequenceDispatchQueue(maxConcurrency = 8, dispatcher = testDispatcher, lifecycleStore)
        val poller = object : EventPollerImplementation(eventStore, queue, dispatcher, lifecycleStore) {}

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
        val testDispatcher = StandardTestDispatcher(testScheduler)
        val queue = TestSequenceDispatchQueue(maxConcurrency = 8, dispatcher = testDispatcher, lifecycleStore)

        val testPoller = object : EventPollerImplementation(eventStore, queue, dispatcher, lifecycleStore) {
            init {
                lastSeenTime = MyTime.utcNow().plusSeconds(1)
            }
        }

        val refId = UUID.randomUUID()
        val ignored = TriggerEvent().usingReferenceId(refId)

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
        val testDispatcher = StandardTestDispatcher(testScheduler)
        val queue = TestSequenceDispatchQueue(maxConcurrency = 8, dispatcher = testDispatcher, lifecycleStore)
        val poller = object : EventPollerImplementation(eventStore, queue, dispatcher, lifecycleStore) {}

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

        val original = EchoEvent("Hello").newReferenceId()
        eventStore.persist(original)

        poller.pollOnce()

        withContext(testDispatcher) {
            withTimeout(60_000.milliseconds) {
                channel.receive()
            }
        }

        val duplicateEvent = EchoEvent("Test me").usingReferenceId(original.referenceId)
        eventStore.persist(duplicateEvent)

        poller.pollOnce()

        withContext(testDispatcher) {
            withTimeout(60_000.milliseconds) {
                channel.receive()
            }
        }

        assertEquals(2, handled.size)
        assertTrue(handled.any { it.eventId == original.eventId })
    }


    @Test
    @DisplayName("""
    Når tre events har identisk persistedAt
    Hvis watermark peker på første event
    Så skal polleren fortsatt hente e2 og e3 (ikke miste events)
""")
    fun pollerDoesNotLoseEventsWithIdenticalTimestamps() = runTest {
        val testDispatcher = StandardTestDispatcher(testScheduler)
        val queue = TestSequenceDispatchQueue(maxConcurrency = 1, dispatcher = testDispatcher, lifecycleStore)

        val poller = object : EventPollerImplementation(eventStore, queue, dispatcher, lifecycleStore) {
            public override fun updateWatermark(ref: UUID, value: Pair<Instant, Long>) {
                super.updateWatermark(ref, value)
            }

        }

        EventTypeRegistry.wipe()
        EventListenerRegistry.wipe()
        eventStore.clear()

        EventTypeRegistry.register(listOf(TriggerEvent::class.java))

        val refId = UUID.randomUUID()
        val dispatched = mutableListOf<UUID>()

        EventListenerRegistry.registerListener(
            object : EventListener() {
                override fun onEvent(event: Event, history: List<Event>): Event? {
                    dispatched += event.eventId
                    return null
                }
            }
        )

        val ts = MyTime.utcNow()

        val e1 = TriggerEvent().usingReferenceId(refId).also { eventStore.persistAt(it, ts) }
        val e2 = TriggerEvent().usingReferenceId(refId).also { eventStore.persistAt(it, ts) }
        val e3 = TriggerEvent().usingReferenceId(refId).also { eventStore.persistAt(it, ts) }

        // Første poll: polleren ser alle tre, men watermark settes til e3
        poller.pollOnce()
        advanceUntilIdle()

        // Nå simulerer vi den gamle buggen:
        // Sett watermark tilbake til e1
        val persistedId1 = eventStore.all().first { it.eventId == e1.eventId }.id
        poller.updateWatermark(refId, ts to persistedId1)

        // Sett lastSeenTime tilbake til ts (ikke etter ts!)
        poller.lastSeenTime = ts

        // Andre poll: polleren skal hente e2 og e3
        poller.pollOnce()
        advanceUntilIdle()

        assertEquals(
            setOf(e1.eventId, e2.eventId, e3.eventId),
            dispatched.toSet(),
            "Polleren skal ikke miste events med identisk timestamp"
        )
    }

    @Test
    @DisplayName("""
        Når tre events har identisk persistedAt
        Hvis polleren kjøres to ganger
        Så skal events kun dispatch'es én gang (ingen re-dispatch)
        """)
    fun pollerDoesNotRedispatchEventsWithIdenticalTimestamps() = runTest {
        val testDispatcher = StandardTestDispatcher(testScheduler)
        val queue = TestSequenceDispatchQueue(maxConcurrency = 1, dispatcher = testDispatcher, lifecycleStore)

        val poller = object : EventPollerImplementation(eventStore, queue, dispatcher, lifecycleStore) {
            public override fun updateWatermark(ref: UUID, value: Pair<Instant, Long>) {
                super.updateWatermark(ref, value)
            }
        }

        EventTypeRegistry.wipe()
        EventListenerRegistry.wipe()
        eventStore.clear()

        EventTypeRegistry.register(listOf(TriggerEvent::class.java))

        val refId = UUID.randomUUID()
        val dispatched = mutableListOf<UUID>()

        val listener = object : EventListener() {
            override fun onEvent(event: Event, history: List<Event>): Event? {
                dispatched += event.eventId
                return null
            }
        }

        val ts = MyTime.utcNow()

        repeat(3) {
            eventStore.persistAt(TriggerEvent().usingReferenceId(refId), ts)
        }

        // Første poll: alle tre events dispatch'es
        poller.pollOnce()
        advanceUntilIdle()

        // Andre poll: watermark + lastSeenTime skal hindre re-dispatch
        poller.pollOnce()
        advanceUntilIdle()

        assertEquals(
            3,
            dispatched.size,
            "Polleren skal ikke re-dispatch'e events med identisk timestamp"
        )
    }

    @Test
    @DisplayName("""
    Når én referanse ligger foran i tid
    Hvis en annen referanse får events med samme persistedAt
    Så skal polleren hente begge (ingen watermark-skew mellom refs)
    """)
    fun pollerDoesNotSkipOtherReferencesWithSameTimestamp() = runTest {
        val testDispatcher = StandardTestDispatcher(testScheduler)
        val queue = TestSequenceDispatchQueue(maxConcurrency = 1, dispatcher = testDispatcher, lifecycleStore)

        val poller = object : EventPollerImplementation(eventStore, queue, dispatcher, lifecycleStore) {
            public override fun updateWatermark(ref: UUID, value: Pair<Instant, Long>) {
                super.updateWatermark(ref, value)
            }
        }

        EventTypeRegistry.wipe()
        EventListenerRegistry.wipe()
        eventStore.clear()

        EventTypeRegistry.register(listOf(TriggerEvent::class.java))

        val refA = UUID.randomUUID()
        val refB = UUID.randomUUID()
        val dispatched = mutableListOf<UUID>()

        EventListenerRegistry.registerListener(
            object : EventListener() {
                override fun onEvent(event: Event, history: List<Event>): Event? {
                    dispatched += event.eventId
                    return null
                }
            }
        )

        val ts = MyTime.utcNow()

        val eA = TriggerEvent().usingReferenceId(refA).also { eventStore.persistAt(it, ts) }
        val eB = TriggerEvent().usingReferenceId(refB).also { eventStore.persistAt(it, ts) }

        poller.pollOnce()
        advanceUntilIdle()

        assertEquals(
            setOf(eA.eventId, eB.eventId),
            dispatched.toSet(),
            "Polleren skal ikke hoppe over referanser med identisk timestamp"
        )
    }



}
