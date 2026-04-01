@file:OptIn(ExperimentalCoroutinesApi::class)

package no.iktdev.eventi.events

import kotlinx.coroutines.*
import kotlinx.coroutines.test.*
import no.iktdev.eventi.InMemoryEventStore
import no.iktdev.eventi.lifecycle.LifecycleStore
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.Metadata
import no.iktdev.eventi.registry.EventTypeRegistry
import org.junit.jupiter.api.DisplayName
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

class FakeDispatchQueue(
    private val scope: CoroutineScope,
    private val lifecycleStore: LifecycleStore
) : SequenceDispatchQueue(8, scope, lifecycleStore) {

    private val active = ConcurrentHashMap.newKeySet<UUID>()

    override fun isProcessing(referenceId: UUID): Boolean = referenceId in active

    override fun dispatch(referenceId: UUID, history: List<Event>, newEvents: List<Event>, dispatcher: EventDispatcher): Job {
        active.add(referenceId)
        return scope.launch {
            try {
                dispatcher.dispatch(referenceId, history, newEvents)
            } finally {
                active.remove(referenceId)
            }
        }
    }
}


class FakeDispatcher(private val lifecycleStore: LifecycleStore) : EventDispatcher(InMemoryEventStore(), lifecycleStore) {

    val dispatched = mutableListOf<Pair<UUID, List<Event>>>()

    override fun dispatch(referenceId: UUID, history: List<Event>, newEvents: List<Event>) {
        dispatched += referenceId to history
    }
}

class TestEvent : Event() {
    fun withReference(id: UUID): TestEvent {
        this.referenceId = id
        return this
    }
    fun setMetadata(metadata: Metadata): TestEvent {
        this.metadata = metadata
        return this
    }
}


@DisplayName("""
EventPollerImplementation – simulert kø og dispatch
Når polleren leser events fra EventStore og samarbeider med SequenceDispatchQueue
Hvis køen er ledig, travel, eller events ankommer i ulike tidsrekkefølger
Så skal polleren oppdatere lastSeenTime, unngå duplikater og prosessere riktig
""")
class RunSimulationTestTest {

    val lifecycleStore = LifecycleStore()

    private lateinit var store: InMemoryEventStore
    private lateinit var dispatcher: FakeDispatcher
    private lateinit var testDispatcher: TestDispatcher
    private lateinit var scope: CoroutineScope
    private lateinit var queue: FakeDispatchQueue
    private lateinit var poller: EventPollerImplementation

    @BeforeEach
    fun setup() {
        store = InMemoryEventStore()
        dispatcher = FakeDispatcher(lifecycleStore)
        testDispatcher = StandardTestDispatcher()
        scope = CoroutineScope(testDispatcher)
        queue = FakeDispatchQueue(scope, lifecycleStore)
        EventTypeRegistry.register(TestEvent::class.java)
        poller = object : EventPollerImplementation(store, queue, dispatcher, lifecycleStore) {
            override suspend fun start() = error("Do not call start() in tests")
        }
    }

    private fun persistEvent(ref: UUID) {
        val e = TestEvent().withReference(ref)
        store.persist(e.setMetadata(Metadata()))
    }

    @Test
    @DisplayName("""
    Når polleren finner nye events
    Hvis dispatch skjer normalt
    Så skal lastSeenTime oppdateres og dispatcheren få én dispatch
    """)
    fun pollerUpdatesLastSeenTimeWhenDispatchHappens() = runTest(testDispatcher) {
        val ref = UUID.randomUUID()

        persistEvent(ref)

        poller.pollOnce()
        advanceUntilIdle()

        assertThat(poller.lastSeenTime).isGreaterThan(Instant.EPOCH)
        assertThat(dispatcher.dispatched).hasSize(1)
    }

    class AlwaysBusyDispatchQueue(private val lifecycleStore: LifecycleStore) : SequenceDispatchQueue(8, CoroutineScope(Dispatchers.Default), lifecycleStore) {
        override fun isProcessing(referenceId: UUID): Boolean = true
        override fun dispatch(referenceId: UUID, history: List<Event>, newEvents: List<Event>, dispatcher: EventDispatcher) = null
    }

    @Test
    @DisplayName("""
        Når køen er travel og ikke kan dispatch'e
        Hvis polleren likevel ser nye events
        Så skal lastSeenTime flyttes frem til eventets timestamp (slik at polleren ikke livelock'er)
    """)
    fun pollerUpdatesLastSeenTimeEvenWhenQueueBusy() = runTest {
        val ref = UUID.randomUUID()
        val t = Instant.parse("2026-01-22T12:00:00Z")

        store.persistAt(TestEvent().withReference(ref), t)

        val busyQueue = AlwaysBusyDispatchQueue(lifecycleStore)
        val poller = object : EventPollerImplementation(store, busyQueue, dispatcher, lifecycleStore) {}

        poller.pollOnce()
        advanceUntilIdle()

        // Etter livelock-fixen skal lastSeenTime være *etter* eventet
        assertThat(poller.lastSeenTime).isGreaterThanOrEqualTo(t)
    }

    @Test
    @DisplayName("""
    Når polleren kjører flere ganger uten nye events
    Hvis første poll allerede dispatch'et eventet
    Så skal polleren ikke dispatch'e samme event to ganger
    """)
    fun pollerDoesNotDoubleDispatch() = runTest(testDispatcher) {
        val ref = UUID.randomUUID()

        persistEvent(ref)

        poller.pollOnce()
        advanceUntilIdle()

        poller.pollOnce()
        advanceUntilIdle()

        assertThat(dispatcher.dispatched).hasSize(1)
    }

    @Test
    @DisplayName("""
    Når flere referenceId-er har nye events
    Hvis polleren kjører én runde
    Så skal begge referenceId-er dispatch'es
    """)
    fun pollerHandlesMultipleReferenceIds() = runTest(testDispatcher) {
        val refA = UUID.randomUUID()
        val refB = UUID.randomUUID()

        persistEvent(refA)
        persistEvent(refB)

        poller.pollOnce()
        advanceUntilIdle()

        assertThat(dispatcher.dispatched).hasSize(2)
    }

    @Test
    @DisplayName("""
    Når to events har identisk timestamp
    Hvis polleren leser dem i samme poll
    Så skal begge referenceId-er dispatch'es
    """)
    fun pollerHandlesIdenticalTimestamps() = runTest(testDispatcher) {
        val refA = UUID.randomUUID()
        val refB = UUID.randomUUID()

        persistEvent(refA)
        persistEvent(refB)

        poller.pollOnce()
        advanceUntilIdle()

        assertThat(dispatcher.dispatched).hasSize(2)
    }

    @Test
    @DisplayName("""
    Når polleren ikke finner nye events
    Hvis pollOnce kjøres
    Så skal backoff økes
    """)
    fun pollerBacksOffWhenNoNewEvents() = runTest(testDispatcher) {
        val before = poller.backoff

        poller.pollOnce()
        advanceUntilIdle()

        assertThat(poller.backoff).isGreaterThan(before)
    }

    class ControlledDispatchQueue(
        private val scope: CoroutineScope,
        private val lifecycleStore: LifecycleStore
    ) : SequenceDispatchQueue(8, scope, lifecycleStore) {

        val busyRefs = mutableSetOf<UUID>()

        override fun isProcessing(referenceId: UUID): Boolean =
            referenceId in busyRefs

        override fun dispatch(referenceId: UUID, history: List<Event>, newEvents: List<Event>, dispatcher: EventDispatcher): Job {
            return scope.launch {
                dispatcher.dispatch(referenceId, history, newEvents)
            }
        }
    }

    @Test
    @DisplayName("""
    Når køen er travel for en referenceId
    Hvis nye events ankommer mens køen er travel
    Så skal polleren prosessere alle events når køen blir ledig
    """)
    fun pollerProcessesEventsArrivingWhileQueueBusy() = runTest(testDispatcher) {
        val ref = UUID.randomUUID()

        persistEvent(ref)

        val controlledQueue = ControlledDispatchQueue(scope, lifecycleStore)
        controlledQueue.busyRefs += ref

        val poller = object : EventPollerImplementation(store, controlledQueue, dispatcher, lifecycleStore) {}

        // Poll #1: busy → no dispatch
        poller.pollOnce()
        advanceUntilIdle()

        assertThat(dispatcher.dispatched).isEmpty()

        // Now free
        controlledQueue.busyRefs.clear()

        // Add new event
        persistEvent(ref)

        // Poll #2: should dispatch both events
        poller.pollOnce()
        advanceUntilIdle()

        assertThat(dispatcher.dispatched).hasSize(1)
        assertThat(dispatcher.dispatched.single().second).hasSize(2)
    }

    @Test
    @DisplayName("""
        Når én referanse ligger foran i tid
        Hvis en annen referanse får events med nyere timestamp
        Så skal polleren hente begge (ingen watermark-skew mellom refs, lastSeenTime flyttes frem til eventets timestamp)
    """)
    fun pollerDoesNotSkipLaggingReferenceWithNewerEvents() = runTest(testDispatcher) {
        val refA = UUID.randomUUID()
        val refB = UUID.randomUUID()

        // A får et event først
        val tA = Instant.parse("2026-01-22T12:00:00Z")
        store.persistAt(TestEvent().withReference(refA), tA)

        poller.pollOnce()
        advanceUntilIdle()

        assertThat(poller.lastSeenTime).isGreaterThanOrEqualTo(tA)

        // B får et event med *nyere* timestamp enn lastSeenTime
        val tB = Instant.parse("2026-01-22T12:05:00Z")
        store.persistAt(TestEvent().withReference(refB), tB)

        poller.pollOnce()
        advanceUntilIdle()

        assertThat(dispatcher.dispatched.any { it.first == refB }).isTrue()
    }

    @Test
    @DisplayName("""
        Når lastSeenTime er flyttet frem av en referanse
        Hvis en ny referanse dukker opp med nyere timestamp
        Så skal polleren hente den nye referansen korrekt
    """)
    fun pollerHandlesNewReferenceAfterLastSeenTimeMoves() = runTest(testDispatcher) {
        val refA = UUID.randomUUID()
        val refC = UUID.randomUUID()

        // A får event → flytter lastSeenTime frem
        val tA = Instant.parse("2026-01-22T12:00:00Z")
        store.persistAt(TestEvent().withReference(refA), tA)

        poller.pollOnce()
        advanceUntilIdle()

        // C får event med nyere timestamp
        val tC = Instant.parse("2026-01-22T12:05:00Z")
        store.persistAt(TestEvent().withReference(refC), tC)

        poller.pollOnce()
        advanceUntilIdle()

        assertThat(dispatcher.dispatched.any { it.first == refC }).isTrue()
    }

    @Test
    @DisplayName("""
        Når polleren har kjørt og lastSeenTime er oppdatert
        Hvis et event ankommer rett etter poll-runden
        Så skal polleren hente det i neste runde
    """)
    fun pollerDoesNotMissEventsArrivingBetweenPolls() = runTest(testDispatcher) {
        val ref = UUID.randomUUID()

        // Første event
        val t1 = Instant.parse("2026-01-22T12:00:00Z")
        store.persistAt(TestEvent().withReference(ref), t1)

        poller.pollOnce()
        advanceUntilIdle()

        // Event som ankommer rett etter poll
        val t2 = t1.plusMillis(1)
        store.persistAt(TestEvent().withReference(ref), t2)

        poller.pollOnce()
        advanceUntilIdle()

        // Skal ha to dispatches for ref
        val eventsForRef = dispatcher.dispatched.filter { it.first == ref }
        assertThat(eventsForRef).hasSize(2)
    }


}
