@file:OptIn(ExperimentalCoroutinesApi::class)

package no.iktdev.eventi.events

import kotlinx.coroutines.*
import kotlinx.coroutines.test.*
import no.iktdev.eventi.InMemoryEventStore
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.Metadata
import org.junit.jupiter.api.DisplayName
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap


class FakeDispatchQueue(
    private val scope: CoroutineScope
) : SequenceDispatchQueue(8, scope) {

    private val active = ConcurrentHashMap.newKeySet<UUID>()

    override fun isProcessing(referenceId: UUID): Boolean = referenceId in active

    override fun dispatch(referenceId: UUID, events: List<Event>, dispatcher: EventDispatcher): Job {
        active.add(referenceId)
        return scope.launch {
            try {
                dispatcher.dispatch(referenceId, events)
            } finally {
                active.remove(referenceId)
            }
        }
    }
}


class FakeDispatcher : EventDispatcher(InMemoryEventStore()) {

    val dispatched = mutableListOf<Pair<UUID, List<Event>>>()

    override fun dispatch(referenceId: UUID, events: List<Event>) {
        dispatched += referenceId to events
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

    private lateinit var store: InMemoryEventStore
    private lateinit var dispatcher: FakeDispatcher
    private lateinit var testDispatcher: TestDispatcher
    private lateinit var scope: CoroutineScope
    private lateinit var queue: FakeDispatchQueue
    private lateinit var poller: EventPollerImplementation

    @BeforeEach
    fun setup() {
        store = InMemoryEventStore()
        dispatcher = FakeDispatcher()
        testDispatcher = StandardTestDispatcher()
        scope = CoroutineScope(testDispatcher)
        queue = FakeDispatchQueue(scope)
        EventTypeRegistry.register(TestEvent::class.java)
        poller = object : EventPollerImplementation(store, queue, dispatcher) {
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

    class AlwaysBusyDispatchQueue : SequenceDispatchQueue(8, CoroutineScope(Dispatchers.Default)) {
        override fun isProcessing(referenceId: UUID): Boolean = true
        override fun dispatch(referenceId: UUID, events: List<Event>, dispatcher: EventDispatcher) = null
    }

    @Test
    @DisplayName("""
    Når køen er travel og ikke kan dispatch'e
    Hvis polleren likevel ser nye events
    Så skal lastSeenTime fortsatt oppdateres (livelock-fix)
    """)
    fun pollerUpdatesLastSeenTimeEvenWhenQueueBusy() = runTest {
        val ref = UUID.randomUUID()
        val t = Instant.parse("2026-01-22T12:00:00Z")

        store.persistAt(TestEvent().withReference(ref), t)

        val busyQueue = AlwaysBusyDispatchQueue()
        val poller = object : EventPollerImplementation(store, busyQueue, dispatcher) {}

        poller.pollOnce()
        advanceUntilIdle()

        // Etter livelock-fixen skal lastSeenTime være *etter* eventet
        assertThat(poller.lastSeenTime)
            .isGreaterThan(t)
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
        private val scope: CoroutineScope
    ) : SequenceDispatchQueue(8, scope) {

        val busyRefs = mutableSetOf<UUID>()

        override fun isProcessing(referenceId: UUID): Boolean =
            referenceId in busyRefs

        override fun dispatch(referenceId: UUID, events: List<Event>, dispatcher: EventDispatcher): Job {
            return scope.launch {
                dispatcher.dispatch(referenceId, events)
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
        val t1 = Instant.parse("2026-01-22T12:00:00Z")

        persistEvent(ref)

        val controlledQueue = ControlledDispatchQueue(scope)
        controlledQueue.busyRefs += ref

        val poller = object : EventPollerImplementation(store, controlledQueue, dispatcher) {}

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
}
