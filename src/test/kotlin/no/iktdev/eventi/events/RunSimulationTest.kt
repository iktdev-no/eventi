package no.iktdev.eventi.events

import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.*
import kotlinx.coroutines.test.*
import no.iktdev.eventi.InMemoryEventStore
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.UUID
import kotlinx.coroutines.*
import no.iktdev.eventi.ZDS.toPersisted
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.Metadata
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


class FakeClock(var now: LocalDateTime) {
    fun advanceSeconds(sec: Long) {
        now = now.plusSeconds(sec)
    }
}


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

    private fun persistEvent(ref: UUID, time: LocalDateTime) {
        val e = TestEvent().withReference(ref)
        store.persist(e.setMetadata(Metadata()))
    }

    @Test
    fun `poller updates lastSeenTime when dispatch happens`() = runTest(testDispatcher) {
        val ref = UUID.randomUUID()
        val t = LocalDateTime.of(2026, 1, 22, 12, 0, 0)

        persistEvent(ref, t)

        poller.pollOnce()
        advanceUntilIdle()

        assertThat(poller.lastSeenTime).isAfter(LocalDateTime.of(1970,1,1,0,0))
        assertThat(dispatcher.dispatched).hasSize(1)
    }


    class AlwaysBusyDispatchQueue : SequenceDispatchQueue(8, CoroutineScope(Dispatchers.Default)) {
        override fun isProcessing(referenceId: UUID): Boolean = true
        override fun dispatch(referenceId: UUID, events: List<Event>, dispatcher: EventDispatcher) = null
    }

    @Test
    fun `poller does NOT update lastSeenTime when queue is busy`() = runTest {
        val ref = UUID.randomUUID()
        val t = LocalDateTime.of(2026,1,22,12,0,0)

        store.persistAt(TestEvent().withReference(ref), t)

        val busyQueue = AlwaysBusyDispatchQueue()

        val poller = object : EventPollerImplementation(store, busyQueue, dispatcher) {}

        poller.pollOnce()
        advanceUntilIdle()

        assertThat(poller.lastSeenTime)
            .isEqualTo(LocalDateTime.of(1970,1,1,0,0))
    }



    @Test
    fun `poller does not double-dispatch`() = runTest(testDispatcher) {
        val ref = UUID.randomUUID()
        val t = LocalDateTime.of(2026, 1, 22, 12, 0, 0)

        persistEvent(ref, t)

        poller.pollOnce()
        advanceUntilIdle()

        poller.pollOnce()
        advanceUntilIdle()

        assertThat(dispatcher.dispatched).hasSize(1)
    }

    @Test
    fun `poller handles multiple referenceIds`() = runTest(testDispatcher) {
        val refA = UUID.randomUUID()
        val refB = UUID.randomUUID()
        val t = LocalDateTime.of(2026, 1, 22, 12, 0, 0)

        persistEvent(refA, t)
        persistEvent(refB, t.plusSeconds(1))

        poller.pollOnce()
        advanceUntilIdle()

        assertThat(dispatcher.dispatched).hasSize(2)
    }

    @Test
    fun `poller handles identical timestamps`() = runTest(testDispatcher) {
        val refA = UUID.randomUUID()
        val refB = UUID.randomUUID()
        val t = LocalDateTime.of(2026, 1, 22, 12, 0, 0)

        persistEvent(refA, t)
        persistEvent(refB, t)

        poller.pollOnce()
        advanceUntilIdle()

        assertThat(dispatcher.dispatched).hasSize(2)
    }

    @Test
    fun `poller backs off when no new events`() = runTest(testDispatcher) {
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
    fun `poller processes events arriving while queue is busy`() = runTest(testDispatcher) {
        val ref = UUID.randomUUID()
        val t1 = LocalDateTime.of(2026, 1, 22, 12, 0, 0)
        val t2 = t1.plusSeconds(5)

        persistEvent(ref, t1)

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
        persistEvent(ref, t2)

        // Poll #2: should dispatch both events
        poller.pollOnce()
        advanceUntilIdle()

        assertThat(dispatcher.dispatched).hasSize(1)
        assertThat(dispatcher.dispatched.single().second).hasSize(2)
    }


}
