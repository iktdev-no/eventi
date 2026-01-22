package no.iktdev.eventi.events.poller

import kotlinx.coroutines.test.*
import no.iktdev.eventi.InMemoryEventStore
import no.iktdev.eventi.events.EventTypeRegistry
import no.iktdev.eventi.events.FakeDispatcher
import no.iktdev.eventi.events.RunSimulationTestTest
import no.iktdev.eventi.events.TestEvent
import no.iktdev.eventi.models.Metadata
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.UUID

class PollerStartLoopTest {

    private lateinit var store: InMemoryEventStore
    private lateinit var dispatcher: FakeDispatcher
    private lateinit var testDispatcher: TestDispatcher
    private lateinit var scope: TestScope
    private lateinit var queue: RunSimulationTestTest.ControlledDispatchQueue
    private lateinit var poller: TestablePoller

    @BeforeEach
    fun setup() {
        store = InMemoryEventStore()
        dispatcher = FakeDispatcher()
        testDispatcher = StandardTestDispatcher()
        scope = TestScope(testDispatcher)
        queue = RunSimulationTestTest.ControlledDispatchQueue(scope)
        EventTypeRegistry.register(TestEvent::class.java)

        poller = TestablePoller(store, queue, dispatcher, scope)
    }

    private fun persistAt(ref: UUID, time: LocalDateTime) {
        val e = TestEvent().withReference(ref).setMetadata(Metadata())
        store.persistAt(e, time)
    }

    @Test
    fun `poller does not spin when no events exist`() = runTest {
        val startBackoff = poller.backoff

        poller.startFor(iterations = 10)

        assertThat(poller.backoff).isGreaterThan(startBackoff)
        assertThat(dispatcher.dispatched).isEmpty()
    }

    @Test
    fun `poller increases backoff exponentially`() = runTest {
        val b1 = poller.backoff

        poller.startFor(iterations = 1)
        val b2 = poller.backoff

        poller.startFor(iterations = 1)
        val b3 = poller.backoff

        assertThat(b2).isGreaterThan(b1)
        assertThat(b3).isGreaterThan(b2)
    }

    @Test
    fun `poller resets backoff when events appear`() = runTest {
        poller.startFor(iterations = 5)
        val before = poller.backoff

        val ref = UUID.randomUUID()
        persistAt(ref, LocalDateTime.now())

        poller.startFor(iterations = 1)

        assertThat(poller.backoff).isEqualTo(java.time.Duration.ofSeconds(2))
    }

    @Test
    fun `poller processes events that arrive while sleeping`() = runTest {
        val ref = UUID.randomUUID()

        poller.startFor(iterations = 3)

        persistAt(ref, LocalDateTime.now())

        poller.startFor(iterations = 1)

        assertThat(dispatcher.dispatched).hasSize(1)
    }

    @Test
    fun `poller does not lose events under concurrency`() = runTest {
        val ref = UUID.randomUUID()

        queue.busyRefs += ref

        persistAt(ref, LocalDateTime.now())

        poller.startFor(iterations = 1)

        assertThat(dispatcher.dispatched).isEmpty()

        queue.busyRefs.clear()

        poller.startFor(iterations = 1)

        assertThat(dispatcher.dispatched).hasSize(1)
    }
}
