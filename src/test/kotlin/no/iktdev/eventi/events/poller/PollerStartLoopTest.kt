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
import org.junit.jupiter.api.DisplayName
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

    private fun t(seconds: Long): LocalDateTime =
        LocalDateTime.of(2024, 1, 1, 12, 0).plusSeconds(seconds)


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

    @Test
    fun `poller does not dispatch when no new events for ref`() = runTest {
        val ref = UUID.randomUUID()

        // E1
        persistAt(ref, t(0))

        poller.startFor(iterations = 1)
        assertThat(dispatcher.dispatched).hasSize(1)

        // Ingen nye events
        poller.startFor(iterations = 3)

        // Fremdeles bare én dispatch
        assertThat(dispatcher.dispatched).hasSize(1)
    }

    @Test
    fun `event arriving while ref is busy is not lost`() = runTest {
        val ref = UUID.randomUUID()

        persistAt(ref, t(0))
        persistAt(ref, t(5))

        // Første poll: dispatcher E1+E2
        poller.startFor(iterations = 1)
        assertThat(dispatcher.dispatched).hasSize(1)

        // Marker ref som busy
        queue.busyRefs += ref

        // E3 kommer mens ref er busy
        persistAt(ref, t(10))

        // Polleren skal IKKE dispatch’e nå
        poller.startFor(iterations = 2)
        assertThat(dispatcher.dispatched).hasSize(1)

        // Frigjør ref
        queue.busyRefs.clear()

        // Nå skal E3 bli dispatch’et
        poller.startFor(iterations = 1)

        assertThat(dispatcher.dispatched).hasSize(2)
        val events = dispatcher.dispatched.last().second
        assertThat(events).hasSize(3)
    }

    @Test
    fun `busy ref does not block dispatch of other refs`() = runTest {
        val refA = UUID.randomUUID()
        val refB = UUID.randomUUID()

        persistAt(refA, t(0))
        persistAt(refB, t(0))

        // Marker A som busy
        queue.busyRefs += refA

        poller.startFor(iterations = 1)

        // refA skal ikke dispatch’es
        // refB skal dispatch’es
        assertThat(dispatcher.dispatched).hasSize(1)
        assertThat(dispatcher.dispatched.first().first).isEqualTo(refB)
    }

    @Test
    fun `watermark advances only for refs that were processed`() = runTest {
        val refA = UUID.randomUUID()
        val refB = UUID.randomUUID()

        persistAt(refA, t(0))
        persistAt(refB, t(0))

        // Første poll: begge refs blir dispatch’et
        poller.startFor(iterations = 1)

        val wmA1 = poller.watermarkFor(refA!!)
        val wmB1 = poller.watermarkFor(refB!!)

        // Marker A som busy
        queue.busyRefs += refA

        // Nye events for begge refs
        persistAt(refA, t(10))
        persistAt(refB, t(10))

        poller.startFor(iterations = 1)

        // A skal IKKE ha flyttet watermark
        assertThat(poller.watermarkFor(refA)).isEqualTo(wmA1)

        // B skal ha flyttet watermark
        assertThat(poller.watermarkFor(refB)).isAfter(wmB1)
    }

    @DisplayName("🍌 Bananastesten™ — stress-test av watermark, busy refs og dispatch-semantikk")
    @Test
    fun `stress test with many refs random busy states and interleaved events`() = runTest {
        val refs = List(50) { UUID.randomUUID() }
        val eventCountPerRef = 20

        // 1. Initial events
        refs.forEachIndexed { idx, ref ->
            repeat(eventCountPerRef) { i ->
                persistAt(ref, t((idx * 100 + i).toLong()))
            }
        }

        // 2. Random busy refs
        val busyRefs = refs.shuffled().take(10).toSet()
        queue.busyRefs += busyRefs

        // 3. First poll: only non-busy refs dispatch
        poller.startFor(iterations = 1)

        val dispatchedFirstRound = dispatcher.dispatched.groupBy { it.first }
        val dispatchedRefsFirstRound = dispatchedFirstRound.keys
        val expectedFirstRound = refs - busyRefs

        assertThat(dispatchedRefsFirstRound)
            .containsExactlyInAnyOrder(*expectedFirstRound.toTypedArray())

        // 4. Add new events for all refs
        refs.forEachIndexed { idx, ref ->
            persistAt(ref, t((10_000 + idx).toLong()))
        }

        // 5. Second poll: only non-busy refs dispatch again
        poller.startFor(iterations = 1)

        val dispatchedSecondRound = dispatcher.dispatched.groupBy { it.first }
        val secondRoundCounts = dispatchedSecondRound.mapValues { (_, v) -> v.size }

        // Non-busy refs should now have 2 dispatches total
        expectedFirstRound.forEach { ref ->
            assertThat(secondRoundCounts[ref]).isEqualTo(2)
        }

        // Busy refs should still have 0 dispatches
        busyRefs.forEach { ref ->
            assertThat(secondRoundCounts).doesNotContainKey(ref)
        }

        // 6. Free busy refs
        queue.busyRefs.clear()

        // 7. Third poll: busy refs dispatch their backlog
        poller.startFor(iterations = 1)

        val dispatchedThirdRound = dispatcher.dispatched.groupBy { it.first }
        val thirdRoundCounts = dispatchedThirdRound.mapValues { (_, v) -> v.size }

        refs.forEach { ref ->
            if (ref in busyRefs) {
                // Busy refs: 1 dispatch total (only in third poll)
                assertThat(thirdRoundCounts[ref]).isEqualTo(1)
            } else {
                // Non-busy refs: 2 dispatches total (first + second)
                assertThat(thirdRoundCounts[ref]).isEqualTo(2)
            }
        }

        // 8. No ref should have more than 2 dispatches (no spinning)
        refs.forEach { ref ->
            assertThat(thirdRoundCounts[ref]).isLessThanOrEqualTo(2)
        }

        // 9. Verify all refs processed all unique events
        refs.forEach { ref ->
            val uniqueEvents = dispatchedThirdRound[ref]!!
                .flatMap { it.second }
                .distinctBy { it.eventId }

            assertThat(uniqueEvents).hasSize(eventCountPerRef + 1)
        }
    }




}
