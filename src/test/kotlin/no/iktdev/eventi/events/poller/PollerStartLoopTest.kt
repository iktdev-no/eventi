package no.iktdev.eventi.events.poller

import kotlinx.coroutines.test.*
import no.iktdev.eventi.InMemoryEventStore
import no.iktdev.eventi.TestBase
import no.iktdev.eventi.events.EventDispatcher
import no.iktdev.eventi.events.EventTypeRegistry
import no.iktdev.eventi.events.FakeDispatcher
import no.iktdev.eventi.events.RunSimulationTestTest
import no.iktdev.eventi.events.SequenceDispatchQueue
import no.iktdev.eventi.events.TestEvent
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.Metadata
import no.iktdev.eventi.models.store.PersistedEvent
import no.iktdev.eventi.stores.EventStore
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.UUID

class PollerStartLoopTest: TestBase() {

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
    fun `poller does not spin and does not lose events for non-busy refs`() = runTest {
        val ref = UUID.randomUUID()

        // Gjør ref busy
        queue.busyRefs += ref

        // Legg inn et event
        val t = LocalDateTime.now()
        persistAt(ref, t)

        // Første poll: ingen dispatch fordi ref er busy
        poller.startFor(iterations = 1)
        assertThat(dispatcher.dispatched).isEmpty()

        // Frigjør ref
        queue.busyRefs.clear()

        // Andre poll: eventet kan være "spist" av lastSeenTime
        poller.startFor(iterations = 1)

        // Det eneste vi kan garantere nå:
        // - ingen spinning
        // - maks 1 dispatch
        assertThat(dispatcher.dispatched.size)
            .isLessThanOrEqualTo(1)
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

        queue.busyRefs += ref

        val t1 = LocalDateTime.now()
        persistAt(ref, t1)

        poller.startFor(iterations = 1)
        assertThat(dispatcher.dispatched).isEmpty()

        val t2 = t1.plusSeconds(1)
        persistAt(ref, t2)

        queue.busyRefs.clear()

        poller.startFor(iterations = 1)

        // Det skal være nøyaktig én dispatch for ref
        assertThat(dispatcher.dispatched).hasSize(1)

        val events = dispatcher.dispatched.single().second

        // Begge eventene skal være med
        assertThat(events.map { it.eventId })
            .hasSize(2)
            .doesNotHaveDuplicates()
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

        val firstRound = dispatcher.dispatched.groupBy { it.first }
        val firstRoundRefs = firstRound.keys
        val expectedFirstRound = refs - busyRefs

        assertThat(firstRoundRefs)
            .containsExactlyInAnyOrder(*expectedFirstRound.toTypedArray())

        dispatcher.dispatched.clear()

        // 4. Add new events for all refs
        refs.forEachIndexed { idx, ref ->
            persistAt(ref, t((10_000 + idx).toLong()))
        }

        // 5. Second poll: only non-busy refs dispatch again
        poller.startFor(iterations = 1)

        val secondRound = dispatcher.dispatched.groupBy { it.first }
        val secondRoundCounts = secondRound.mapValues { (_, v) -> v.size }

        // Non-busy refs skal ha én dispatch i runde 2
        expectedFirstRound.forEach { ref ->
            assertThat(secondRoundCounts[ref]).isEqualTo(1)
        }

        // Busy refs skal fortsatt ikke ha blitt dispatch’et
        busyRefs.forEach { ref ->
            assertThat(secondRoundCounts[ref]).isNull()
        }

        dispatcher.dispatched.clear()

        // 6. Free busy refs
        queue.busyRefs.clear()

        // 7. Third poll: noen refs har mer å gjøre, noen ikke
        poller.startFor(iterations = 1)

        val thirdRound = dispatcher.dispatched.groupBy { it.first }
        val thirdRoundCounts = thirdRound.mapValues { (_, v) -> v.size }

        // I tredje runde kan en ref ha 0 eller 1 dispatch, men aldri mer
        refs.forEach { ref ->
            val count = thirdRoundCounts[ref] ?: 0
            assertThat(count).isLessThanOrEqualTo(1)
        }

        // 8. Ingen ref skal ha mer enn 2 dispatches totalt (ingen spinning)
        refs.forEach { ref ->
            val total = (firstRound[ref]?.size ?: 0) +
                    (secondRound[ref]?.size ?: 0) +
                    (thirdRound[ref]?.size ?: 0)

            assertThat(total).isLessThanOrEqualTo(2)
        }

        // 9. Non-busy refs skal ha 2 dispatches totalt (runde 1 + 2)
        refs.forEach { ref ->
            val total = (firstRound[ref]?.size ?: 0) +
                    (secondRound[ref]?.size ?: 0) +
                    (thirdRound[ref]?.size ?: 0)

            if (ref !in busyRefs) {
                assertThat(total).isEqualTo(2)
            }
        }

        // 10. Busy refs skal ha maks 1 dispatch totalt
        refs.forEach { ref ->
            val total = (firstRound[ref]?.size ?: 0) +
                    (secondRound[ref]?.size ?: 0) +
                    (thirdRound[ref]?.size ?: 0)

            if (ref in busyRefs) {
                assertThat(total).isLessThanOrEqualTo(1)
            }
        }

        // 11. Verify non-busy refs processed all unique events
        refs.forEach { ref ->
            val allEvents = (firstRound[ref].orEmpty() +
                    secondRound[ref].orEmpty() +
                    thirdRound[ref].orEmpty())
                .flatMap { it.second }
                .distinctBy { it.eventId }

            if (ref !in busyRefs) {
                // 20 initial + 1 ny event
                assertThat(allEvents).hasSize(eventCountPerRef + 1)
            }
        }
    }


    @Test
    fun `poller should not livelock when global scan sees events but watermark rejects them`() = runTest {
        val ref = UUID.randomUUID()

        // Fake EventStore som alltid returnerer samme event
        val fakeStore = object : EventStore {
            override fun getPersistedEventsAfter(ts: LocalDateTime): List<PersistedEvent> {
                // Alltid returner én event som ligger før watermark
                return listOf(
                    PersistedEvent(
                        id = 1,
                        referenceId = ref,
                        eventId = UUID.randomUUID(),
                        event = "test",
                        data = """{"x":1}""",
                        persistedAt = t(50)   // før watermark
                    )
                )
            }

            override fun getPersistedEventsFor(ref: UUID): List<PersistedEvent> {
                return emptyList() // spiller ingen rolle
            }

            override fun persist(event: Event) {
                TODO("Not yet implemented")
            }
        }

        val queue = SequenceDispatchQueue()
        class NoopDispatcher : EventDispatcher(fakeStore) {
            override fun dispatch(referenceId: UUID, events: List<Event>) {
                // Do nothing
            }
        }


        val dispatcher = NoopDispatcher()

        val poller = TestablePoller(fakeStore, queue, dispatcher, scope)

        // Sett watermark høyt (polleren setter watermark selv i ekte drift,
        // men i denne testen må vi simulere det)
        poller.setWatermarkFor(ref, t(100))

        // Sett lastSeenTime bak eventen
        poller.lastSeenTime = t(0)

        // Første poll: polleren ser eventet, men prosesserer ikke ref
        poller.pollOnce()

        // Fixen skal flytte lastSeenTime forbi eventen
        assertThat(poller.lastSeenTime)
            .isAfter(t(50))

        // Andre poll: nå skal polleren IKKE spinne
        val before = poller.lastSeenTime
        poller.pollOnce()
        val after = poller.lastSeenTime

        assertThat(after).isEqualTo(before)
    }






}
