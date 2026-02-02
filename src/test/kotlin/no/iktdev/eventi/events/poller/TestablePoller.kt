package no.iktdev.eventi.events.poller

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.TestScope
import no.iktdev.eventi.events.EventDispatcher
import no.iktdev.eventi.events.EventPollerImplementation
import no.iktdev.eventi.events.SequenceDispatchQueue
import no.iktdev.eventi.stores.EventStore
import java.time.Instant
import java.util.*

@ExperimentalCoroutinesApi
class TestablePoller(
    eventStore: EventStore,
    dispatchQueue: SequenceDispatchQueue,
    dispatcher: EventDispatcher,
    val scope: TestScope
) : EventPollerImplementation(eventStore, dispatchQueue, dispatcher), WatermarkDebugView {

    suspend fun startFor(iterations: Int) {
        repeat(iterations) {
            try {
                pollOnce()
            } catch (_: Exception) {
                // same as prod
            }

            // Simuler delay(backoff)
            scope.testScheduler.advanceTimeBy(backoff.toMillis())
        }
    }

    override fun watermarkFor(ref: UUID): Pair<Instant, Long>? {
        return refWatermark[ref]
    }

    override fun setWatermarkFor(ref: UUID, time: Instant, id: Long) {
        refWatermark[ref] = time to id
    }
}

interface WatermarkDebugView {
    fun watermarkFor(ref: UUID): Pair<Instant, Long>?
    fun setWatermarkFor(ref: UUID, time: Instant, id: Long)
}

