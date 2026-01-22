package no.iktdev.eventi.events.poller

import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import no.iktdev.eventi.events.EventDispatcher
import no.iktdev.eventi.events.EventPollerImplementation
import no.iktdev.eventi.events.SequenceDispatchQueue
import no.iktdev.eventi.stores.EventStore
import java.time.LocalDateTime
import java.util.UUID

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

    override fun watermarkFor(ref: UUID): LocalDateTime? {
        return refWatermark[ref]?.let {
            return it
        }
    }


}
interface WatermarkDebugView {
    fun watermarkFor(ref: UUID): LocalDateTime?
}
