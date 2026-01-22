package no.iktdev.eventi.events.poller

import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import no.iktdev.eventi.events.EventDispatcher
import no.iktdev.eventi.events.EventPollerImplementation
import no.iktdev.eventi.events.SequenceDispatchQueue
import no.iktdev.eventi.stores.EventStore

class TestablePoller(
    eventStore: EventStore,
    dispatchQueue: SequenceDispatchQueue,
    dispatcher: EventDispatcher,
    val scope: TestScope
) : EventPollerImplementation(eventStore, dispatchQueue, dispatcher) {

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
}
