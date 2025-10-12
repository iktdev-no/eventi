package no.iktdev.eventi.events

import kotlinx.coroutines.delay
import no.iktdev.eventi.ZDS.toEvent
import no.iktdev.eventi.stores.EventStore
import java.time.Duration
import java.time.LocalDateTime
import java.util.UUID
import kotlin.collections.iterator

abstract class AbstractEventPoller(
    private val eventStore: EventStore,
    private val dispatchQueue: SequenceDispatchQueue,
    private val dispatcher: EventDispatcher
) {
    var lastSeenTime: LocalDateTime = LocalDateTime.MIN
    open var backoff = Duration.ofSeconds(2)
        protected set
    private val maxBackoff = Duration.ofMinutes(1)


    suspend fun start() {
        while (true) {
            pollOnce()
        }
    }

    suspend fun pollOnce() {
        val newPersisted = eventStore.getPersistedEventsAfter(lastSeenTime)

        if (newPersisted.isEmpty()) {
            delay(backoff.toMillis())
            backoff = backoff.multipliedBy(2).coerceAtMost(maxBackoff)
            return
        }

        backoff = Duration.ofSeconds(2)

        val grouped = newPersisted.groupBy { it.referenceId }

        for ((referenceId, _) in grouped) {
            if (dispatchQueue.isProcessing(referenceId)) continue

            val fullLog = eventStore.getPersistedEventsFor(referenceId)
            val events = fullLog.mapNotNull { it.toEvent() }

            dispatchQueue.dispatch(referenceId, events, dispatcher)
            lastSeenTime = fullLog.maxOf { it.persistedAt }
        }
    }

}
