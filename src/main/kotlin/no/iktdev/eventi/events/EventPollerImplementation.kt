package no.iktdev.eventi.events

import kotlinx.coroutines.delay
import mu.KotlinLogging
import no.iktdev.eventi.ZDS.toEvent
import no.iktdev.eventi.stores.EventStore
import java.time.Duration
import java.time.LocalDateTime
import kotlin.collections.iterator

abstract class EventPollerImplementation(
    private val eventStore: EventStore,
    private val dispatchQueue: SequenceDispatchQueue,
    private val dispatcher: EventDispatcher
) {
    var lastSeenTime: LocalDateTime = LocalDateTime.of(1970, 1, 1, 0, 0)
    open var backoff = Duration.ofSeconds(2)
        protected set
    private val maxBackoff = Duration.ofMinutes(1)
    private val log = KotlinLogging.logger {}

    open suspend fun start() {
        log.info { "EventPoller starting with initial backoff=$backoff" }
        while (true) {
            try {
                pollOnce()
            } catch (e: Exception) {
                e.printStackTrace()
                delay(backoff.toMillis())
                backoff = backoff.multipliedBy(2).coerceAtMost(maxBackoff)
            }
        }
    }

    suspend fun pollOnce() {
        val pollStartedAt = LocalDateTime.now()
        log.debug { "Polling for new events" }
        val newPersisted = eventStore.getPersistedEventsAfter(lastSeenTime)

        if (newPersisted.isEmpty()) {
            log.debug { "No new events found. Backing off for $backoff" }
            delay(backoff.toMillis())
            backoff = backoff.multipliedBy(2).coerceAtMost(maxBackoff)
            return
        }

        backoff = Duration.ofSeconds(2)

        val grouped = newPersisted.groupBy { it.referenceId }

        // Samle persistedAt KUN for referanser vi faktisk dispatch’et
        val processedTimes = mutableListOf<LocalDateTime>()

        for ((referenceId, _) in grouped) {
            if (dispatchQueue.isProcessing(referenceId)) {
                log.debug { "Skipping dispatch for $referenceId as it is already being processed" }
                continue
            }

            val fullLog = eventStore.getPersistedEventsFor(referenceId)
            val events = fullLog.mapNotNull { it.toEvent() }
            processedTimes += fullLog.map { it.persistedAt }
            dispatchQueue.dispatch(referenceId, events, dispatcher)
        }

        if (processedTimes.isNotEmpty()) {
            val maxPersistedAt = processedTimes.max()
            val newLastSeen = minOf(pollStartedAt, maxPersistedAt).plusNanos(1)
            log.debug { "Updating lastSeenTime from $lastSeenTime to $newLastSeen" }
            lastSeenTime = newLastSeen
        } else {
            // Ingen referanser ble dispatch’et → IKKE oppdater lastSeenTime
            log.debug { "No dispatches performed; lastSeenTime remains $lastSeenTime" }
        }
    }


}
