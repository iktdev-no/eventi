package no.iktdev.eventi.events

import kotlinx.coroutines.delay
import mu.KotlinLogging
import no.iktdev.eventi.MyTime
import no.iktdev.eventi.models.store.PersistedEvent
import no.iktdev.eventi.serialization.ZDS.toEvent
import no.iktdev.eventi.stores.EventStore
import java.time.Duration
import java.time.Instant
import java.util.UUID

abstract class EventPollerImplementation(
    private val eventStore: EventStore,
    private val dispatchQueue: SequenceDispatchQueue,
    private val dispatcher: EventDispatcher
) {
    private val log = KotlinLogging.logger {}

    /**
     * Per-reference watermark:
     *  - first = last seen persistedAt
     *  *  - second = last seen persistedId
     */
    protected val refWatermark = mutableMapOf<UUID, Pair<Instant, Long>>()

    protected open fun updateWatermark(ref: UUID, value: Pair<Instant, Long>) {
        refWatermark[ref] = value
    }

    /**
     * Global scan hint (timestamp only).
     * Used to avoid scanning entire table every time.
     */
    var lastSeenTime: Instant = Instant.EPOCH

    open var backoff = Duration.ofSeconds(2)
        protected set
    private val maxBackoff = Duration.ofMinutes(1)

    open suspend fun start() {
        log.info { "EventPoller starting with initial backoff=$backoff" }
        while (true) {
            try {
                pollOnce()
            } catch (e: Exception) {
                log.error(e) { "Error in poller loop" }
                delay(backoff.toMillis())
                backoff = backoff.multipliedBy(2).coerceAtMost(maxBackoff)
            }
        }
    }

    suspend fun pollOnce() {
        val pollStartedAt = MyTime.utcNow()
        log.debug { "🔍 Polling for new events at $pollStartedAt (scanFrom=$lastSeenTime)" }

        val newPersisted = fetchNewPersistedEvents(lastSeenTime)
        if (newPersisted.isEmpty()) {
            handleEmptyPoll()
            return
        }

        resetBackoff()
        log.debug { "📬 Found ${newPersisted.size} new events after $lastSeenTime" }

        val grouped = newPersisted.groupBy { it.referenceId }

        var anyProcessed = false
        for ((ref, eventsForRef) in grouped) {
            if (processReference(ref, eventsForRef)) {
                anyProcessed = true
            }
        }

        // Move global scan hint forward to avoid livelock
        val maxSeenThisRound = newPersisted.maxOfOrNull { it.persistedAt }
        if (maxSeenThisRound != null && maxSeenThisRound > lastSeenTime) {
            lastSeenTime = maxSeenThisRound
        }


        updateGlobalWatermark(anyProcessed)
    }

    private suspend fun fetchNewPersistedEvents(scanFrom: Instant) =
        eventStore.getPersistedEventsAfter(scanFrom)

    private suspend fun handleEmptyPoll() {
        log.debug { "😴 No new events found. Backing off for $backoff" }
        delay(backoff.toMillis())
        backoff = backoff.multipliedBy(2).coerceAtMost(maxBackoff)
    }

    private fun resetBackoff() {
        backoff = Duration.ofSeconds(2)
    }

    /**
     * Prosesserer alle nye events for én referenceId.
     * Returnerer true hvis vi faktisk dispatch’et noe.
     */
    private fun processReference(ref: UUID, eventsForRef: List<PersistedEvent>): Boolean {
        val (refSeenAt, refSeenId) = refWatermark[ref] ?: (Instant.EPOCH to 0L)

        // Filter new events using (timestamp, id) ordering
        val newForRef = eventsForRef.filter { ev ->
            ev.persistedAt > refSeenAt ||
                    (ev.persistedAt == refSeenAt && ev.id > refSeenId)
        }

        if (newForRef.isEmpty()) {
            log.debug { "🧊 No new events for $ref since ($refSeenAt, id=$refSeenId)" }
            return false
        }

        // If ref is busy, advance watermark but skip dispatch
        if (dispatchQueue.isProcessing(ref)) {
            val maxEvent = newForRef.maxWith(
                compareBy({ it.persistedAt }, { it.id })
            )
            log.debug {
                log.debug { "⏳ $ref is busy — deferring ${newForRef.size} events" }
            }
            return false
        }

        // Fetch full sequence for dispatch
        val fullLog = eventStore.getPersistedEventsFor(ref)
        val history = fullLog.mapNotNull { it.toEvent() }
        val newEvents = newForRef.mapNotNull { it.toEvent() }

        log.debug { "🚀 Dispatching ${history.size} events for $ref (new=${newEvents.size})" }

        dispatchQueue.dispatch(ref, history, newEvents, dispatcher)

        // Update watermark for this reference
        val maxEvent = fullLog.maxWith(
            compareBy({ it.persistedAt }, { it.id })
        )

        val newWatermarkAt = maxEvent.persistedAt
        val newWatermarkId = maxEvent.id

        updateWatermark(ref, newWatermarkAt to newWatermarkId)

        log.debug { "⏩ Updated watermark for $ref → ($newWatermarkAt, id=$newWatermarkId)" }
        return true
    }

    /**
     * Oppdaterer global scan hint basert på per-ref watermarks.
     * Global watermark = min(persistedAt over alle refs).
     */
    private fun updateGlobalWatermark(anyProcessed: Boolean) {
        if (anyProcessed) {
            log.debug { "📉 Global scanFrom updated → $lastSeenTime (anyProcessed=true)" }
        } else {
            log.debug { "🔁 No refs processed — keeping global scanFrom at $lastSeenTime" }
        }
    }
}
