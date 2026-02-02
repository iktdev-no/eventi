package no.iktdev.eventi.events

import kotlinx.coroutines.delay
import mu.KotlinLogging
import no.iktdev.eventi.MyTime
import no.iktdev.eventi.ZDS.toEvent
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
     *  - second = last seen persistedId
     */
    protected val refWatermark = mutableMapOf<UUID, Pair<Instant, Long>>()

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
        log.debug { "🔍 Polling for new events" }

        // Determine global scan start
        val minRefTs = refWatermark.values.minOfOrNull { it.first }
        val scanFrom = when (minRefTs) {
            null -> lastSeenTime
            else -> maxOf(lastSeenTime, minRefTs)
        }

        val newPersisted = eventStore.getPersistedEventsAfter(scanFrom)

        if (newPersisted.isEmpty()) {
            log.debug { "😴 No new events found. Backing off for $backoff" }
            delay(backoff.toMillis())
            backoff = backoff.multipliedBy(2).coerceAtMost(maxBackoff)
            return
        }

        // Reset backoff
        backoff = Duration.ofSeconds(2)
        log.debug { "📬 Found ${newPersisted.size} new events after $scanFrom" }

        val grouped = newPersisted.groupBy { it.referenceId }
        var anyProcessed = false

        // Track highest persistedAt seen globally this round
        val maxPersistedThisRound = newPersisted.maxOf { it.persistedAt }

        for ((ref, eventsForRef) in grouped) {
            val (refSeenAt, refSeenId) = refWatermark[ref] ?: (Instant.EPOCH to 0L)

            // Filter new events using (timestamp, id) ordering
            val newForRef = eventsForRef.filter { ev ->
                ev.persistedAt > refSeenAt ||
                        (ev.persistedAt == refSeenAt && ev.id > refSeenId)
            }

            if (newForRef.isEmpty()) {
                log.debug { "🧊 No new events for $ref since ($refSeenAt, id=$refSeenId)" }
                continue
            }

            // If ref is busy, skip dispatch
            if (dispatchQueue.isProcessing(ref)) {
                log.debug { "⏳ $ref is busy — deferring ${newForRef.size} events" }
                continue
            }

            // Fetch full sequence for dispatch
            val fullLog = eventStore.getPersistedEventsFor(ref)
            val events = fullLog.mapNotNull { it.toEvent() }

            log.debug { "🚀 Dispatching ${events.size} events for $ref" }
            dispatchQueue.dispatch(ref, events, dispatcher)

            // Update watermark for this reference
            val maxEvent = newForRef.maxWith(
                compareBy({ it.persistedAt }, { it.id })
            )

            val newWatermarkAt = minOf(pollStartedAt, maxEvent.persistedAt)
            val newWatermarkId = maxEvent.id

            refWatermark[ref] = newWatermarkAt to newWatermarkId
            anyProcessed = true

            log.debug { "⏩ Updated watermark for $ref → ($newWatermarkAt, id=$newWatermarkId)" }
        }

        // Update global scan hint
        val newLastSeen = maxOf(
            lastSeenTime,
            maxPersistedThisRound.plusNanos(1)
        )

        if (anyProcessed) {
            val minRef = refWatermark.values.minOfOrNull { it.first }
            lastSeenTime = when (minRef) {
                null -> newLastSeen
                else -> maxOf(newLastSeen, minRef)
            }
            log.debug { "📉 Global scanFrom updated → $lastSeenTime (anyProcessed=true)" }
        } else {
            lastSeenTime = newLastSeen
            log.debug { "🔁 No refs processed — advancing global scanFrom to $lastSeenTime" }
        }
    }
}
