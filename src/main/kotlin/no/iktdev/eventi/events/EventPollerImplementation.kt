package no.iktdev.eventi.events

import kotlinx.coroutines.delay
import mu.KotlinLogging
import no.iktdev.eventi.MyTime
import no.iktdev.eventi.ZDS.toEvent
import no.iktdev.eventi.stores.EventStore
import java.time.Duration
import java.time.LocalDateTime
import java.util.UUID
import kotlin.collections.iterator

abstract class EventPollerImplementation(
    private val eventStore: EventStore,
    private val dispatchQueue: SequenceDispatchQueue,
    private val dispatcher: EventDispatcher
) {
    // Erstatter ikke lastSeenTime, men supplerer den
    protected val refWatermark = mutableMapOf<UUID, LocalDateTime>()

    // lastSeenTime brukes kun som scan hint
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
        val pollStartedAt = MyTime.UtcNow()
        log.debug { "🔍 Polling for new events" }

        // Global scan hint: start fra laveste watermark
        val scanFrom = refWatermark.values.minOrNull() ?: lastSeenTime

        val newPersisted = eventStore.getPersistedEventsAfter(scanFrom)

        if (newPersisted.isEmpty()) {
            log.debug { "😴 No new events found. Backing off for $backoff" }
            delay(backoff.toMillis())
            backoff = backoff.multipliedBy(2).coerceAtMost(maxBackoff)
            return
        }

        backoff = Duration.ofSeconds(2)
        log.debug { "📬 Found ${newPersisted.size} new events" }

        val grouped = newPersisted.groupBy { it.referenceId }
        var anyProcessed = false

        for ((ref, eventsForRef) in grouped) {
            val refSeen = refWatermark[ref] ?: LocalDateTime.of(1970, 1, 1, 0, 0)

            // Finn kun nye events for denne ref’en
            val newForRef = eventsForRef.filter { it.persistedAt > refSeen }
            if (newForRef.isEmpty()) {
                log.debug { "🧊 No new events for $ref since $refSeen" }
                continue
            }

            // Hvis ref er busy → ikke oppdater watermark, ikke dispatch
            if (dispatchQueue.isProcessing(ref)) {
                log.debug { "⏳ $ref is busy — deferring ${newForRef.size} events" }
                continue
            }

            // Hent full sekvens for ref (Eventi-invariant)
            val fullLog = eventStore.getPersistedEventsFor(ref)
            val events = fullLog.mapNotNull { it.toEvent() }

            log.debug { "🚀 Dispatching ${events.size} events for $ref" }
            dispatchQueue.dispatch(ref, events, dispatcher)

            // Oppdater watermark for denne ref’en
            val maxPersistedAt = newForRef.maxOf { it.persistedAt }
            val newWatermark = minOf(pollStartedAt, maxPersistedAt).plusNanos(1)

            refWatermark[ref] = newWatermark
            anyProcessed = true

            log.debug { "⏩ Updated watermark for $ref → $newWatermark" }
        }

        // Oppdater global scan hint
        if (anyProcessed) {
            lastSeenTime = refWatermark.values.minOrNull() ?: lastSeenTime
            log.debug { "📉 Global scanFrom updated → $lastSeenTime" }
        } else {
            log.debug { "🔁 No refs processed — global scanFrom unchanged ($lastSeenTime)" }
        }
    }





}
