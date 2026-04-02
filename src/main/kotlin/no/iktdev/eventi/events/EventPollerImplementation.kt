package no.iktdev.eventi.events

import kotlinx.coroutines.delay
import kotlinx.coroutines.time.delay
import mu.KotlinLogging
import no.iktdev.eventi.MyTime
import no.iktdev.eventi.PollerTraceStore
import no.iktdev.eventi.lifecycle.CurrentState
import no.iktdev.eventi.lifecycle.EventState
import no.iktdev.eventi.lifecycle.ILifecycleStore
import no.iktdev.eventi.lifecycle.LifecycleStore
import no.iktdev.eventi.lifecycle.PollerBackoff
import no.iktdev.eventi.lifecycle.PollerCycleStart
import no.iktdev.eventi.lifecycle.PollerFetched
import no.iktdev.eventi.lifecycle.PollerGrouped
import no.iktdev.eventi.lifecycle.PollerUpdatedLastSeen
import no.iktdev.eventi.lifecycle.RefBusy
import no.iktdev.eventi.lifecycle.RefDispatchCompleted
import no.iktdev.eventi.lifecycle.RefDispatchSkipped
import no.iktdev.eventi.lifecycle.RefDispatchStarted
import no.iktdev.eventi.lifecycle.RefFiltered
import no.iktdev.eventi.lifecycle.RefState
import no.iktdev.eventi.lifecycle.RefWatermarkUpdated
import no.iktdev.eventi.models.SignalEvent
import no.iktdev.eventi.models.store.PersistedEvent
import no.iktdev.eventi.serialization.ZDS.toEvent
import no.iktdev.eventi.stores.EventStore
import java.time.Duration
import java.time.Instant
import java.util.UUID
import kotlin.text.compareTo
import kotlin.time.Duration.Companion.milliseconds

abstract class EventPollerImplementation(
    private val eventStore: EventStore,
    private val dispatchQueue: SequenceDispatchQueue,
    private val dispatcher: EventDispatcher,
    private val lifecycleStore: ILifecycleStore
) {
    private val log = KotlinLogging.logger {}

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
                delay(backoff)
                backoff = backoff.multipliedBy(2).coerceAtMost(maxBackoff)
            }
        }
    }

    suspend fun pollOnce() {
        val pollStartedAt = MyTime.utcNow()

        // Lifecycle: start av poll-syklus
        lifecycleStore.add(
            PollerCycleStart(
                timestamp = pollStartedAt,
                scanFrom = lastSeenTime
            )
        )
        log.debug { "🔍 Polling for new events at $pollStartedAt (scanFrom=$lastSeenTime)" }

        val newPersisted = fetchNewPersistedEvents(lastSeenTime)

        // Lifecycle: hvor mange events ble hentet
        lifecycleStore.add(
            PollerFetched(
                timestamp = MyTime.utcNow(),
                count = newPersisted.size
            )
        )

        if (newPersisted.isEmpty()) {
            handleEmptyPoll()
            return
        }

        resetBackoff()
        log.debug { "📬 Found ${newPersisted.size} new events after $lastSeenTime" }

        val grouped = newPersisted.groupBy { it.referenceId }

        // Lifecycle: hvilke refs som var med i denne poll-runden
        lifecycleStore.add(
            PollerGrouped(
                timestamp = MyTime.utcNow(),
                refs = grouped.keys.toList()
            )
        )

        var anyProcessed = false
        for ((ref, eventsForRef) in grouped) {
            if (processReference(ref, eventsForRef)) {
                anyProcessed = true
            }
        }

        // Move global scan hint forward to avoid livelock
        val maxSeenThisRound = newPersisted.maxOfOrNull { it.persistedAt }
        if (maxSeenThisRound != null && maxSeenThisRound > lastSeenTime) {
            val before = lastSeenTime
            lastSeenTime = maxSeenThisRound

            // Lifecycle: global lastSeenTime flyttet
            lifecycleStore.add(
                PollerUpdatedLastSeen(
                    timestamp = MyTime.utcNow(),
                    before = before,
                    after = lastSeenTime
                )
            )
        }

        updateGlobalWatermark(anyProcessed)
    }

    private suspend fun fetchNewPersistedEvents(scanFrom: Instant) =
        eventStore.getPersistedEventsAfter(scanFrom)

    private suspend fun handleEmptyPoll() {
        // Lifecycle: backoff når ingen events ble funnet
        lifecycleStore.add(
            PollerBackoff(
                timestamp = MyTime.utcNow(),
                backoffMillis = backoff.toMillis()
            )
        )
        log.debug { "😴 No new events found. Backing off for $backoff" }

        delay(backoff)
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
        // (Neste steg: vi legger inn Lifecycle her også)
        val (refSeenAt, refSeenId) = refWatermark[ref] ?: (Instant.EPOCH to 0L)

        // Filter new events using (timestamp, id) ordering
        val newForRef = eventsForRef.filter { ev ->
            ev.persistedAt > refSeenAt ||
                    (ev.persistedAt == refSeenAt && ev.id >= refSeenId)
        }

        lifecycleStore.add(
            RefFiltered(
                timestamp = MyTime.utcNow(),
                ref = ref,
                seenCount = eventsForRef.size,
                newCount = newForRef.size,
                watermarkBefore = refSeenAt to refSeenId
            )
        )

        if (newForRef.isEmpty()) {
            log.debug { "🧊 No new events for $ref since ($refSeenAt, id=$refSeenId)" }
            lifecycleStore.add(
                RefDispatchSkipped(
                    timestamp = MyTime.utcNow(),
                    ref = ref,
                    reason = "No new events since watermark"
                )
            )
            return false
        }

        // If ref is busy, advance watermark but skip dispatch
        if (dispatchQueue.isProcessing(ref)) {
            log.debug {
                log.debug { "⏳ $ref is busy — deferring ${newForRef.size} events" }
            }
            lifecycleStore.add(
                RefBusy(
                    timestamp = MyTime.utcNow(),
                    ref = ref,
                    deferredCount = newForRef.size
                )
            )
            return false
        }

        // Fetch full sequence for dispatch
        val fullLog = eventStore.getPersistedEventsFor(ref)
        if (fullLog.isEmpty()) {
            log.warn { "⚠️ No events found for $ref when fetching full log (should not happen)" }
            lifecycleStore.add(
                RefDispatchSkipped(
                    timestamp = Instant.now(),
                    ref = ref,
                    reason = "Full log empty for ref"
                )
            )
            return false
        }

        val history = fullLog.mapNotNull { it.toEvent() }
        val newEvents = newForRef.mapNotNull { it.toEvent() }

        lifecycleStore.add(
            RefDispatchStarted(
                timestamp = MyTime.utcNow(),
                ref = ref,
                historyCount = history.size,
                newCount = newEvents.size
            )
        )
        log.debug { "🚀 Dispatching ${history.size} events for $ref (new=${newEvents.size})" }

        dispatchQueue.dispatch(ref, history, newEvents, dispatcher)

        lifecycleStore.add(
            RefDispatchCompleted(
                timestamp = MyTime.utcNow(),
                ref = ref
            )
        )

        // Update watermark for this reference
        val maxDomainEvent = fullLog
            .filter { it.toEvent() !is SignalEvent }
            .maxWithOrNull(compareBy({ it.persistedAt }, { it.id }))

        if (maxDomainEvent == null) {
            // Ingen domenedata å flytte watermark til → men vi har prosessert signaler
            return true
        }



        val before = refWatermark[ref] ?: (Instant.EPOCH to 0L)
        val after = maxDomainEvent.persistedAt to maxDomainEvent.id

        updateWatermark(ref, after)

        lifecycleStore.add(
            RefWatermarkUpdated(
                timestamp = MyTime.utcNow(),
                ref = ref,
                before = before,
                after = after
            )
        )

        log.debug { "⏩ Updated watermark for $ref → (${maxDomainEvent.persistedAt}, id=${maxDomainEvent.id})" }
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

    fun eventState(): EventState {
        val now = MyTime.utcNow()

        val refStates = refWatermark.map { (ref, wm) ->
            val fullLog = eventStore.getPersistedEventsFor(ref)
            val lastEvent = fullLog.maxByOrNull { it.persistedAt }

            val hasUnprocessed = lastEvent != null &&
                    (lastEvent.persistedAt > wm.first || lastEvent.id > wm.second)

            RefState(
                ref = ref,
                watermark = wm,
                isProcessing = dispatchQueue.isProcessing(ref),
                hasUnprocessedEvents = hasUnprocessed,
                lastEventAt = lastEvent?.persistedAt
            )
        }

        return EventState(
            lastSeenTime = lastSeenTime,
            backoffMillis = backoff.toMillis(),
            nextPollExpectedAt = now.plusMillis(backoff.toMillis()),
            activeRefs = dispatchQueue.activeRefs(),
            refStates = refStates
        )
    }

}
