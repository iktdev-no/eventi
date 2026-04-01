package no.iktdev.eventi.lifecycle

import java.time.Instant
import java.util.UUID

data class TimelineEntry(
    val timestamp: Instant,
    val ref: UUID?,
    val category: TimelineCategory,
    val description: String,
    val raw: LifecycleEntry
)

enum class TimelineCategory {
    POLLER,
    REF,
    DISPATCH_QUEUE,
    DISPATCHER,
    TASK,
    ERROR
}

fun LifecycleStore.timeline(): List<TimelineEntry> =
    getAll().map { it.toTimeline() }.sortedBy { it.timestamp }

fun LifecycleStore.timelineForRef(ref: UUID): List<TimelineEntry> =
    getForRef(ref).map { it.toTimeline() }.sortedBy { it.timestamp }



fun LifecycleEntry.toTimeline(): TimelineEntry {
    return when (this) {

        /* -------------------- EVENT POLLER -------------------- */

        is PollerCycleStart -> TimelineEntry(
            timestamp, null, TimelineCategory.POLLER,
            "Poller cycle start (scanFrom=$scanFrom)", this
        )

        is PollerFetched -> TimelineEntry(
            timestamp, null, TimelineCategory.POLLER,
            "Fetched $count new events", this
        )

        is PollerGrouped -> TimelineEntry(
            timestamp, null, TimelineCategory.POLLER,
            "Grouped refs: ${refs.size}", this
        )

        is PollerUpdatedLastSeen -> TimelineEntry(
            timestamp, null, TimelineCategory.POLLER,
            "Updated lastSeenTime → $after", this
        )

        is PollerBackoff -> TimelineEntry(
            timestamp, null, TimelineCategory.POLLER,
            "Backoff for ${backoffMillis}ms", this
        )


        /* -------------------- EVENT REF -------------------- */

        is RefFiltered -> TimelineEntry(
            timestamp, ref, TimelineCategory.REF,
            "Ref $ref filtered: ${newCount}/${seenCount} new events", this
        )

        is RefBusy -> TimelineEntry(
            timestamp, ref, TimelineCategory.REF,
            "Ref $ref busy, deferred ${deferredCount} events", this
        )

        is RefDispatchStarted -> TimelineEntry(
            timestamp, ref, TimelineCategory.REF,
            "Dispatch started (history=${historyCount}, new=${newCount})", this
        )

        is RefDispatchCompleted -> TimelineEntry(
            timestamp, ref, TimelineCategory.REF,
            "Dispatch completed", this
        )

        is RefWatermarkUpdated -> TimelineEntry(
            timestamp, ref, TimelineCategory.REF,
            "Watermark updated → ${after.first} / ${after.second}", this
        )


        /* -------------------- DISPATCH QUEUE -------------------- */

        is DispatchQueueAcquired -> TimelineEntry(
            timestamp, ref, TimelineCategory.DISPATCH_QUEUE,
            "Semaphore acquired", this
        )

        is DispatchQueueReleased -> TimelineEntry(
            timestamp, ref, TimelineCategory.DISPATCH_QUEUE,
            "Semaphore released", this
        )

        is DispatchQueueSkipped -> TimelineEntry(
            timestamp, ref, TimelineCategory.DISPATCH_QUEUE,
            "Dispatch skipped (already active)", this
        )


        /* -------------------- EVENT DISPATCHER -------------------- */

        is ListenerInvoked -> TimelineEntry(
            timestamp, ref, TimelineCategory.DISPATCHER,
            "Listener ${listener} invoked for $eventName", this
        )

        is ListenerResult -> TimelineEntry(
            timestamp, ref, TimelineCategory.DISPATCHER,
            "Listener ${listener} result: $result", this
        )

        is ListenerFatalError -> TimelineEntry(
            timestamp, ref, TimelineCategory.ERROR,
            "FATAL listener error in ${listener}: $exception", this
        )

        is DispatchExceptionEntry -> TimelineEntry(
            timestamp, ref, TimelineCategory.ERROR,
            "FATAL dispatch error: $exception", this
        )


        /* -------------------- TASK POLLER -------------------- */

        is TaskPollerCycleStart -> TimelineEntry(
            timestamp, null, TimelineCategory.TASK,
            "Task poller cycle start", this
        )

        is TaskPollerFetched -> TimelineEntry(
            timestamp, null, TimelineCategory.TASK,
            "Fetched ${count} pending tasks", this
        )

        is TaskPollerBackoff -> TimelineEntry(
            timestamp, null, TimelineCategory.TASK,
            "Task poller backoff for ${backoffMillis}ms", this
        )


        /* -------------------- TASK DISPATCH -------------------- */

        is TaskListenerInvoked -> TimelineEntry(
            timestamp, null, TimelineCategory.TASK,
            "Task ${taskId}: listener ${listener} invoked", this
        )

        is TaskListenerResult -> TimelineEntry(
            timestamp, null, TimelineCategory.TASK,
            "Task ${taskId}: listener ${listener} → $result", this
        )

        is TaskListenerFatalError -> TimelineEntry(
            timestamp, null, TimelineCategory.ERROR,
            "FATAL task listener error for task ${taskId}: $exception", this
        )
        /* -------------------- TASK LIFECYCLE (REPORTER) -------------------- */

        is TaskClaimed -> TimelineEntry(
            timestamp, null, TimelineCategory.TASK,
            "Task ${taskId} claimed by ${workerId}", this
        )

        is TaskProgress -> TimelineEntry(
            timestamp, null, TimelineCategory.TASK,
            "Task ${taskId} progress: ${progress}", this
        )

        is TaskCompleted -> TimelineEntry(
            timestamp, null, TimelineCategory.TASK,
            "Task ${taskId} completed", this
        )

        is TaskFailed -> TimelineEntry(
            timestamp, null, TimelineCategory.ERROR,
            "Task ${taskId} failed (ref=$referenceId)", this
        )

        is TaskCancelled -> TimelineEntry(
            timestamp, null, TimelineCategory.TASK,
            "Task ${taskId} cancelled (ref=$referenceId)", this
        )

    }
}
