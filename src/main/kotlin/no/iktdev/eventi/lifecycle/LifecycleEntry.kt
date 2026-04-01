package no.iktdev.eventi.lifecycle

import no.iktdev.eventi.models.Progress
import java.time.Instant
import java.util.UUID

sealed interface LifecycleEntry {
    val timestamp: Instant
}

/* ----------------------------- POLLER ----------------------------- */

data class PollerCycleStart(
    override val timestamp: Instant,
    val scanFrom: Instant
) : LifecycleEntry

data class PollerFetched(
    override val timestamp: Instant,
    val count: Int
) : LifecycleEntry

data class PollerGrouped(
    override val timestamp: Instant,
    val refs: List<UUID>
) : LifecycleEntry

data class PollerUpdatedLastSeen(
    override val timestamp: Instant,
    val before: Instant,
    val after: Instant
) : LifecycleEntry

data class PollerBackoff(
    override val timestamp: Instant,
    val backoffMillis: Long
) : LifecycleEntry

/* ----------------------------- REF ----------------------------- */

data class RefFiltered(
    override val timestamp: Instant,
    val ref: UUID,
    val seenCount: Int,
    val newCount: Int,
    val watermarkBefore: Pair<Instant, Long>
) : LifecycleEntry

data class RefBusy(
    override val timestamp: Instant,
    val ref: UUID,
    val deferredCount: Int
) : LifecycleEntry

data class RefDispatchStarted(
    override val timestamp: Instant,
    val ref: UUID,
    val historyCount: Int,
    val newCount: Int
) : LifecycleEntry

data class RefDispatchCompleted(
    override val timestamp: Instant,
    val ref: UUID
) : LifecycleEntry

data class RefWatermarkUpdated(
    override val timestamp: Instant,
    val ref: UUID,
    val before: Pair<Instant, Long>,
    val after: Pair<Instant, Long>
) : LifecycleEntry

/* ----------------------------- DISPATCH QUEUE ----------------------------- */

data class DispatchQueueAcquired(
    override val timestamp: Instant,
    val ref: UUID
) : LifecycleEntry

data class DispatchQueueReleased(
    override val timestamp: Instant,
    val ref: UUID
) : LifecycleEntry

data class DispatchQueueSkipped(
    override val timestamp: Instant,
    val ref: UUID
) : LifecycleEntry

data class DispatchExceptionEntry(
    override val timestamp: Instant,
    val ref: UUID,
    val exception: String,
    val stacktrace: String
) : LifecycleEntry


/* ----------------------------- DISPATCHER ----------------------------- */

data class ListenerInvoked(
    override val timestamp: Instant,
    val ref: UUID,
    val listener: String,
    val eventName: String
) : LifecycleEntry

data class ListenerResult(
    override val timestamp: Instant,
    val ref: UUID,
    val listener: String,
    val result: String
) : LifecycleEntry

data class ListenerFatalError(
    override val timestamp: Instant,
    val ref: UUID,
    val listener: String,
    val eventName: String,
    val exception: String
) : LifecycleEntry

/* ----------------------------- Tasks ----------------------------- */

data class TaskPollerCycleStart(
    override val timestamp: Instant
) : LifecycleEntry

data class TaskPollerFetched(
    override val timestamp: Instant,
    val count: Int
) : LifecycleEntry

data class TaskPollerBackoff(
    override val timestamp: Instant,
    val backoffMillis: Long
) : LifecycleEntry

data class TaskListenerInvoked(
    override val timestamp: Instant,
    val taskId: String,
    val listener: String
) : LifecycleEntry

data class TaskListenerResult(
    override val timestamp: Instant,
    val taskId: String,
    val listener: String,
    val result: String
) : LifecycleEntry

data class TaskListenerFatalError(
    override val timestamp: Instant,
    val taskId: String,
    val listener: String,
    val exception: String,
    val stacktrace: String
) : LifecycleEntry

data class TaskClaimed(
    override val timestamp: Instant,
    val taskId: UUID,
    val workerId: String
) : LifecycleEntry

data class TaskProgress(
    override val timestamp: Instant,
    val taskId: UUID,
    val progress: Progress
) : LifecycleEntry

data class TaskCompleted(
    override val timestamp: Instant,
    val taskId: UUID
) : LifecycleEntry

data class TaskFailed(
    override val timestamp: Instant,
    val taskId: UUID,
    val referenceId: UUID
) : LifecycleEntry

data class TaskCancelled(
    override val timestamp: Instant,
    val taskId: UUID,
    val referenceId: UUID
) : LifecycleEntry
