package no.iktdev.eventi.lifecycle

import java.time.Instant
import java.util.UUID

data class EventState(
    val lastSeenTime: Instant,
    val backoffMillis: Long,
    val nextPollExpectedAt: Instant,
    val activeRefs: Set<UUID>,
    val refStates: List<RefState>
)

data class TaskStateSummary(
    val taskBackoffMillis: Long,
    val nextTaskPollExpectedAt: Instant,
    val pendingTasks: Int,
    val taskStates: List<TaskState>
)


data class CurrentState(
    // ----- EVENT POLLER -----
    val lastSeenTime: Instant,
    val backoffMillis: Long,
    val nextPollExpectedAt: Instant,
    val activeRefs: Set<UUID>,
    val refStates: List<RefState>,

    // ----- TASK POLLER -----
    val taskBackoffMillis: Long,
    val nextTaskPollExpectedAt: Instant,
    val pendingTasks: Int,
    val taskStates: List<TaskState>
)

data class RefState(
    val ref: UUID,
    val watermark: Pair<Instant, Long>,
    val isProcessing: Boolean,
    val hasUnprocessedEvents: Boolean,
    val lastEventAt: Instant?
)

data class TaskState(
    val taskId: String,
    val type: String,
    val listener: String?,
    val status: TaskStatus
)

enum class TaskStatus {
    PENDING,
    ACCEPTED,
    IGNORED,
    FAILED
}
