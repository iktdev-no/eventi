package no.iktdev.eventi.models.store

import java.time.Instant
import java.util.UUID

data class PersistedTask(
    val id: Long,
    val referenceId: UUID,
    val status: TaskStatus,
    val taskId: UUID,
    val task: String,
    val data: String,
    val claimed: Boolean,
    val claimedBy: String? = null,
    val consumed: Boolean,
    val lastCheckIn: Instant? = null,
    val persistedAt: Instant
) {}

enum class TaskStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Cancelled
}