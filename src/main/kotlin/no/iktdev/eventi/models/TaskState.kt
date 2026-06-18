package no.iktdev.eventi.models

import no.iktdev.eventi.models.store.TaskStatus
import java.time.Instant

data class TaskState(
    val status: TaskStatus = TaskStatus.Pending,
    val claimed: Boolean = false,
    val claimedBy: String? = null,
    val consumed: Boolean = false,
    val lastCheckIn: Instant? = null,
) {
}