package no.iktdev.eventi.stores

import no.iktdev.eventi.models.Task
import no.iktdev.eventi.models.store.PersistedTask
import no.iktdev.eventi.models.store.TaskStatus
import java.time.Duration
import java.util.UUID

interface TaskStore {
    fun persist(task: Task)

    fun findByTaskId(taskId: UUID): PersistedTask?
    fun findByReferenceId(referenceId: UUID): List<PersistedTask>
    fun findUnclaimed(referenceId: UUID): List<PersistedTask>

    fun claim(taskId: UUID, workerId: String): Boolean
    fun heartbeat(taskId: UUID)
    fun markConsumed(taskId: UUID, status: TaskStatus)
    fun releaseExpiredTasks(timeout: Duration = Duration.ofMinutes(15))

    fun getPendingTasks(): List<PersistedTask>
}