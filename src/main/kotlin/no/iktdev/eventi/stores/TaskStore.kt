package no.iktdev.eventi.stores

import no.iktdev.eventi.models.StoreResult
import no.iktdev.eventi.models.Task
import no.iktdev.eventi.models.store.PersistedTask
import no.iktdev.eventi.models.store.TaskStatus
import java.time.Duration
import java.util.UUID

interface TaskStore {
    fun persist(task: Task): Boolean

    fun findByTaskId(taskId: UUID): PersistedTask?
    fun findByReferenceId(referenceId: UUID): List<PersistedTask>
    fun findUnclaimed(referenceId: UUID): List<PersistedTask>

    fun claim(taskId: UUID, workerId: String): Boolean
    fun heartbeat(taskId: UUID): Boolean
    fun markConsumed(taskId: UUID, status: TaskStatus): Boolean
    fun releaseExpiredTasks()
    fun resetTaskById(taskId: UUID): Boolean

    /**
     * Required to perform a rollback if one or more fails
     */
    fun resetTasksById(taskId: List<UUID>): StoreResult

    /**
     * Required to perform a rollback if one or more fails
     */
    fun deleteTasksById(taskId: UUID): StoreResult

    fun getPendingTasks(): List<PersistedTask>
}