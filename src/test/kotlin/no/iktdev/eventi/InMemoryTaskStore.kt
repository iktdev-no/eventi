package no.iktdev.eventi

import no.iktdev.eventi.ZDS.toPersisted
import no.iktdev.eventi.models.Task
import no.iktdev.eventi.models.store.PersistedTask
import no.iktdev.eventi.models.store.TaskStatus
import no.iktdev.eventi.stores.TaskStore
import java.time.Duration
import java.time.LocalDateTime
import java.util.UUID

open class InMemoryTaskStore : TaskStore {
    private val tasks = mutableListOf<PersistedTask>()
    private var nextId = 1L

    override fun persist(task: Task) {
        val persistedTask = task.toPersisted(nextId++)
        tasks += persistedTask
    }

    override fun findByTaskId(taskId: UUID) = tasks.find { it.taskId == taskId }

    override fun findByEventId(eventId: UUID) =
        tasks.filter { it.data.contains(eventId.toString()) }

    override fun findUnclaimed(referenceId: UUID) =
        tasks.filter { it.referenceId == referenceId && !it.claimed && !it.consumed }

    override fun claim(taskId: UUID, workerId: String): Boolean {
        val task = findByTaskId(taskId) ?: return false
        if (task.claimed && !isExpired(task)) return false
        update(task.copy(claimed = true, claimedBy = workerId, lastCheckIn = LocalDateTime.now()))
        return true
    }

    override fun heartbeat(taskId: UUID) {
        val task = findByTaskId(taskId) ?: return
        update(task.copy(lastCheckIn = LocalDateTime.now()))
    }

    override fun markConsumed(taskId: UUID) {
        val task = findByTaskId(taskId) ?: return
        update(task.copy(consumed = true, status = TaskStatus.Completed))
    }

    override fun releaseExpiredTasks(timeout: Duration) {
        val now = LocalDateTime.now()
        tasks.filter {
            it.claimed && !it.consumed && it.lastCheckIn?.isBefore(now.minus(timeout)) == true
        }.forEach {
            update(it.copy(claimed = false, claimedBy = null, lastCheckIn = null))
        }
    }

    override fun getPendingTasks() = tasks.filter { !it.consumed }

    private fun update(updated: PersistedTask) {
        tasks.replaceAll { if (it.taskId == updated.taskId) updated else it }
    }

    private fun isExpired(task: PersistedTask): Boolean {
        val now = LocalDateTime.now()
        return task.lastCheckIn?.isBefore(now.minusMinutes(15)) == true
    }

    private fun serialize(data: Any?): String = data?.toString() ?: "{}"
}
