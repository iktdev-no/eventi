package no.iktdev.eventi.lifecycle

import no.iktdev.eventi.MyTime
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.Progress
import no.iktdev.eventi.tasks.Result
import no.iktdev.eventi.tasks.TaskReporter
import java.util.UUID

class LifecycleTaskReporter(
    private val delegate: TaskReporter,
    private val lifecycleStore: ILifecycleStore
) : TaskReporter {
    override fun markClaimed(taskId: UUID, workerId: String): Result {
        lifecycleStore.add(
            TaskClaimed(
                timestamp = MyTime.utcNow(),
                taskId = taskId,
                workerId = workerId
            )
        )
        return delegate.markClaimed(taskId, workerId)
    }

    override fun updateLastSeen(taskId: UUID): Result {
        return delegate.updateLastSeen(taskId)
    }

    override fun markCompleted(taskId: UUID): Result {
        lifecycleStore.add(
            TaskCompleted(
                timestamp = MyTime.utcNow(),
                taskId = taskId
            )
        )
        return delegate.markCompleted(taskId)
    }

    override fun markFailed(referenceId: UUID, taskId: UUID): Result {
        lifecycleStore.add(
            TaskFailed(
                timestamp = MyTime.utcNow(),
                taskId = taskId,
                referenceId = referenceId
            )
        )
        return delegate.markFailed(referenceId, taskId)
    }

    override fun markCancelled(referenceId: UUID, taskId: UUID): Result {
        lifecycleStore.add(
            TaskCancelled(
                timestamp = MyTime.utcNow(),
                taskId = taskId,
                referenceId = referenceId
            )
        )
        return delegate.markCancelled(referenceId, taskId)
    }

    override fun updateProgress(
        referenceId: UUID,
        taskId: UUID,
        payload: Progress
    ): Result {
        lifecycleStore.add(
            TaskProgress(
                timestamp = MyTime.utcNow(),
                taskId = taskId,
                progress = payload
            )
        )
        return delegate.updateProgress(referenceId, taskId, payload)
    }

    override fun log(taskId: UUID, message: String) {
        delegate.log(taskId, message)
    }

    override fun publishEvent(event: Event): Result {
        return delegate.publishEvent(event)
    }


}
