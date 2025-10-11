package no.iktdev.eventi.tasks

import kotlinx.coroutines.delay
import no.iktdev.eventi.ZDS.toTask
import no.iktdev.eventi.models.Task
import no.iktdev.eventi.stores.TaskStore
import java.time.Duration

abstract class AbstractTaskPoller(
    private val taskStore: TaskStore,
    private val reporterFactory: (Task) -> TaskReporter
) {
    var backoff = Duration.ofSeconds(2)
        protected set
    private val maxBackoff = Duration.ofMinutes(1)

    suspend fun start() {
        while (true) {
            pollOnce()
        }
    }

    suspend fun pollOnce() {
        val newPersistedTasks = taskStore.getPendingTasks()

        if (newPersistedTasks.isEmpty()) {
            delay(backoff.toMillis())
            backoff = backoff.multipliedBy(2).coerceAtMost(maxBackoff)
            return
        }
        val tasks = newPersistedTasks.mapNotNull { it.toTask() }
        var acceptedAny = false

        for (task in tasks) {
            val listener = TaskListenerRegistry.getListeners().firstOrNull { it.supports(task) && !it.isBusy } ?: continue
            val claimed = taskStore.claim(task.taskId, listener.getWorkerId())
            if (!claimed) continue

            val reporter = reporterFactory(task)
            val accepted = listener.accept(task, reporter)
            acceptedAny = acceptedAny || accepted
        }

        if (!acceptedAny) {
            delay(backoff.toMillis())
            backoff = backoff.multipliedBy(2).coerceAtMost(maxBackoff)
        } else {
            backoff = Duration.ofSeconds(2)
        }
    }
}