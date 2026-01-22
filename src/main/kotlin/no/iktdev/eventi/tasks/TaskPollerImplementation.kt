package no.iktdev.eventi.tasks

import kotlinx.coroutines.delay
import mu.KotlinLogging
import no.iktdev.eventi.ZDS.toTask
import no.iktdev.eventi.models.Task
import no.iktdev.eventi.stores.TaskStore
import java.time.Duration

abstract class TaskPollerImplementation(
    private val taskStore: TaskStore,
    private val reporterFactory: (Task) -> TaskReporter
) {
    private val log = KotlinLogging.logger {}


    open var backoff = Duration.ofSeconds(2)
        protected set
    private val maxBackoff = Duration.ofMinutes(1)

    open suspend fun start() {
        log.info { "TaskPoller starting with initial backoff=$backoff" }
        while (true) {
            try {
                pollOnce()
            } catch (e: Exception) {
                e.printStackTrace()
                delay(backoff.toMillis())
                backoff = backoff.multipliedBy(2).coerceAtMost(maxBackoff)
            }
        }
    }

    suspend fun pollOnce() {
        log.debug { "Polling for pending tasks…" }
        val newPersistedTasks = taskStore.getPendingTasks()

        if (newPersistedTasks.isEmpty()) {
            log.debug { "No pending tasks found. Backing off for $backoff" }
            delay(backoff.toMillis())
            backoff = backoff.multipliedBy(2).coerceAtMost(maxBackoff)
            return
        }
        log.debug { "Found ${newPersistedTasks.size} persisted tasks" }

        val tasks = newPersistedTasks.mapNotNull { it.toTask() }
        var acceptedAny = false

        for (task in tasks) {
            val listener = TaskListenerRegistry.getListeners().firstOrNull { it.supports(task) && !it.isBusy } ?: continue
            val claimed = taskStore.claim(task.taskId, listener.getWorkerId())
            if (!claimed) {
                log.debug { "Task ${task.taskId} is already claimed by another worker" }
                continue
            }

            log.debug { "Task ${task.taskId} claimed by ${listener.getWorkerId()}" }

            val reporter = reporterFactory(task)
            val accepted = try {
                listener.accept(task, reporter)
            } catch (e: Exception) {
                log.error("Error while processing task ${task.taskId} by listener ${listener.getWorkerId()}: ${e.message}")
                e.printStackTrace()
                false
            }
            acceptedAny = acceptedAny || accepted
        }

        if (!acceptedAny) {
            log.debug { "No tasks were accepted. Backing off for $backoff" }
            delay(backoff.toMillis())
            backoff = backoff.multipliedBy(2).coerceAtMost(maxBackoff)
        } else {
            log.debug { "At least one task accepted. Resetting backoff." }
            backoff = Duration.ofSeconds(2)
        }
    }
}