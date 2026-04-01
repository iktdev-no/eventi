package no.iktdev.eventi.tasks

import kotlinx.coroutines.delay
import kotlinx.coroutines.time.delay
import mu.KotlinLogging
import no.iktdev.eventi.MyTime
import no.iktdev.eventi.lifecycle.ILifecycleStore
import no.iktdev.eventi.lifecycle.LifecycleStore
import no.iktdev.eventi.lifecycle.LifecycleTaskReporter
import no.iktdev.eventi.lifecycle.TaskListenerFatalError
import no.iktdev.eventi.lifecycle.TaskListenerInvoked
import no.iktdev.eventi.lifecycle.TaskListenerResult
import no.iktdev.eventi.lifecycle.TaskPollerBackoff
import no.iktdev.eventi.lifecycle.TaskPollerCycleStart
import no.iktdev.eventi.lifecycle.TaskPollerFetched
import no.iktdev.eventi.lifecycle.TaskState
import no.iktdev.eventi.lifecycle.TaskStateSummary
import no.iktdev.eventi.lifecycle.TaskStatus
import no.iktdev.eventi.serialization.ZDS.toTask
import no.iktdev.eventi.models.Task
import no.iktdev.eventi.registry.TaskListenerRegistry
import no.iktdev.eventi.stores.TaskStore
import java.time.Duration

abstract class TaskPollerImplementation(
    private val taskStore: TaskStore,
    private val lifecycleStore: ILifecycleStore,
    private val reporterFactory: (Task) -> TaskReporter,
) {
    private val log = KotlinLogging.logger {}

    open var backoff = Duration.ofSeconds(2)
        protected set
    private val maxBackoff = Duration.ofMinutes(1)

    // Toggle ANSI colors in logs (set to false if your logging sink doesn't support ANSI)
    protected open val enableColors: Boolean = true

    private companion object {
        const val ANSI_GREEN = "\u001B[32m"
        const val ANSI_YELLOW = "\u001B[33m"
        const val ANSI_RED = "\u001B[31m"
        const val ANSI_RESET = "\u001B[0m"
    }

    open suspend fun start() {
        log.info { "TaskPoller starting with initial backoff=$backoff" }
        while (true) {
            try {
                pollOnce()
            } catch (e: Exception) {
                log.error(e) { "Unhandled error in poll loop, backing off for $backoff" }
                lifecycleStore.add(
                    TaskListenerFatalError(
                        timestamp = MyTime.utcNow(),
                        taskId = "N/A",
                        listener = "TaskPoller",
                        exception = "${e::class.java.simpleName}: ${e.message}",
                        stacktrace = e.stackTraceToString()
                    )
                )
                delay(backoff)
                backoff = backoff.multipliedBy(2).coerceAtMost(maxBackoff)
            }
        }
    }

    suspend fun pollOnce() {
        lifecycleStore.add(
            TaskPollerCycleStart(
                timestamp = MyTime.utcNow()
            )
        )
        log.debug { "Polling for pending tasks…" }
        val newPersistedTasks = taskStore.getPendingTasks()

        lifecycleStore.add(
            TaskPollerFetched(
                timestamp = MyTime.utcNow(),
                count = newPersistedTasks.size
            )
        )

        if (newPersistedTasks.isEmpty()) {
            lifecycleStore.add(
                TaskPollerBackoff(
                    timestamp = MyTime.utcNow(),
                    backoffMillis = backoff.toMillis()
                )
            )
            log.debug { "No pending tasks found. Backing off for $backoff" }
            delay(backoff)
            backoff = backoff.multipliedBy(2).coerceAtMost(maxBackoff)
            return
        }

        val count = newPersistedTasks.size

        log.debug {
            buildString {
                append("Found $count pending ${if (count == 1) "task" else "tasks"}:\n")
                newPersistedTasks.forEach { t ->
                    append("\t${t.taskId} - ${t.task}\n")
                }
            }
        }

        val tasks = newPersistedTasks.mapNotNull { it.toTask() }
        var acceptedAny = false
        val results = mutableListOf<PollResult>()

        for (task in tasks) {

            val candidates = TaskListenerRegistry.getListeners()
                .filter { it.supports(task) && !it.isBusy }

            if (candidates.isEmpty()) {
                lifecycleStore.add(
                    TaskListenerResult(
                        timestamp = MyTime.utcNow(),
                        taskId = task.taskId.toString(),
                        listener = "NO_MATCHING_LISTENER",
                        result = "IGNORED"
                    )
                )
                results += PollResult(
                    taskId = task.taskId.toString(),
                    type = task::class.simpleName.toString(),
                    statusPlain = "[IGNORED]",
                    statusColored = colorize("[IGNORED]", Color.YELLOW),
                    listener = "NO_MATCHING_LISTENER"
                )
                continue
            }

            var accepted = false

            for (listener in candidates) {
                val reporter = LifecycleTaskReporter(
                    delegate = reporterFactory(task),
                    lifecycleStore = lifecycleStore
                )

                lifecycleStore.add(
                    TaskListenerInvoked(
                        timestamp = MyTime.utcNow(),
                        taskId = task.taskId.toString(),
                        listener = listener.getWorkerId()
                    )
                )

                val ok = try {
                    listener.accept(task, reporter)
                } catch (e: Exception) {
                    lifecycleStore.add(
                        TaskListenerFatalError(
                            timestamp = MyTime.utcNow(),
                            taskId = task.taskId.toString(),
                            listener = listener.getWorkerId(),
                            exception = "${e::class.java.simpleName}: ${e.message}",
                            stacktrace = e.stackTraceToString()
                        )
                    )
                    log.error(e) {
                        "Error while processing task ${task.taskId} by listener ${listener.getWorkerId()}"
                    }
                    false
                }

                lifecycleStore.add(
                    TaskListenerResult(
                        timestamp = MyTime.utcNow(),
                        taskId = task.taskId.toString(),
                        listener = listener.getWorkerId(),
                        result = if (ok) "ACCEPTED" else "IGNORED"
                    )
                )

                val (statusPlain, statusColored) = when {
                    ok -> "[ACCEPTED]" to colorize("[ACCEPTED]", Color.GREEN)
                    else -> "[IGNORED]" to colorize("[IGNORED]", Color.YELLOW)
                }

                results += PollResult(
                    taskId = task.taskId.toString(),
                    type = task::class.simpleName.toString(),
                    statusPlain = statusPlain,
                    statusColored = statusColored,
                    listener = listener.getWorkerId()
                )

                if (ok) {
                    accepted = true
                    break
                }
            }

            acceptedAny = acceptedAny || accepted
        }

        // Log a compact, column-aligned report
        log.debug { formatTaskReport(results) }

        if (!acceptedAny) {
            lifecycleStore.add(
                TaskPollerBackoff(
                    timestamp = MyTime.utcNow(),
                    backoffMillis = backoff.toMillis()
                )
            )
            log.debug { "No tasks were accepted. Backing off for $backoff" }
            delay(backoff)
            backoff = backoff.multipliedBy(2).coerceAtMost(maxBackoff)
        } else {
            log.debug { "At least one task accepted. Resetting backoff." }
            backoff = Duration.ofSeconds(2)
        }
    }

    private enum class Color { GREEN, YELLOW, RED }

    private fun colorize(text: String, color: Color): String {
        if (!enableColors) return text
        val code = when (color) {
            Color.GREEN -> ANSI_GREEN
            Color.YELLOW -> ANSI_YELLOW
            Color.RED -> ANSI_RED
        }
        return "$code$text$ANSI_RESET"
    }

    private fun formatTaskReport(results: List<PollResult>): String {
        if (results.isEmpty()) return "\t(no tasks processed)"

        // Compute widths based on plain (non-ANSI) status strings
        val statusWidth = results.maxOf { it.statusPlain.length }
        val idWidth = results.maxOf { it.taskId.length }
        val typeWidth = results.maxOf { it.type.length }

        return buildString {
            append("Task processing report:\n")
            results.forEach { r ->
                // statusColored may contain ANSI codes; pad using plain length
                val statusPadding = " ".repeat((statusWidth - r.statusPlain.length).coerceAtLeast(0))
                append("\t")
                append(r.statusColored)
                append(statusPadding)
                append("  ")
                append(r.taskId.padEnd(idWidth))
                append("  ")
                append(r.type.padEnd(typeWidth))
                append("  ")
                append("(listener=${r.listener})")
                append("\n")
            }
        }
    }

    internal data class PollResult(
        val taskId: String,
        val type: String,
        val statusPlain: String,
        val statusColored: String,
        val listener: String
    )

    suspend fun taskState(): TaskStateSummary {
        val now = MyTime.utcNow()
        val pending = taskStore.getPendingTasks()

        return TaskStateSummary(
            taskBackoffMillis = backoff.toMillis(),
            nextTaskPollExpectedAt = now.plusMillis(backoff.toMillis()),
            pendingTasks = pending.size,
            taskStates = pending.map {
                TaskState(
                    taskId = it.taskId.toString(),
                    type = it.task,
                    listener = null,
                    status = TaskStatus.PENDING
                )
            }
        )
    }

}
