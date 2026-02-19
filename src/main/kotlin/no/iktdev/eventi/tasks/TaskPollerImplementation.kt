package no.iktdev.eventi.tasks

import kotlinx.coroutines.delay
import mu.KotlinLogging
import no.iktdev.eventi.serialization.ZDS.toTask
import no.iktdev.eventi.models.Task
import no.iktdev.eventi.registry.TaskListenerRegistry
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
                val reporter = reporterFactory(task)

                val ok = try {
                    listener.accept(task, reporter)
                } catch (e: Exception) {
                    log.error(e) {
                        "Error while processing task ${task.taskId} by listener ${listener.getWorkerId()}"
                    }
                    false
                }

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
            log.debug { "No tasks were accepted. Backing off for $backoff" }
            delay(backoff.toMillis())
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
}
