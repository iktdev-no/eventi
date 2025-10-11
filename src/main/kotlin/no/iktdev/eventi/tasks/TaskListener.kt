package no.iktdev.eventi.tasks

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.Task
import java.util.UUID
import kotlin.coroutines.cancellation.CancellationException

/**
 * Abstract base class for handling tasks with asynchronous processing and reporting.
 *
 * @param T The type of result produced by processing the task.
 * @param reporter An instance of [TaskReporter] for reporting task status and events.
 */
abstract class TaskListener<T>(val taskType: TaskType): TaskListenerImplementation<T> {

    init {
        TaskListenerRegistry.registerListener(this)
    }

    var reporter: TaskReporter? = null
        private set
    abstract fun getWorkerId(): String
    protected var currentJob: Job? = null
    var currentTask: Task? = null
        private set

    open val isBusy: Boolean get() = currentJob?.isActive == true
    val currentTaskId: UUID? get() = currentTask?.taskId

    private fun getDispatcherForTask(task: Task): CoroutineScope {
        return when (taskType) {
            TaskType.CPU_INTENSIVE,
            TaskType.MIXED -> CoroutineScope(Dispatchers.Default)
            TaskType.IO_INTENSIVE -> CoroutineScope(Dispatchers.IO)
        }
    }

    override fun accept(task: Task, reporter: TaskReporter): Boolean {
        if (isBusy || !supports(task)) return false
        this.reporter = reporter
        currentTask = task
        reporter.markClaimed(task.taskId, getWorkerId())

        currentJob = getDispatcherForTask(task).launch {
            try {
                val result = onTask(task)
                reporter.markConsumed(task.taskId)
                onComplete(task, result)
            } catch (e: CancellationException) {
                onCancelled()
            } catch (e: Exception) {
                onError(task, e)
            } finally {
                currentJob = null
                currentTask = null
                this@TaskListener.reporter = null
            }
        }
        return true
    }

    override fun onError(task: Task, exception: Exception) {
        reporter?.log(task.taskId, "Error processing task: ${exception.message}")
        exception.printStackTrace()
    }

    override fun onComplete(task: Task, result: T?) {
        reporter?.markConsumed(task.taskId)
        reporter?.log(task.taskId, "Task completed successfully.")
    }

    override fun onCancelled() {
        currentJob?.cancel()
        currentJob = null
        currentTask = null
    }
}

enum class TaskType {
    CPU_INTENSIVE,
    IO_INTENSIVE,
    MIXED
}


interface TaskListenerImplementation<T> {
    fun supports(task: Task): Boolean
    fun accept(task: Task, reporter: TaskReporter): Boolean
    fun onTask(task: Task): T
    fun onComplete(task: Task, result: T?)
    fun onError(task: Task, exception: Exception)
    fun onCancelled()
}

interface TaskReporter {
    fun markClaimed(taskId: UUID, workerId: String)
    fun updateLastSeen(taskId: UUID)
    fun markConsumed(taskId: UUID)
    fun updateProgress(taskId: UUID, progress: Int)
    fun log(taskId: UUID, message: String)
    fun publishEvent(event: Event)
}
