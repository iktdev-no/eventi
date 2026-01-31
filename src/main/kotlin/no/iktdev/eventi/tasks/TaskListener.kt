package no.iktdev.eventi.tasks

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.Task
import no.iktdev.eventi.models.store.TaskStatus
import org.jetbrains.annotations.VisibleForTesting
import java.util.UUID
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

/**
 * Abstract base class for handling tasks with asynchronous processing and reporting.
 *

 * @param reporter An instance of [TaskReporter] for reporting task status and events.
 */
abstract class TaskListener(val taskType: TaskType = TaskType.CPU_INTENSIVE): TaskListenerImplementation {

    init {
        TaskListenerRegistry.registerListener(this)
    }

    var reporter: TaskReporter? = null
        private set
    abstract fun getWorkerId(): String
    var currentJob: Job? = null
        protected set
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

    private val heartbeatScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    @VisibleForTesting
    internal var heartbeatRunner: Job? = null
    fun withHeartbeatRunner(interval: Duration = 5.minutes, block: () -> Unit): Job {
        return heartbeatScope.launch {
            while (isActive) {
                block()
                delay(interval)
            }
        }.also { heartbeatRunner = it }
    }

    override fun accept(task: Task, reporter: TaskReporter): Boolean {
        if (isBusy || !supports(task)) return false
        this.reporter = reporter
        currentTask = task
        reporter.markClaimed(task.taskId, getWorkerId())

        currentJob = getDispatcherForTask(task).launch {
            try {
                val result = onTask(task)
                reporter.markCompleted(task.taskId)
                onComplete(task, result)

            } catch (e: CancellationException) {
                // Dette er en ekte kansellering
                onCancelled(task)
                throw e // viktig: ikke svelg cancellation

            } catch (e: Exception) {
                // Dette er en faktisk feil
                onError(task, e)

            } finally {
                heartbeatRunner?.cancel()
                currentJob?.cancel()
                heartbeatRunner = null
                currentJob = null
                currentTask = null
                this@TaskListener.reporter = null
            }
        }
        return true
    }

    abstract fun createIncompleteStateTaskEvent(task: Task, status: TaskStatus, exception: Exception? = null): Event

    override fun onError(task: Task, exception: Exception) {
        reporter?.log(task.taskId, "Error processing task: ${exception.message}")
        exception.printStackTrace()
        reporter?.markFailed(task.referenceId, task.taskId)
        reporter!!.publishEvent(createIncompleteStateTaskEvent(task, TaskStatus.Failed, exception))
    }

    override fun onComplete(task: Task, result: Event?) {
        reporter!!.markCompleted(task.taskId)
        reporter!!.log(task.taskId, "Task completed successfully.")
        result?.let {
            reporter!!.publishEvent(result)
        }
    }

    override fun onCancelled(task: Task) {
        reporter!!.markCancelled(task.referenceId, task.taskId)
        currentJob?.cancel()
        heartbeatRunner?.cancel()
        currentTask = null
        reporter!!.publishEvent(createIncompleteStateTaskEvent(task, TaskStatus.Cancelled))
    }
}

enum class TaskType {
    CPU_INTENSIVE,
    IO_INTENSIVE,
    MIXED
}


interface TaskListenerImplementation {
    fun supports(task: Task): Boolean
    fun accept(task: Task, reporter: TaskReporter): Boolean
    suspend fun onTask(task: Task): Event?
    fun onComplete(task: Task, result: Event?)
    fun onError(task: Task, exception: Exception)
    fun onCancelled(task: Task)
}

interface TaskReporter {
    fun markClaimed(taskId: UUID, workerId: String)
    fun updateLastSeen(taskId: UUID)
    fun markCompleted(taskId: UUID)
    fun markFailed(referenceId: UUID, taskId: UUID)
    fun markCancelled(referenceId: UUID, taskId: UUID)
    fun updateProgress(taskId: UUID, progress: Int)
    fun log(taskId: UUID, message: String)
    fun publishEvent(event: Event)
}
