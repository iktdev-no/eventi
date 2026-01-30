package no.iktdev.eventi.tasks

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import no.iktdev.eventi.InMemoryTaskStore
import no.iktdev.eventi.TestBase
import no.iktdev.eventi.events.EventTypeRegistry
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.Task
import no.iktdev.eventi.stores.TaskStore
import no.iktdev.eventi.testUtil.multiply
import no.iktdev.eventi.testUtil.wipe
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.UUID
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class TaskPollerImplementationTest : TestBase() {

    @BeforeEach
    fun setup() {
        TaskListenerRegistry.wipe()
        TaskTypeRegistry.wipe()
        eventDeferred = CompletableDeferred()
    }

    private lateinit var eventDeferred: CompletableDeferred<Event>
    val reporterFactory = { _: Task ->
        object : TaskReporter {
            override fun markClaimed(taskId: UUID, workerId: String) {}
            override fun updateLastSeen(taskId: UUID) {}
            override fun markCompleted(taskId: UUID) {}
            override fun markFailed(taskId: UUID) {}
            override fun markCancelled(taskId: UUID) {}
            override fun updateProgress(taskId: UUID, progress: Int) {}
            override fun log(taskId: UUID, message: String) {}
            override fun publishEvent(event: Event) {
                eventDeferred.complete(event)
            }
        }
    }

    data class EchoTask(var data: String?) : Task() {
    }

    data class EchoEvent(var data: String) : Event() {
    }

    class TaskPollerImplementationTest(taskStore: TaskStore, reporterFactory: (Task) -> TaskReporter): TaskPollerImplementation(taskStore, reporterFactory) {
        fun overrideSetBackoff(duration: java.time.Duration) {
            backoff = duration
        }
    }


    open class EchoListener : TaskListener(TaskType.MIXED) {
        var result: Event? = null

        fun getJob() = currentJob

        override fun getWorkerId() = this.javaClass.simpleName

        override fun supports(task: Task): Boolean {
            return task is EchoTask
        }

        override suspend fun onTask(task: Task): Event {
            withHeartbeatRunner(1.seconds) {
                println("Heartbeat")
            }
            if (task is EchoTask) {
                return EchoEvent(task.data + " Potetmos").producedFrom(task)
            }
            throw IllegalArgumentException("Unsupported task type: ${task::class.java}")
        }

        override fun onComplete(task: Task, result: Event?) {
            super.onComplete(task, result)
            this.result = result;
            reporter?.publishEvent(result!!)
        }

        override fun onError(task: Task, exception: Exception) {
            exception.printStackTrace()
            super.onError(task, exception)
        }

        override fun onCancelled(task: Task) {
            super.onCancelled(task)
        }

    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun scenario1() = runTest {
        // Register Task and Event
        TaskTypeRegistry.register(EchoTask::class.java)
        EventTypeRegistry.register(EchoEvent::class.java)

        val listener = EchoListener()

        val poller = object : TaskPollerImplementation(taskStore, reporterFactory) {}

        val task = EchoTask("Hello").newReferenceId().derivedOf(object : Event() {})
        taskStore.persist(task)
        poller.pollOnce()
        advanceUntilIdle()
        val producedEvent = eventDeferred.await()
        assertThat(producedEvent).isNotNull
        assertThat(producedEvent.metadata.derivedFromId).hasSize(2)
        assertThat(producedEvent.metadata.derivedFromId).contains(task.metadata.derivedFromId!!.first())
        assertThat(producedEvent.metadata.derivedFromId).contains(task.taskId)
        assertThat((listener.result as EchoEvent).data).isEqualTo("Hello Potetmos")
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `poller resets backoff when task is accepted`() = runTest {
        TaskTypeRegistry.register(EchoTask::class.java)
        EventTypeRegistry.register(EchoEvent::class.java)

        val listener = EchoListener()
        val poller = TaskPollerImplementationTest(taskStore, reporterFactory)
        val initialBackoff = poller.backoff

        poller.overrideSetBackoff(Duration.ofSeconds(16))
        val task = EchoTask("Hello").newReferenceId()
        taskStore.persist(task)

        poller.pollOnce()

        listener.getJob()?.join()
        advanceTimeBy(1.minutes)
        advanceUntilIdle()

        assertEquals(initialBackoff, poller.backoff)
        assertEquals("Hello Potetmos", (listener.result as EchoEvent).data)
    }

    @Test
    fun `poller increases backoff when no tasks`() = runTest {
        val poller = object : TaskPollerImplementation(taskStore, reporterFactory) {}
        val initialBackoff = poller.backoff
        val totalBackoff = initialBackoff.multiply(2)

        poller.pollOnce()

        assertEquals(totalBackoff, poller.backoff)
    }


    @Test
    fun `poller increases backoff when no listener supports task`() = runTest {
        val poller = object : TaskPollerImplementation(taskStore, reporterFactory) {}
        val initialBackoff = poller.backoff

        // as long as the task is not added to registry this will be unsupported
        val unsupportedTask = EchoTask("Hello").newReferenceId()
        taskStore.persist(unsupportedTask)

        poller.pollOnce()

        assertEquals(initialBackoff.multiply(2), poller.backoff)
    }

    @Test
    fun `poller increases backoff when listener is busy`() = runTest {
        val busyListener = object : EchoListener() {
            override val isBusy = true
        }

        val poller = object : TaskPollerImplementation(taskStore, reporterFactory) {}
        val intialBackoff = poller.backoff

        val task = EchoTask("Busy").newReferenceId()
        taskStore.persist(task)

        poller.pollOnce()

        assertEquals(intialBackoff.multiply(2), poller.backoff)
    }

    @Test
    fun `poller increases backoff when task is not claimed`() = runTest {
        val listener = EchoListener()
        TaskTypeRegistry.register(EchoTask::class.java)
        val task = EchoTask("Unclaimable").newReferenceId()
        taskStore.persist(task)

        // Simuler at claim alltid feiler
        val failingStore = object : InMemoryTaskStore() {
            override fun claim(taskId: UUID, workerId: String): Boolean = false
        }
        val pollerWithFailingClaim = object : TaskPollerImplementation(failingStore, reporterFactory) {}
        val initialBackoff = pollerWithFailingClaim.backoff

        failingStore.persist(task)

        pollerWithFailingClaim.pollOnce()

        assertEquals(initialBackoff.multiply(2), pollerWithFailingClaim.backoff)
    }







}
