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
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.UUID
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

@DisplayName("""
TaskPollerImplementation
Når polleren henter og prosesserer tasks
Hvis lyttere, backoff og event-produksjon fungerer som forventet
Så skal polleren håndtere alle scenarier korrekt
""")
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

    data class EchoTask(var data: String?) : Task()
    data class EchoEvent(var data: String) : Event()

    class TaskPollerImplementationTest(
        taskStore: TaskStore,
        reporterFactory: (Task) -> TaskReporter
    ) : TaskPollerImplementation(taskStore, reporterFactory) {
        fun overrideSetBackoff(duration: java.time.Duration) {
            backoff = duration
        }
    }

    open class EchoListener : TaskListener(TaskType.MIXED) {
        var result: Event? = null

        fun getJob() = currentJob
        override fun getWorkerId() = this.javaClass.simpleName
        override fun supports(task: Task) = task is EchoTask

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
            this.result = result
            reporter?.publishEvent(result!!)
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    @DisplayName("""
    Når en EchoTask finnes i TaskStore
    Hvis polleren prosesserer tasken og lytteren produserer en EchoEvent
    Så skal eventen publiseres og metadata inneholde korrekt avledningskjede
    """)
    fun scenario1() = runTest {
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
    @DisplayName("""
    Når en task blir akseptert av lytteren
    Hvis polleren tidligere har økt backoff
    Så skal backoff resettes til startverdi
    """)
    fun pollerResetsBackoffWhenTaskAccepted() = runTest {
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
    @DisplayName("""
    Når polleren ikke finner noen tasks
    Hvis ingen lyttere har noe å gjøre
    Så skal backoff dobles
    """)
    fun pollerIncreasesBackoffWhenNoTasks() = runTest {
        val poller = object : TaskPollerImplementation(taskStore, reporterFactory) {}
        val initialBackoff = poller.backoff

        poller.pollOnce()

        assertEquals(initialBackoff.multiply(2), poller.backoff)
    }

    @Test
    @DisplayName("""
    Når en task finnes men ingen lyttere støtter den
    Hvis polleren forsøker å prosessere tasken
    Så skal backoff dobles
    """)
    fun pollerIncreasesBackoffWhenNoListenerSupportsTask() = runTest {
        val poller = object : TaskPollerImplementation(taskStore, reporterFactory) {}
        val initialBackoff = poller.backoff

        // as long as the task is not added to registry this will be unsupported
        val unsupportedTask = EchoTask("Hello").newReferenceId()
        taskStore.persist(unsupportedTask)

        poller.pollOnce()

        assertEquals(initialBackoff.multiply(2), poller.backoff)
    }

    @Test
    @DisplayName("""
    Når en lytter er opptatt
    Hvis polleren forsøker å prosessere en task
    Så skal backoff dobles
    """)
    fun pollerIncreasesBackoffWhenListenerBusy() = runTest {
        val busyListener = object : EchoListener() {
            override val isBusy = true
        }

        val poller = object : TaskPollerImplementation(taskStore, reporterFactory) {}
        val initialBackoff = poller.backoff

        val task = EchoTask("Busy").newReferenceId()
        taskStore.persist(task)

        poller.pollOnce()

        assertEquals(initialBackoff.multiply(2), poller.backoff)
    }

    @Test
    @DisplayName("""
    Når en task ikke kan claimes av polleren
    Hvis claim-operasjonen feiler
    Så skal backoff dobles
    """)
    fun pollerIncreasesBackoffWhenTaskNotClaimed() = runTest {
        TaskTypeRegistry.register(EchoTask::class.java)

        val task = EchoTask("Unclaimable").newReferenceId()
        taskStore.persist(task)

        val failingStore = object : InMemoryTaskStore() {
            override fun claim(taskId: UUID, workerId: String) = false
        }

        val poller = object : TaskPollerImplementation(failingStore, reporterFactory) {}
        val initialBackoff = poller.backoff

        failingStore.persist(task)
        poller.pollOnce()

        assertEquals(initialBackoff.multiply(2), poller.backoff)
    }
}

