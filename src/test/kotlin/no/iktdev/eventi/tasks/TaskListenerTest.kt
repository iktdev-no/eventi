package no.iktdev.eventi.tasks

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.Task
import no.iktdev.eventi.models.store.TaskStatus
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.time.Duration.Companion.milliseconds

@DisplayName("""
TaskListener
Når en task prosesseres i en coroutine med heartbeat
Hvis lytteren håndterer arbeid, feil, avbrudd og sekvensiell kjøring
Så skal state, heartbeat og cleanup fungere korrekt
""")
class TaskListenerTest {

    class FakeTask : Task()

    class FakeReporter : TaskReporter {
        var claimed = false
        var completed = false
        var failed = false
        var cancelled = false
        val logs = mutableListOf<String>()
        val events = mutableListOf<Event>()

        override fun markClaimed(taskId: UUID, workerId: String) { claimed = true }
        override fun markCompleted(taskId: UUID) { completed = true }
        override fun markFailed(referenceId: UUID, taskId: UUID) { failed = true }
        override fun markCancelled(referenceId: UUID, taskId: UUID) { cancelled = true }
        override fun updateProgress(taskId: UUID, progress: Int) {}
        override fun publishEvent(event: Event) { events.add(event) }
        override fun updateLastSeen(taskId: UUID) {}
        override fun log(taskId: UUID, message: String) { logs.add(message) }
    }

    // ---------------------------------------------------------
    // 1 — Heartbeat starter og stopper riktig
    // ---------------------------------------------------------
    @Test
    @DisplayName("""
    Når onTask starter heartbeat-runner
    Hvis tasken fullføres normalt
    Så skal heartbeat kjøre, kanselleres og state nullstilles etterpå
    """)
    fun heartbeatStartsAndStopsCorrectly() = runTest {
        val listener = object : TaskListener() {

            var heartbeatRan = false
            var onTaskCalled = false

            override fun getWorkerId() = "worker"

            override fun createIncompleteStateTaskEvent(
                task: Task, status: TaskStatus, exception: Exception?
            ) = object : Event() {}

            override fun supports(task: Task) = true

            override suspend fun onTask(task: Task): Event? {
                onTaskCalled = true

                withHeartbeatRunner(10.milliseconds) {
                    heartbeatRan = true
                }

                yield()
                return object : Event() {}
            }
        }

        val reporter = FakeReporter()
        listener.accept(FakeTask(), reporter)

        listener.currentJob?.join()

        assertTrue(listener.heartbeatRan)
        assertNull(listener.heartbeatRunner)
        assertNull(listener.currentJob)
        assertNull(listener.currentTask)
        assertNull(listener.reporter)
    }

    // ---------------------------------------------------------
    // 2 — Heartbeat blokkerer ikke annen jobb
    // ---------------------------------------------------------
    @Test
    @DisplayName("""
    Når heartbeat kjører i bakgrunnen
    Hvis onTask gjør annen coroutine-arbeid samtidig
    Så skal heartbeat ikke blokkere annet arbeid
    """)
    fun heartbeatDoesNotBlockOtherWork() = runTest {
        val otherWorkCompleted = CompletableDeferred<Unit>()
        val allowFinish = CompletableDeferred<Unit>()

        val listener = object : TaskListener() {

            var heartbeatRan = false

            override fun getWorkerId() = "worker"

            override fun createIncompleteStateTaskEvent(
                task: Task, status: TaskStatus, exception: Exception?
            ) = object : Event() {}

            override fun supports(task: Task) = true

            override suspend fun onTask(task: Task): Event {
                withHeartbeatRunner(10.milliseconds) {
                    heartbeatRan = true
                }

                launch {
                    delay(30)
                    otherWorkCompleted.complete(Unit)
                }

                allowFinish.await()
                return object : Event() {}
            }
        }

        val reporter = FakeReporter()
        listener.accept(FakeTask(), reporter)

        otherWorkCompleted.await()

        assertTrue(listener.heartbeatRan)
        assertNotNull(listener.currentJob)
        assertTrue(listener.currentJob!!.isActive)

        allowFinish.complete(Unit)
        listener.currentJob?.join()

        assertNull(listener.heartbeatRunner)
        assertNull(listener.currentJob)
        assertNull(listener.currentTask)
    }

    // ---------------------------------------------------------
    // 3 — Heartbeat + CPU + IO arbeid
    // ---------------------------------------------------------
    @Test
    @DisplayName("""
    Når heartbeat kjører og flere parallelle jobber startes
    Hvis både CPU- og IO-arbeid fullføres
    Så skal heartbeat fortsatt kjøre og cleanup skje etterpå
    """)
    fun heartbeatAndConcurrentTasksRunCorrectly() = runTest {
        val converterDone = CompletableDeferred<Unit>()
        val videoDone = CompletableDeferred<Unit>()
        val allowFinish = CompletableDeferred<Unit>()

        val listener = object : TaskListener() {

            var heartbeatRan = false

            override fun getWorkerId() = "worker"

            override fun createIncompleteStateTaskEvent(
                task: Task, status: TaskStatus, exception: Exception?
            ) = object : Event() {}

            override fun supports(task: Task) = true

            override suspend fun onTask(task: Task): Event? {
                withHeartbeatRunner(10.milliseconds) {
                    heartbeatRan = true
                }

                launch(Dispatchers.Default) {
                    repeat(1000) {}
                    converterDone.complete(Unit)
                }

                launch(Dispatchers.IO) {
                    delay(40)
                    videoDone.complete(Unit)
                }

                allowFinish.await()
                return object : Event() {}
            }
        }

        val reporter = FakeReporter()
        listener.accept(FakeTask(), reporter)

        converterDone.await()
        videoDone.await()

        assertTrue(listener.heartbeatRan)
        assertNotNull(listener.currentJob)

        allowFinish.complete(Unit)
        listener.currentJob?.join()

        assertNull(listener.heartbeatRunner)
        assertNull(listener.currentJob)
        assertNull(listener.currentTask)
    }

    // ---------------------------------------------------------
    // 4 — Arbeid fullføres, heartbeat kjører
    // ---------------------------------------------------------
    @Test
    @DisplayName("""
    Når onTask gjør ferdig arbeid
    Hvis heartbeat kjører parallelt
    Så skal heartbeat kjøre, kanselleres og state nullstilles
    """)
    fun taskWorkCompletesAndHeartbeatBehaves() = runTest {
        val workCompleted = CompletableDeferred<Unit>()

        val listener = object : TaskListener() {

            var heartbeatRan = false
            var onTaskCalled = false

            override fun getWorkerId() = "worker"

            override fun createIncompleteStateTaskEvent(
                task: Task, status: TaskStatus, exception: Exception?
            ) = object : Event() {}

            override fun supports(task: Task) = true

            override suspend fun onTask(task: Task): Event {
                onTaskCalled = true

                withHeartbeatRunner(10.milliseconds) {
                    heartbeatRan = true
                }

                delay(20)
                workCompleted.complete(Unit)

                return object : Event() {}
            }
        }

        val reporter = FakeReporter()
        listener.accept(FakeTask(), reporter)

        workCompleted.await()
        listener.currentJob?.join()

        assertTrue(listener.onTaskCalled)
        assertTrue(listener.heartbeatRan)

        assertNull(listener.heartbeatRunner)
        assertNull(listener.currentJob)
        assertNull(listener.currentTask)
        assertNull(listener.reporter)
    }

    // ---------------------------------------------------------
    // 5 — accept() returnerer false når busy
    // ---------------------------------------------------------
    @Test
    @DisplayName("""
    Når listener er opptatt med en task
    Hvis en ny task forsøkes akseptert
    Så skal accept() returnere false
    """)
    fun acceptReturnsFalseWhenBusy() = runTest {
        val allowFinish = CompletableDeferred<Unit>()

        val listener = object : TaskListener() {
            override fun getWorkerId() = "worker"

            override fun createIncompleteStateTaskEvent(
                task: Task, status: TaskStatus, exception: Exception?
            ) = object : Event() {}

            override fun supports(task: Task) = true

            override suspend fun onTask(task: Task): Event? {
                allowFinish.await()
                return object : Event() {}
            }
        }

        val reporter = FakeReporter()

        assertTrue(listener.accept(FakeTask(), reporter))
        assertFalse(listener.accept(FakeTask(), reporter))

        allowFinish.complete(Unit)
        listener.currentJob?.join()

        assertNull(listener.currentJob)
        assertNull(listener.currentTask)
    }

    // ---------------------------------------------------------
    // 6 — accept() returnerer false når unsupported
    // ---------------------------------------------------------
    @Test
    @DisplayName("""
    Når supports() returnerer false
    Hvis accept() kalles
    Så skal listener avvise tasken uten å starte jobb
    """)
    fun acceptReturnsFalseWhenUnsupported() = runTest {
        val listener = object : TaskListener() {
            override fun getWorkerId() = "worker"

            override fun createIncompleteStateTaskEvent(
                task: Task, status: TaskStatus, exception: Exception?
            ) = object : Event() {}

            override fun supports(task: Task) = false
            override suspend fun onTask(task: Task): Event? = error("Should not be called")
        }

        val reporter = FakeReporter()

        assertFalse(listener.accept(FakeTask(), reporter))
        assertNull(listener.currentJob)
        assertNull(listener.currentTask)
        assertNull(listener.reporter)
    }

    // ---------------------------------------------------------
    // 7 — onError kalles når onTask kaster
    // ---------------------------------------------------------
    @Test
    @DisplayName("""
    Når onTask kaster en exception
    Hvis listener håndterer feil via onError
    Så skal cleanup kjøre og state nullstilles
    """)
    fun onErrorCalledWhenOnTaskThrows() = runTest {
        val errorLogged = CompletableDeferred<Unit>()

        val listener = object : TaskListener() {
            override fun getWorkerId() = "worker"

            override fun createIncompleteStateTaskEvent(
                task: Task, status: TaskStatus, exception: Exception?
            ) = object : Event() {}

            override fun supports(task: Task) = true

            override suspend fun onTask(task: Task): Event? {
                throw RuntimeException("boom")
            }

            override fun onError(task: Task, exception: Exception) {
                super.onError(task, exception)
                errorLogged.complete(Unit)
            }
        }

        val reporter = FakeReporter()
        listener.accept(FakeTask().newReferenceId(), reporter)

        errorLogged.await()
        listener.currentJob?.join()

        assertNull(listener.currentJob)
        assertNull(listener.currentTask)
        assertNull(listener.heartbeatRunner)
    }

    // ---------------------------------------------------------
    // 8 — onCancelled kalles når jobben kanselleres
    // ---------------------------------------------------------
    @Test
    @DisplayName("""
    Når jobben kanselleres mens onTask kjører
    Hvis listener implementerer onCancelled
    Så skal onCancelled kalles og cleanup skje
    """)
    fun onCancelledCalledWhenJobCancelled() = runTest {
        val allowStart = CompletableDeferred<Unit>()
        val cancelledCalled = CompletableDeferred<Unit>()

        val listener = object : TaskListener() {
            override fun getWorkerId() = "worker"

            override fun createIncompleteStateTaskEvent(
                task: Task, status: TaskStatus, exception: Exception?
            ) = object : Event() {}

            override fun supports(task: Task) = true

            override suspend fun onTask(task: Task): Event? {
                allowStart.complete(Unit)
                delay(Long.MAX_VALUE)
                return null
            }

            override fun onCancelled(task: Task) {
                super.onCancelled(task)
                cancelledCalled.complete(Unit)
            }
        }

        val reporter = FakeReporter()
        listener.accept(FakeTask().newReferenceId(), reporter)

        allowStart.await()
        listener.currentJob!!.cancel()

        cancelledCalled.await()
        listener.currentJob?.join()

        assertNull(listener.currentJob)
        assertNull(listener.currentTask)
        assertNull(listener.heartbeatRunner)
    }

    // ---------------------------------------------------------
    // 9 — Sekvensiell kjøring uten state‑lekkasje
    // ---------------------------------------------------------
    @Test
    @DisplayName("""
    Når listener prosesserer to tasks sekvensielt
    Hvis cleanup fungerer riktig
    Så skal ingen state lekke mellom tasks
    """)
    fun listenerHandlesSequentialTasksWithoutLeakingState() = runTest {
        val started1 = CompletableDeferred<Unit>()
        val finish1 = CompletableDeferred<Unit>()

        val started2 = CompletableDeferred<Unit>()
        val finish2 = CompletableDeferred<Unit>()

        val listener = object : TaskListener() {

            var callCount = 0

            override fun getWorkerId() = "worker"

            override fun createIncompleteStateTaskEvent(
                task: Task, status: TaskStatus, exception: Exception?
            ) = object : Event() {}

            override fun supports(task: Task) = true

            override suspend fun onTask(task: Task): Event {
                callCount++

                if (callCount == 1) {
                    started1.complete(Unit)
                    finish1.await()
                }

                if (callCount == 2) {
                    started2.complete(Unit)
                    finish2.await()
                }

                return object : Event() {}
            }
        }

        val reporter = FakeReporter()

        listener.accept(FakeTask(), reporter)
        started1.await()
        finish1.complete(Unit)
        listener.currentJob?.join()

        assertNull(listener.currentJob)
        assertNull(listener.currentTask)
        assertNull(listener.heartbeatRunner)

        listener.accept(FakeTask(), reporter)
        started2.await()
        finish2.complete(Unit)
        listener.currentJob?.join()

        assertNull(listener.currentJob)
        assertNull(listener.currentTask)
        assertNull(listener.heartbeatRunner)

        assertEquals(2, listener.callCount)
    }
}
