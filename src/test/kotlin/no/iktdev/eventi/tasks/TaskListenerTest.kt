package no.iktdev.eventi.tasks

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
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

    // -------------------------
    // Fake Task + Reporter
    // -------------------------

    class FakeTask : Task()

    class FakeReporter : TaskReporter {
        var claimed = false
        var consumed = false
        var logs = mutableListOf<String>()
        var events = mutableListOf<Event>()

        override fun markClaimed(taskId: UUID, workerId: String) { claimed = true }
        override fun markCompleted(taskId: UUID) { consumed = true }
        override fun markFailed(referenceId: UUID, taskId: UUID) { consumed = true }
        override fun markCancelled(referenceId: UUID, taskId: UUID) {}
        override fun updateProgress(taskId: UUID, progress: Int) {}
        override fun publishEvent(event: Event) { events.add(event) }
        override fun updateLastSeen(taskId: UUID) {}
        override fun log(taskId: UUID, message: String) { logs.add(message) }
    }

    // -------------------------
    // Tests
    // -------------------------

    @Test
    @DisplayName("""
    Når onTask starter heartbeat-runner
    Hvis tasken fullføres normalt
    Så skal heartbeat kjøre, kanselleres og state nullstilles etterpå
    """)
    fun heartbeatStartsAndStopsCorrectly() = runTest {
        val listener = object : TaskListener() {

            var heartbeatStarted: Job? = null
            var heartbeatRan: Boolean = false
                private set

            var onTaskCalled = false

            override fun getWorkerId() = "worker"
            override fun createIncompleteStateTaskEvent(
                task: Task,
                status: TaskStatus,
                exception: Exception?
            ): Event {
                return object : Event() {}
            }

            override fun supports(task: Task) = true

            override suspend fun onTask(task: Task): Event? {
                onTaskCalled = true

                withHeartbeatRunner(10.milliseconds) {
                    heartbeatRan = true
                }.also { heartbeatStarted = it }

                // Gi heartbeat en sjanse til å kjøre
                yield()

                return object : Event() {}
            }
        }

        val reporter = FakeReporter()
        val task = FakeTask()

        val accepted = listener.accept(task, reporter)
        assertTrue(accepted)

        listener.currentJob!!.join()

        assertNotNull(listener.heartbeatStarted)
        assertFalse(listener.heartbeatStarted!!.isActive)
        assertTrue(listener.heartbeatRan)
        assertNull(listener.heartbeatRunner)
        assertNull(listener.currentJob)
        assertNull(listener.currentTask)
        assertNull(listener.reporter)
    }

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

            var heartbeatStarted: Job? = null
            var heartbeatRan = false
            override fun getWorkerId(): String {
                return UUID.randomUUID().toString()
            }

            override fun createIncompleteStateTaskEvent(
                task: Task,
                status: TaskStatus,
                exception: Exception?
            ): Event {
                return object : Event() {}
            }

            override fun supports(task: Task) = true

            override suspend fun onTask(task: Task): Event {

                // Start heartbeat
                withHeartbeatRunner(10.milliseconds) {
                    heartbeatRan = true
                }.also { heartbeatStarted = it }

                // Simuler annen coroutine-oppgave (VideoTaskListener/Converter)
                launch {
                    delay(30)
                    otherWorkCompleted.complete(Unit)
                }

                // ⭐ Ikke fullfør onTask før testen sier det
                allowFinish.await()
                return object : Event() {}
            }
        }

        val reporter = FakeReporter()
        val task = FakeTask()

        listener.accept(task, reporter)

        // Vent på annen jobb
        otherWorkCompleted.await()

        // ⭐ Nå er onTask fortsatt i live, cleanup har ikke skjedd
        assertNotNull(listener.currentJob)
        assertTrue(listener.currentJob!!.isActive)

        // Heartbeat kjørte
        assertNotNull(listener.heartbeatStarted)
        assertTrue(listener.heartbeatRan)

        // ⭐ Nå lar vi onTask fullføre
        allowFinish.complete(Unit)

        // Vent på listener-jobben
        listener.currentJob!!.join()

        // Heartbeat ble kansellert
        assertFalse(listener.heartbeatStarted!!.isActive)

        // Cleanup
        assertNull(listener.heartbeatRunner)
        assertNull(listener.currentJob)
        assertNull(listener.currentTask)
    }

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

            var heartbeatStarted: Job? = null
            var heartbeatRan = false

            override fun getWorkerId() = "worker"
            override fun createIncompleteStateTaskEvent(
                task: Task,
                status: TaskStatus,
                exception: Exception?
            ): Event {
                return object : Event() {}
            }

            override fun supports(task: Task) = true

            override suspend fun onTask(task: Task): Event? {

                // Start heartbeat
                withHeartbeatRunner(10.milliseconds) {
                    heartbeatRan = true
                }.also { heartbeatStarted = it }

                // Simuler Converter (CPU)
                launch(Dispatchers.Default) {
                    repeat(1000) { /* CPU work */ }
                    converterDone.complete(Unit)
                }

                // Simuler VideoTaskListener (IO)
                launch(Dispatchers.IO) {
                    delay(40)
                    videoDone.complete(Unit)
                }

                // ⭐ Vent til testen sier "nå kan du fullføre"
                allowFinish.await()
                return object : Event() {}
            }
        }

        val reporter = FakeReporter()
        val task = FakeTask()

        listener.accept(task, reporter)

        // Vent på begge "andre" oppgaver
        converterDone.await()
        videoDone.await()

        // ⭐ Verifiser at begge faktisk ble fullført
        assertTrue(converterDone.isCompleted)
        assertTrue(videoDone.isCompleted)

        // ⭐ Nå er onTask fortsatt i live, cleanup har ikke skjedd
        assertNotNull(listener.currentJob)
        assertTrue(listener.currentJob!!.isActive)

        // Heartbeat kjørte
        assertNotNull(listener.heartbeatStarted)
        assertTrue(listener.heartbeatRan)

        // ⭐ Nå lar vi onTask fullføre
        allowFinish.complete(Unit)

        // Vent på listener-jobben
        listener.currentJob!!.join()

        // Heartbeat ble kansellert
        assertFalse(listener.heartbeatStarted!!.isActive)

        // Cleanup
        assertNull(listener.heartbeatRunner)
        assertNull(listener.currentJob)
        assertNull(listener.currentTask)
    }

    @Test
    @DisplayName("""
    Når onTask gjør ferdig arbeid
    Hvis heartbeat kjører parallelt
    Så skal heartbeat kjøre, kanselleres og state nullstilles
    """)
    fun taskWorkCompletesAndHeartbeatBehaves() = runTest {
        val workCompleted = CompletableDeferred<Unit>()

        val listener = object : TaskListener() {

            var heartbeatStarted: Job? = null
            var heartbeatRan = false
            var onTaskCalled = false

            override fun getWorkerId() = "worker"
            override fun createIncompleteStateTaskEvent(
                task: Task,
                status: TaskStatus,
                exception: Exception?
            ): Event {
                return object : Event() {}
            }

            override fun supports(task: Task) = true

            override suspend fun onTask(task: Task): Event {
                onTaskCalled = true

                withHeartbeatRunner(10.milliseconds) {
                    heartbeatRan = true
                }.also { heartbeatStarted = it }

                // Simuler arbeid
                delay(20)

                // ⭐ signaliser at arbeidet er ferdig
                workCompleted.complete(Unit)

                return object : Event() {}
            }
        }

        val reporter = FakeReporter()
        val task = FakeTask()

        val accepted = listener.accept(task, reporter)
        assertTrue(accepted)

        // ⭐ Verifiser at arbeidet faktisk ble fullført
        workCompleted.await()

        // Vent på jobben
        listener.currentJob!!.join()

        // onTask ble kalt
        assertTrue(listener.onTaskCalled)

        // Heartbeat ble startet
        assertNotNull(listener.heartbeatStarted)
        assertTrue(listener.heartbeatRan)

        // Heartbeat ble kansellert
        assertFalse(listener.heartbeatStarted!!.isActive)

        // Cleanup
        assertNull(listener.heartbeatRunner)
        assertNull(listener.currentJob)
        assertNull(listener.currentTask)
        assertNull(listener.reporter)
    }

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
                task: Task,
                status: TaskStatus,
                exception: Exception?
            ): Event {
                return object : Event() {}
            }

            override fun supports(task: Task) = true

            override suspend fun onTask(task: Task): Event? {
                // Hold jobben i live
                allowFinish.await()
                return object : Event() {}
            }
        }

        val reporter = FakeReporter()
        val task1 = FakeTask()
        val task2 = FakeTask()

        // Første task aksepteres
        val accepted1 = listener.accept(task1, reporter)
        assertTrue(accepted1)

        // Listener er busy → andre task skal avvises
        val accepted2 = listener.accept(task2, reporter)
        assertFalse(accepted2)

        // Fullfør første task
        allowFinish.complete(Unit)
        listener.currentJob!!.join()

        // Cleanup
        assertNull(listener.currentJob)
        assertNull(listener.currentTask)
    }

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
                task: Task,
                status: TaskStatus,
                exception: Exception?
            ): Event {
                return object : Event() {}
            }

            override fun supports(task: Task) = false
            override suspend fun onTask(task: Task): Event? = error("Should not be called")
        }

        val reporter = FakeReporter()
        val task = FakeTask()

        val accepted = listener.accept(task, reporter)

        assertFalse(accepted)
        assertNull(listener.currentJob)
        assertNull(listener.currentTask)
        assertNull(listener.reporter)
    }

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
                task: Task,
                status: TaskStatus,
                exception: Exception?
            ): Event {
                return object : Event() {}
            }

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
        val task = FakeTask().newReferenceId()

        listener.accept(task, reporter)

        // Vent på error-path
        errorLogged.await()

        // ⭐ Vent på at cleanup i finally kjører
        listener.currentJob?.join()

        // Cleanup verifisering
        assertNull(listener.currentJob)
        assertNull(listener.currentTask)
        assertNull(listener.heartbeatRunner)
    }

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
                task: Task,
                status: TaskStatus,
                exception: Exception?
            ): Event {
                return object : Event() {}
            }

            override fun supports(task: Task) = true

            override suspend fun onTask(task: Task): Event? {
                allowStart.complete(Unit)
                delay(Long.MAX_VALUE) // hold jobben i live
                return null
            }

            override fun onCancelled(task: Task) {
                super.onCancelled(task)
                cancelledCalled.complete(Unit)
            }
        }

        val reporter = FakeReporter()
        val task = FakeTask().newReferenceId()

        listener.accept(task, reporter)

        // Vent til onTask har startet
        allowStart.await()

        // Kanseller jobben
        listener.currentJob!!.cancel()

        // Vent til onCancelled() ble kalt
        cancelledCalled.await()

        // ⭐ Vent til cleanup i finally har kjørt
        listener.currentJob?.join()

        // Cleanup verifisering
        assertNull(listener.currentJob)
        assertNull(listener.currentTask)
        assertNull(listener.heartbeatRunner)
    }

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
                task: Task,
                status: TaskStatus,
                exception: Exception?
            ): Event {
                return object : Event() {}
            }

            override fun supports(task: Task) = true

            override suspend fun onTask(task: Task): Event {
                callCount++

                if (callCount == 1) {
                    started1.complete(Unit)   // signal: coroutine har startet
                    finish1.await()           // vent til testen sier "fortsett"
                }

                if (callCount == 2) {
                    started2.complete(Unit)
                    finish2.await()
                }

                return object : Event() {}
            }
        }

        val reporter = FakeReporter()

        // --- Task 1 ---
        val task1 = FakeTask()
        listener.accept(task1, reporter)

        started1.await()         // garanterer at coroutine kjører
        finish1.complete(Unit)   // la coroutine fullføre
        listener.currentJob!!.join()

        // Verifiser cleanup
        assertNull(listener.currentJob)
        assertNull(listener.currentTask)
        assertNull(listener.heartbeatRunner)

        // --- Task 2 ---
        val task2 = FakeTask()
        listener.accept(task2, reporter)

        started2.await()
        finish2.complete(Unit)
        listener.currentJob!!.join()

        // Verifiser cleanup igjen
        assertNull(listener.currentJob)
        assertNull(listener.currentTask)
        assertNull(listener.heartbeatRunner)

        // onTask ble kalt to ganger
        assertEquals(2, listener.callCount)
    }

}
