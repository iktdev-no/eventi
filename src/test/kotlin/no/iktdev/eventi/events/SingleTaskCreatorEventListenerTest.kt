package no.iktdev.eventi.events

import no.iktdev.eventi.TestBase
import no.iktdev.eventi.models.*
import no.iktdev.eventi.models.store.TaskStatus
import no.iktdev.eventi.registry.EventTypeRegistry
import no.iktdev.eventi.registry.TaskTypeRegistry
import no.iktdev.eventi.serialization.ZDS.toTask
import no.iktdev.eventi.testUtil.wipe
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.util.UUID

class SingleTaskCreatorEventListenerTest : TestBase() {

    // ---------------------------------------------------------
    // TEST EVENTKLASSER
    // ---------------------------------------------------------

    class TriggerEvent : Event()
    class CreatedEvent(taskId: UUID) : SingleTaskCratedEvent(taskId)
    class ResultEvent(): Event()
    class ABasicEvent: Event()
    class ADeleteEvent(deletedEventId: UUID) : DeleteEvent(deletedEventId)

    // ---------------------------------------------------------
    // TEST TASK
    // ---------------------------------------------------------

    class ASimpleTask : Task()

    // ---------------------------------------------------------
    // TEST LISTENER
    // ---------------------------------------------------------

    private val listener = object : SingleTaskCreatorEventListener(eventStore, taskStore) {

        override fun isEventOfMyCreation(event: Event): Boolean =
            event is CreatedEvent

        override fun onCreateTask(event: Event, history: List<Event>): Task? {
            if (event !is TriggerEvent) return null
            return ASimpleTask().derivedOf(event)
        }

        override fun onTaskCreated(event: Event, history: List<Event>, task: Task): SingleTaskCratedEvent =
            CreatedEvent(task.taskId).derivedOf(event)
    }

    // ---------------------------------------------------------
    // REGISTRERING AV TYPER
    // ---------------------------------------------------------

    @BeforeEach
    fun setup() {
        EventTypeRegistry.wipe()
        TaskTypeRegistry.wipe()

        EventTypeRegistry.register(
            listOf(
                TriggerEvent::class.java,
                CreatedEvent::class.java,
                ResultEvent::class.java,
                ABasicEvent::class.java,
                ADeleteEvent::class.java,
            )
        )

        TaskTypeRegistry.register(
            listOf(
                ASimpleTask::class.java
            )
        )
    }

    private fun trigger(): Event = TriggerEvent().newReferenceId()

    // ---------------------------------------------------------
    // NORMAL PATH
    // ---------------------------------------------------------

    @Test
    @DisplayName("Normal path → onCreateTask + onTaskCreated → returns CreatedEvent")
    fun normalPathCreatesTask() {
        val t = trigger()

        val result = listener.onEvent(t, listOf(t))

        assertNotNull(result)
        assertTrue(result is CreatedEvent)

        val created = result as CreatedEvent
        val stored = taskStore.findByTaskId(created.taskId)?.toTask()

        assertNotNull(stored)
        assertEquals(created.taskId, stored!!.taskId)
        assertEquals(t.referenceId, stored.referenceId)
        assertEquals(setOf(created.eventId), stored.metadata.derivedFromId)
    }

    // ---------------------------------------------------------
    // RECOVERY PATH — recreate missing task
    // ---------------------------------------------------------

    @Test
    @DisplayName("""
SingleTaskCreatorEventListener
Når ResultEvent slettes og CreatedEvent finnes i effective history
Og tasken mangler i TaskStore
Så skal recovery gjenopprette tasken korrekt
""")
    fun recoveryRecreatesMissingTask() {
        val t = trigger()

        val dummyTask = ASimpleTask()

        val created = CreatedEvent(dummyTask.taskId).derivedOf(t)
        eventStore.persist(created)

        dummyTask.apply {
            this.derivedOf(created)
        }

        val result = ResultEvent().producedFrom(dummyTask)
        eventStore.persist(result)

        val delete = ADeleteEvent(result.eventId).derivedOf(result)

        val outcome = listener.onEvent(delete, listOf(t, created, result, delete))

        assertNull(outcome)

        val recreated = taskStore.findByTaskId(created.taskId)?.toTask()
        assertNotNull(recreated)
        assertEquals(t.referenceId, recreated!!.referenceId)
        assertEquals(setOf(created.eventId), recreated.metadata.derivedFromId)
    }


    @Test
    @DisplayName("""
SingleTaskCreatorEventListener
Når ResultEvent slettes men CreatedEvent mangler i effective history
Og chainen dermed er brutt
Så skal recovery ikke gjenopprette tasken
""")
    fun recoveryCannotRecreateWhenCreatorMissing() {
        val t = trigger()

        val created = CreatedEvent(UUID.randomUUID()).derivedOf(t)
        eventStore.persist(created)

        val result = ResultEvent().derivedOf(created)
        eventStore.persist(result)

        val delete = ADeleteEvent(result.eventId).derivedOf(result)

        // Effective history MANGLER CreatedEvent
        val outcome = listener.onEvent(delete, listOf(t, result, delete))

        assertNull(outcome)

        val recreated = taskStore.findByTaskId(created.taskId)?.toTask()
        assertNull(recreated)
    }


    // ---------------------------------------------------------
    // RECOVERY PATH — reset consumed task
    // ---------------------------------------------------------

    @Test
    @DisplayName("Recovery → consumed task → reset")
    fun recoveryResetsConsumedTask() {
        val t = trigger()

        val created = CreatedEvent(UUID.randomUUID()).derivedOf(t)
        eventStore.persist(created)

        val task = ASimpleTask()
            .derivedOf(created)
            .apply {
                reUseTaskId(created.taskId)
                state = TaskState(consumed = true, status = TaskStatus.Completed)
            }

        taskStore.persist(task)

        val delete = ADeleteEvent(created.eventId)
            .derivedOf(created)

        listener.onEvent(delete, listOf(t, created, delete))

        val reset = taskStore.findByTaskId(created.taskId)
        assertEquals(TaskStatus.Pending, reset!!.status)
        assertFalse(reset.consumed)
    }

    // ---------------------------------------------------------
    // RECOVERY PATH — reset failed task
    // ---------------------------------------------------------

    @Test
    @DisplayName("Recovery → failed task → reset")
    fun recoveryResetsFailedTask() {
        val t = trigger()

        val created = CreatedEvent(UUID.randomUUID()).derivedOf(t)
        eventStore.persist(created)

        val task = ASimpleTask()
            .derivedOf(created)
            .apply {
                reUseTaskId(created.taskId)
                state = TaskState(consumed = false, status = TaskStatus.Failed)
            }

        taskStore.persist(task)

        val delete = ADeleteEvent(created.eventId)
            .derivedOf(created)

        listener.onEvent(delete, listOf(t, created, delete))

        val reset = taskStore.findByTaskId(created.taskId)
        assertEquals(TaskStatus.Pending, reset!!.status)
        assertFalse(reset.consumed)
    }

    // ---------------------------------------------------------
    // IGNORE PATH — wrong listener
    // ---------------------------------------------------------

    @Test
    @DisplayName("DeleteEvent for wrong listener → ignore")
    fun ignoreWrongListener() {
        val t = trigger()

        val wrong = ABasicEvent().derivedOf(t)
        EventTypeRegistry.register(wrong::class.java)
        eventStore.persist(wrong)

        val delete = ADeleteEvent(wrong.eventId)
            .derivedOf(wrong)

        val result = listener.onEvent(delete, listOf(t, wrong, delete))

        assertNull(result)
        assertTrue(taskStore.findByReferenceId(t.referenceId).isEmpty())
    }

    // ---------------------------------------------------------
    // IGNORE PATH — no matching creator event
    // ---------------------------------------------------------

    @Test
    @DisplayName("DeleteEvent without matching creator event → ignore")
    fun ignoreMissingCreatorEvent() {
        val t = trigger()

        val delete = ADeleteEvent(UUID.randomUUID())
            .derivedOf(t)

        val result = listener.onEvent(delete, listOf(t, delete))

        assertNull(result)
    }
}
