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

class MultiTaskCreatorEventListenerTest : TestBase() {

    // ---------------------------------------------------------
    // TEST EVENTKLASSER
    // ---------------------------------------------------------

    class TriggerEvent : Event()
    class CreatedEvent(taskIds: Set<MultiTaskIdentity>) : MultiTaskCreatedEvent(taskIds)
    class ResultEvent : Event()
    class ABasicEvent : Event()
    class ADeleteEvent(deletedEventId: UUID) : DeleteEvent(deletedEventId)

    // ---------------------------------------------------------
    // TEST TASK
    // ---------------------------------------------------------

    class ATask(val name: String) : Task()

    // ---------------------------------------------------------
    // TEST LISTENER
    // ---------------------------------------------------------

    private val listener = object : MultiTaskCreatorEventListener(eventStore, taskStore) {

        override fun isEventOfMyCreation(event: Event): Boolean =
            event is CreatedEvent

        override fun onCreateTask(event: Event, history: List<Event>): List<Task> {
            if (event !is TriggerEvent) return emptyList()

            return listOf(
                ATask("A").derivedOf(event),
                ATask("B").derivedOf(event),
                ATask("C").derivedOf(event)
            )
        }

        override fun onTasksCreated(event: Event, history: List<Event>, tasks: List<Task>): MultiTaskCreatedEvent {
            val identities = tasks.map {
                MultiTaskIdentity(it.taskId, onGetTaskIdentity(it))
            }.toSet()

            return CreatedEvent(identities).derivedOf(event)
        }

        override fun onGetTaskIdentity(task: Task): String {
            return (task as ATask).name
        }
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
                ATask::class.java
            )
        )
    }

    private fun trigger(): Event = TriggerEvent().newReferenceId()

    // ---------------------------------------------------------
    // NORMAL PATH
    // ---------------------------------------------------------

    @Test
    @DisplayName("Normal path → onCreateTask + onTasksCreated → returns MultiTaskCreatedEvent")
    fun normalPathCreatesTasks() {
        val t = trigger()

        val result = listener.onEvent(t, listOf(t))

        assertNotNull(result)
        assertTrue(result is CreatedEvent)

        val created = result as CreatedEvent
        assertEquals(3, created.taskIds.size)

        created.taskIds.forEach { identity ->
            val stored = taskStore.findByTaskId(identity.taskId)?.toTask()
            assertNotNull(stored)
            assertEquals(identity.identity, listener.onGetTaskIdentity(stored!!))
            assertEquals(t.referenceId, stored.referenceId)
            assertEquals(setOf(created.eventId), stored.metadata.derivedFromId)
        }
    }

    // ---------------------------------------------------------
    // RECOVERY — recreate missing tasks
    // ---------------------------------------------------------

    @Test
    @DisplayName("Recovery → missing tasks → recreate only missing")
    fun recoveryRecreatesMissingTasks() {
        val t = trigger()

        // Normal creation
        val created = listener.onEvent(t, listOf(t)) as CreatedEvent

        // Mark all tasks consumed (IMPORTANT!)
        created.taskIds.forEach {
            taskStore.markConsumed(it.taskId, TaskStatus.Completed)
        }

        // Remove ONE task from store
        val missing = created.taskIds.first()
        taskStore.deleteTasksById(missing.taskId)

        // Produce a result event
        val result = ResultEvent().derivedOf(created)
        eventStore.persist(result)

        // Delete the result event
        val delete = ADeleteEvent(result.eventId).derivedOf(result)

        // Run recovery
        listener.onEvent(delete, listOf(t, created, result, delete))

        // Missing task must be recreated
        val recreated = taskStore.findByTaskId(missing.taskId)?.toTask()
        assertNotNull(recreated)
        assertEquals(missing.identity, listener.onGetTaskIdentity(recreated!!))
    }


    // ---------------------------------------------------------
    // RECOVERY — reset existing tasks
    // ---------------------------------------------------------

    @Test
    @DisplayName("Recovery → existing tasks → reset")
    fun recoveryResetsExistingTasks() {
        val t = trigger()

        val created = listener.onEvent(t, listOf(t)) as CreatedEvent

        // Mark all tasks consumed
        created.taskIds.forEach {
            val task = taskStore.findByTaskId(it.taskId)!!.toTask()?.apply {
                state = TaskState(consumed = true, status = TaskStatus.Completed)
            }
            task?.let {
                taskStore.persist(it)
            }
        }

        val delete = ADeleteEvent(created.eventId).derivedOf(created)

        listener.onEvent(delete, listOf(t, created, delete))

        created.taskIds.forEach {
            val reset = taskStore.findByTaskId(it.taskId)
            assertEquals(TaskStatus.Pending, reset!!.status)
            assertFalse(reset.consumed)
        }
    }

    // ---------------------------------------------------------
    // RECOVERY — identity mismatch → wipe
    // ---------------------------------------------------------

    @Test
    @DisplayName("Recovery → identity mismatch → wipe + DeleteEvent")
    fun recoveryIdentityMismatchWipes() {
        val t = trigger()

        // Normal creation
        val created = listener.onEvent(t, listOf(t)) as CreatedEvent

        // Produce a result event
        val result = ResultEvent().derivedOf(created)
        eventStore.persist(result)

        // Corrupt identity in the CREATED event
        val corrupted = created.taskIds.first()
        val newIdentity = MultiTaskIdentity(corrupted.taskId, "WRONG")
        val corruptedSet = created.taskIds - corrupted + newIdentity
        val corruptedEvent = CreatedEvent(corruptedSet).apply {
            withEventId(created.eventId)
            withMetadata(created.metadata)
            usingReferenceId(created.referenceId)
        }
        eventStore.persist(corruptedEvent)
        created.taskIds.forEach {
            taskStore.markConsumed(it.taskId, TaskStatus.Completed)
        }
        // Delete the RESULT event (correct!)
        val delete = ADeleteEvent(result.eventId).derivedOf(result)

        // Effective history MUST include:
        // trigger → corruptedCreated → result → delete
        val outcome = listener.onEvent(delete, listOf(t, corruptedEvent, result, delete))

        assertTrue(outcome is MultiTaskCreatedEvent)

        // All tasks must be wiped
        corruptedSet.forEach {
            assertNull(taskStore.findByTaskId(it.taskId))
        }
    }


    @Test
    @DisplayName("Recovery → recreated missing identity → reset existing, no wipe")
    fun recoveryRecreatedMissingIdentityResets() {
        val t = trigger()

        val created = listener.onEvent(t, listOf(t)) as CreatedEvent

        // Mark consumed
        created.taskIds.forEach {
            taskStore.markConsumed(it.taskId, TaskStatus.Completed)
        }

        // Patch listener to simulate missing identity in recreated
        val hacked = object : MultiTaskCreatorEventListener(eventStore, taskStore) {
            override fun isEventOfMyCreation(event: Event) = event is CreatedEvent
            override fun onCreateTask(event: Event, history: List<Event>) =
                listOf(ATask("A").derivedOf(event), ATask("B").derivedOf(event)) // missing C
            override fun onTasksCreated(event: Event, history: List<Event>, tasks: List<Task>) =
                CreatedEvent(tasks.map { MultiTaskIdentity(it.taskId, onGetTaskIdentity(it)) }.toSet()).derivedOf(event)
            override fun onGetTaskIdentity(task: Task) = (task as ATask).name
        }

        val result = ResultEvent().derivedOf(created)
        eventStore.persist(result)
        val delete = ADeleteEvent(result.eventId).derivedOf(result)

        val outcome = hacked.onEvent(delete, listOf(t, created, result, delete))

        // Recovery should NOT wipe → outcome = null
        assertNull(outcome)

        // All original tasks should be reset (including the one recreated didn't produce)
        created.taskIds.forEach {
            val reset = taskStore.findByTaskId(it.taskId)
            assertEquals(TaskStatus.Pending, reset!!.status)
            assertFalse(reset.consumed)
        }
    }


    @Test
    @DisplayName("Recovery → recreated extra identity → wipe")
    fun recoveryRecreatedExtraIdentityWipes() {
        val t = trigger()
        val created = listener.onEvent(t, listOf(t)) as CreatedEvent

        created.taskIds.forEach {
            taskStore.markConsumed(it.taskId, TaskStatus.Completed)
        }

        val hacked = object : MultiTaskCreatorEventListener(eventStore, taskStore) {
            override fun isEventOfMyCreation(event: Event) = event is CreatedEvent
            override fun onCreateTask(event: Event, history: List<Event>) =
                listOf(ATask("A"), ATask("B"), ATask("C"), ATask("D")).map { it.derivedOf(event) }
            override fun onTasksCreated(event: Event, history: List<Event>, tasks: List<Task>) =
                CreatedEvent(tasks.map { MultiTaskIdentity(it.taskId, onGetTaskIdentity(it)) }.toSet()).derivedOf(event)
            override fun onGetTaskIdentity(task: Task) = (task as ATask).name
        }

        val result = ResultEvent().derivedOf(created)
        eventStore.persist(result)
        val delete = ADeleteEvent(result.eventId).derivedOf(result)

        val outcome = hacked.onEvent(delete, listOf(t, created, result, delete))

        assertTrue(outcome is MultiTaskCreatedEvent)
        created.taskIds.forEach { assertNull(taskStore.findByTaskId(it.taskId)) }
    }


    @Test
    @DisplayName("Recovery → store contains extra identity → rogue task is deleted")
    fun recoveryStoreExtraIdentityDeleted() {
        val t = trigger()
        val created = listener.onEvent(t, listOf(t)) as CreatedEvent

        // Mark consumed
        created.taskIds.forEach {
            taskStore.markConsumed(it.taskId, TaskStatus.Completed)
        }

        // Inject rogue task (not referenced by CreatedEvent)
        val rogue = ATask("X").derivedOf(t)
        taskStore.persist(rogue)

        val result = ResultEvent().derivedOf(created)
        eventStore.persist(result)
        val delete = ADeleteEvent(result.eventId).derivedOf(result)

        // Run recovery
        val outcome = listener.onEvent(delete, listOf(t, created, result, delete))

        // Recovery should NOT wipe → outcome = null
        assertNull(outcome)

        // Rogue task should now be deleted by validateAndCleanupConsistency()
        assertNull(taskStore.findByTaskId(rogue.taskId))

        // Existing tasks should be reset
        created.taskIds.forEach {
            val reset = taskStore.findByTaskId(it.taskId)
            assertEquals(TaskStatus.Pending, reset!!.status)
            assertFalse(reset.consumed)
        }
    }




    // ---------------------------------------------------------
    // RECOVERY — missing creator → ignore
    // ---------------------------------------------------------

    @Test
    @DisplayName("Recovery → missing creator event → ignore")
    fun recoveryMissingCreatorIgnored() {
        val t = trigger()

        val created = listener.onEvent(t, listOf(t)) as CreatedEvent

        val delete = ADeleteEvent(created.eventId).derivedOf(created)

        // Effective history mangler created
        val outcome = listener.onEvent(delete, listOf(t, delete))

        assertNull(outcome)
    }

    // ---------------------------------------------------------
    // RECOVERY — wrong listener → ignore
    // ---------------------------------------------------------

    @Test
    @DisplayName("Recovery → wrong listener → ignore")
    fun recoveryWrongListenerIgnored() {
        val t = trigger()

        val wrong = ABasicEvent().derivedOf(t)
        eventStore.persist(wrong)

        val delete = ADeleteEvent(wrong.eventId).derivedOf(wrong)

        val outcome = listener.onEvent(delete, listOf(t, wrong, delete))

        assertNull(outcome)
    }

    // ---------------------------------------------------------
    // RECOVERY — reorder → identity matching works
    // ---------------------------------------------------------

    @Test
    @DisplayName("Recovery → reorder → identity matching still correct")
    fun recoveryReorderIdentityMatching() {
        val t = trigger()

        val created = listener.onEvent(t, listOf(t)) as CreatedEvent

        // Reorder taskIds
        val reordered = created.taskIds.toList().reversed().toSet()
        val reorderedEvent = CreatedEvent(reordered).derivedOf(t)
        eventStore.persist(reorderedEvent)

        val delete = ADeleteEvent(reorderedEvent.eventId).derivedOf(reorderedEvent)

        listener.onEvent(delete, listOf(t, reorderedEvent, delete))

        // All tasks must still match identity
        reordered.forEach {
            val stored = taskStore.findByTaskId(it.taskId)?.toTask()
            assertNotNull(stored)
            assertEquals(it.identity, listener.onGetTaskIdentity(stored!!))
        }
    }
}
