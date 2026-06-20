package no.iktdev.eventi.events

import mu.KotlinLogging
import no.iktdev.eventi.models.DeleteEvent
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.MultiTaskCreatedEvent
import no.iktdev.eventi.models.Task
import no.iktdev.eventi.serialization.ZDS.toTask
import no.iktdev.eventi.stores.EventStore
import no.iktdev.eventi.stores.TaskStore
import java.util.UUID

abstract class MultiTaskCreatorEventListener(eventStore: EventStore, taskStore: TaskStore) :
    TaskCreatorEventListener(eventStore, taskStore),
    MultiTaskCreatorEventListenerImplementation {
    private val log = KotlinLogging.logger {}

    private fun resetTasksAndGetConsumedIds(ids: List<UUID>): List<UUID> {
        val resetStatus = taskStore.resetTasksById(ids)
        return if (resetStatus.success) ids else
            throw TaskResetInRecoveryException("Failed to reset one or more tasks!")
    }


    /**
     * @param event is the MultiTaskCreatedEvent
     * @return List<MultiTaskIdentity> of the entries that were not found and has to be recreated.
     */
    fun getCreatedAndConsumedTasks(event: Event): List<Task>? {
        val createdEvent = event as? MultiTaskCreatedEvent ?: return null
        val originalTaskIds = createdEvent.taskIds.toList()
        val tasks = originalTaskIds.mapNotNull { it ->
            taskStore.findByTaskId(it.taskId)?.toTask()
        }
        if (tasks.any { !it.state.consumed }) {
            throw UnableToPerformRecoveryIllegalTaskStateException()
        }
        return tasks
    }

    fun performUnrecoverableProcedure(triggerEvent: Event, entryEvent: Event, tasks: List<Task>) {
        // Nuke tasks (must succeed)
        val allDeleted = tasks.map { it.taskId }.all { id ->
            taskStore.deleteTasksById(id).success
        }

        if (!allDeleted) {
            throw UnableToPerformRecoveryIllegalStateException(
                "Failed to delete all tasks for ${entryEvent.eventId}. Recovery cannot continue."
            )
        }

        // Lag DeleteEvent for creation event
        val deleteCreationEvent = DeleteEvent(deletedEventId = entryEvent.eventId)
            .derivedOf(triggerEvent)

        eventStore.persist(deleteCreationEvent)
    }

    private fun validateAndCleanupConsistency(createdEvent: MultiTaskCreatedEvent) {
        val requiredIds = createdEvent.taskIds.map { it.taskId }.toSet()

        // Finn alle tasks som deler referenceId med createdEvent
        val allTasksForReference = taskStore.findByReferenceId(createdEvent.referenceId)

        // Slett alle som ikke finnes i CreatedEvent
        allTasksForReference
            .filter { it.taskId !in requiredIds }
            .forEach { rogue ->
                taskStore.deleteTasksById(rogue.taskId)
            }
    }


    fun onHandleRecoverySequence(
        incomingEvent: Event,
        deletedEvent: Event,
        history: List<Event>
    ): Event? {

        // 1. Finn MultiTaskCreatedEvent
        val createdEvent = getTaskCreatorEvent(
            deletedEvent.metadata.derivedFromId ?: emptySet(),
            history
        ) as? MultiTaskCreatedEvent ?: return null

        if (!isEventOfMyCreation(createdEvent)) return null

        validateAndCleanupConsistency(createdEvent)

        val presentTasks = getCreatedAndConsumedTasks(createdEvent) ?: return null
        val presentByIdentity = presentTasks.associateBy { onGetTaskIdentity(it) }

        val triggerEvent = getParentToCreationEvent(createdEvent, history)
            ?: throw MissingTriggerEventException(
                "Could not find trigger event for MultiTaskCreatedEvent ${createdEvent.eventId}"
            )

        val requiredTaskIdentities = createdEvent.taskIds
        val recreated = onCreateTask(triggerEvent, history).map { task ->
            val stableId = onGetTaskIdentity(task)
            val existing = requiredTaskIdentities.find { it.identity == stableId }
            if (existing != null) {
                task.reUseTaskId(existing.taskId)
            }

            task.derivedOf(createdEvent)
            task
        }

        val requiredIdentities = createdEvent.taskIds.map { it.identity }.toSet()

        val existingIdentities = presentByIdentity.keys.toSet()

        val recreatedIdentities = recreated.map { onGetTaskIdentity(it) }.toSet()

        val mismatch = !sameValues(requiredIdentities, (recreatedIdentities + existingIdentities))


        // 6. Hvis mismatch → nuke + delete creation event
        if (mismatch) {
            log.error { "Recovery mismatch for ${createdEvent.eventId}, nuking tasks and deleting creation event" }
            performUnrecoverableProcedure(triggerEvent, createdEvent, presentTasks)
        } else {
            val resetIds = presentTasks.map { it.taskId }
            resetTasksAndGetConsumedIds(resetIds)

            val persist = recreated.filter { task ->
                task.taskId !in resetIds
            }

            persist.forEach { taskStore.persist(it) }
            return null
        }
        return triggerEvent
    }

    open fun onEjectException(event: Event, history: List<Event>, exception: EjectException): Event {
        log.error { "Do not call super." }
        throw exception
    }

    final override fun onEvent(event: Event, history: List<Event>): Event? {
        val deletedEvent = getDeletedResultEvent(event)

        val effectiveEvent = if (deletedEvent != null) {
            val result = onHandleRecoverySequence(event, deletedEvent, history)
            // null → recovery OK eller ikke recovery → stopp
            // triggerEvent → kjør normal path
            result ?: return null
        } else event

        // Normal path
        val tasks = try {
            onCreateTask(effectiveEvent, history).ifEmpty { return null }
        } catch (e: EjectException) {
            return onEjectException(event, history, e)
        }
        val createdEvent = onTasksCreated(effectiveEvent, history, tasks)

        tasks.onEach { it.derivedOf(createdEvent) }
            .forEach { task ->
                if (!taskStore.persist(task)) {
                    throw IllegalStateException("Could not persist task ${task.taskId}")
                }
            }

        return createdEvent
    }

    internal fun sameValues(a: Collection<String>, b: Collection<String>): Boolean {
        if (a.size != b.size) return false
        return a.toSet() == b.toSet()
    }

}

/**
 * Simple ejection that should be handled.
 */
class EjectException(message: String) : RuntimeException(message)

class UnableToPerformRecoveryIllegalTaskStateException(override val message: String? = null) : Exception()
class UnableToPerformRecoveryIllegalStateException(override val message: String? = null) : Exception()
class TaskResetInRecoveryException(override val message: String?) : Exception()
class IncorrectTaskProvided(message: String? = null) : Exception(message)
class MissingTriggerEventException(message: String) : Exception(message)


interface MultiTaskCreatorEventListenerImplementation {
    fun onCreateTask(event: Event, history: List<Event>): List<Task>
    fun onTasksCreated(event: Event, history: List<Event>, tasks: List<Task>): MultiTaskCreatedEvent

    /**
     * Requires production of a stable identity that can be re-created at any time using the task object itself
     */
    fun onGetTaskIdentity(task: Task): String
}
