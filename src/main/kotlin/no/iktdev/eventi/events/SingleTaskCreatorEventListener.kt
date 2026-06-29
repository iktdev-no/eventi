package no.iktdev.eventi.events

import mu.KotlinLogging
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.SingleTaskCratedEvent
import no.iktdev.eventi.models.Task
import no.iktdev.eventi.models.store.TaskStatus
import no.iktdev.eventi.serialization.ZDS.toTask
import no.iktdev.eventi.stores.EventStore
import no.iktdev.eventi.stores.TaskStore

abstract class SingleTaskCreatorEventListener(eventStore: EventStore, taskStore: TaskStore): TaskCreatorEventListener(eventStore, taskStore),
    SingleTaskCreatorEventListenerImplementation{
    private val log = KotlinLogging.logger {}

    /**
     * @param event is the created event, the one that is to be produced from the implementor of this listener, in this instance its SingleTaskCratedEvent
     * @param history the effective history that we received from the dispatcher
     * @return null if task is reset, task if it is to be re-created
     */
    fun getTaskOrNewFromChain(event: Event, history: List<Event>): Task? {
        val originalTaskId = event as? SingleTaskCratedEvent ?: return null
        val foundTask = originalTaskId.taskId.let { taskStore.findByTaskId(it)?.toTask() }
        if (foundTask != null) {
            if (foundTask.state.consumed || foundTask.state.status == TaskStatus.Failed) {
                taskStore.resetTaskById(foundTask.taskId)
            }
            return null
        }
        val triggerEvent: List<Event> =
            event.metadata.derivedFromId?.mapNotNull { did -> history.find { it.eventId == did } }.orEmpty()
        val tasks = triggerEvent.mapNotNull { e ->
            try {
                onCreateTask(e, history)?.apply {
                    originalTaskId.taskId.let { id ->  reUseTaskId(id) }
                }?.derivedOf(event)
            } catch (e: Exception) {
                null
            }
        }
        val useTask = tasks.firstOrNull()
        if (useTask == null) {
            log.error("Could not create a new task for event ${event::class.simpleName}")
        }
        return useTask

    }

    final override fun onEvent(event: Event, history: List<Event>): Event? {
        if (isEventOfMyCreation(event)) {
            return null
        }
        val deletedEvent = getDeletedResultEvent(event)

        // Recovery path
        if (deletedEvent != null) {

            val taskCreatorEvent =
                getTaskCreatorEvent(deletedEvent.metadata.derivedFromId ?: emptySet(), history)
                    ?: return null

            // Dette er den riktige sjekken
            if (!isEventOfMyCreation(taskCreatorEvent)) {
                return null
            }

            val task = getTaskOrNewFromChain(taskCreatorEvent, history)
            task?.let { t ->
                val persisted = taskStore.persist(t)
                if (!persisted) {
                    log.error("Could not persist recovered task ${t.taskId} for creator event ${taskCreatorEvent::class.simpleName}")
                }
            }

            return null
        }

        // Normal path
        val task = onCreateTask(event, history) ?: return null
        val createdTaskEvent = onTaskCreated(event, history, task)
            .derivedOf(event)
        val taskToPersist = task.derivedOf(createdTaskEvent)

        val persisted = taskStore.persist(taskToPersist)
        if (!persisted) {
            throw IllegalStateException("Could not create a new task for event ${event::class.simpleName}")
        }
        return createdTaskEvent
    }

}

interface SingleTaskCreatorEventListenerImplementation {
    fun onCreateTask(event: Event, history: List<Event>): Task?
    fun onTaskCreated(event: Event, history: List<Event>, task: Task): SingleTaskCratedEvent
}