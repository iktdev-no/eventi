package no.iktdev.eventi.events

import mu.KotlinLogging
import no.iktdev.eventi.models.DeleteEvent
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.Task
import no.iktdev.eventi.models.store.TaskStatus
import no.iktdev.eventi.registry.EventListenerRegistry
import no.iktdev.eventi.serialization.ZDS.toTask
import no.iktdev.eventi.stores.EventStore
import no.iktdev.eventi.stores.TaskStore
import java.util.UUID

abstract class TaskCreatorEventListener(val eventStore: EventStore, val taskStore: TaskStore) : EventListener(),
    TaskCreatingEventListenerImplementation {
    private val log = KotlinLogging.logger {}


    /**
     * Performs a lookup against the database to find which event we are excluding from the effective history
     * @param event instance of DeleteEvent
     * @return deleted event or null if it is unable to find it
     */
    fun getDeletedResultEvent(event: Event): Event? {
        if (event !is DeleteEvent) return null
        return eventStore.getEventInSequence(event.referenceId, event.deletedEventId)
    }

    /**
     * Find the correct task created event using chain
     * @param derivedId is the derivedIds obtained form the deleted event
     * @param history is the effective history
     */
    fun getTaskCreatorEvent(derivedId: Set<UUID>, history: List<Event>): Event? {
        return derivedId.firstNotNullOfOrNull { did -> history.find { it.eventId == did } }
    }

    fun getParentToCreationEvent(event: Event, history: List<Event>): Event? {
        val ids = event.metadata.derivedFromId ?: return null
        val triggerEvent = history.find { it.eventId in ids } ?: return null
        return triggerEvent
    }

    /**
     * Finds the created task based on uuids
     * When creating the result event we are to also include the taskId in the derivedOf along with the parent of the task
     * @param derivedId Ids fetched from deleted event, or single id obtained from created event
     */
    fun getTaskAssociatedWithEvent(derivedId: Set<UUID>): Task? {
        return derivedId.firstNotNullOfOrNull { taskStore.findByTaskId(it)?.toTask() }
    }



}

interface TaskCreatingEventListenerImplementation {
    fun isEventOfMyCreation(event: Event): Boolean

}