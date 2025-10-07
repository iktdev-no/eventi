package no.iktdev.eventi.events

import no.iktdev.eventi.models.DeleteEvent
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.stores.EventStore
import java.util.UUID

class EventDispatcher(val eventStore: EventStore) {

    fun dispatch(referenceId: UUID, events: List<Event>) {
        val derivedFromIds = events.mapNotNull { it.metadata.derivedFromId }.toSet()
        val deletedEventIds = events.filterIsInstance<DeleteEvent>().map { it.deletedEventId }
        val candidates = events
            .filter { it.eventId !in derivedFromIds }
            .filter { it.eventId !in deletedEventIds }

        EventListenerRegistry.getListeners().forEach { listener ->
            for (candidate in candidates) {
                val result = listener.onEvent(candidate, events)
                if (result != null) {

                    eventStore.save(result)
                    return
                }
            }
        }
    }
}