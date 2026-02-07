package no.iktdev.eventi.events

import mu.KotlinLogging
import no.iktdev.eventi.models.DeleteEvent
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.SignalEvent
import no.iktdev.eventi.stores.EventStore
import java.util.UUID

open class EventDispatcher(val eventStore: EventStore) {

    private val log = KotlinLogging.logger {}

    open fun dispatch(referenceId: UUID, events: List<Event>) {
        val derivedFromIds = events.mapNotNull { it.metadata.derivedFromId }.flatten().toSet()
        val deletedEventIds = events.filterIsInstance<DeleteEvent>().map { it.deletedEventId }
        val candidates = events
            .filterNot { it is SignalEvent }
            .filter { it.eventId !in derivedFromIds }
            .filter { it.eventId !in deletedEventIds }

        val effectiveHistory = events
            .filter { it.eventId !in deletedEventIds }        // fjern slettede events
            .filterNot { it is DeleteEvent }                  // fjern selve delete-eventet


        EventListenerRegistry.getListeners().forEach { listener ->
            for (candidate in candidates) {
                try {
                    val result = listener.onEvent(candidate, effectiveHistory)
                    if (result != null) {
                        eventStore.persist(result)
                    }
                } catch (e: SoftDispatchException) {
                    log.warn( "Soft-dispatch in ${listener::class.simpleName} " + "for event ${e.eventType?.simpleName ?: "Unknown"}: ${e.message}" )
                }
            }
        }
    }
}