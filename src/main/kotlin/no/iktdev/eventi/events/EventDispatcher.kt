package no.iktdev.eventi.events

import mu.KotlinLogging
import no.iktdev.eventi.models.DeleteEvent
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.SignalEvent
import no.iktdev.eventi.registry.EventListenerRegistry
import no.iktdev.eventi.stores.EventStore
import java.util.UUID

open class EventDispatcher(val eventStore: EventStore) {

    private val log = KotlinLogging.logger {}

    open fun dispatch(referenceId: UUID, events: List<Event>) {
        val deletedEventIds = events.filterIsInstance<DeleteEvent>().map { it.deletedEventId }

        val candidates = events
            .validEvents(deletedEventIds)
            .replayCandidates()

        val effectiveHistory = events
            .validEvents(deletedEventIds)
            .filterNot { it is DeleteEvent }

        EventListenerRegistry.getListeners().forEach { listener ->
            for (candidate in candidates) {
                try {
                    val result = listener.onEvent(candidate, effectiveHistory)

                    if (result != null) {
                        validateReferenceId(result, listener)
                        validateDerivation(result, candidate, effectiveHistory, listener)
                        eventStore.persist(result)
                    }

                } catch (e: SoftDispatchException.UnqualifiedEntryEventException) {
                    log.debug("Soft-dispatch skip (unqualified entry): ${e.message}")

                } catch (e: SoftDispatchException.SkipListenerException) {
                    log.debug("Soft-dispatch skip: ${e.message}")

                } catch (e: SoftDispatchException) {
                    log.warn(
                        "Soft-dispatch in ${listener::class.simpleName} " +
                                "for event ${e.eventType?.simpleName}: ${e.message}"
                    )
                }
            }
        }
    }

    fun List<Event>.replayCandidates(): List<Event> {
        val derivedFromIds = this.mapNotNull { it.metadata.derivedFromId }.flatten().toSet()

        return this
            .filterNot { it is SignalEvent }
            .filter { it.eventId !in derivedFromIds }
    }

    fun List<Event>.validEvents(deletedEventIds: List<UUID>): List<Event> {
        return this.filter {it.eventId !in deletedEventIds }
    }

    /**
     * Validates that referenceId (a lateinit) is initialized.
     * If not, throws a clear and actionable exception.
     */
    private fun validateReferenceId(event: Event, listener: Any) {
        try {
            // Accessing lateinit will throw if not initialized
            event.referenceId
        } catch (e: UninitializedPropertyAccessException) {
            throw IllegalStateException(
                "Listener ${listener::class.simpleName} attempted to persist " +
                        "${event::class.simpleName} (${event.eventId}) without initializing referenceId"
            )
        }
    }

    private fun validateDerivation(
        producedEvent: Event,
        sourceEvent: Event,
        history: List<Event>,
        listener: EventListener
    ) {
        val parents = producedEvent.metadata.derivedFromId ?: emptyList()
        if (parents.isEmpty()) return

        // Lytteren har eksplisitt opt-in → da er alt i historikken lov
        if (listener.allowDerivativeOnHistoricalEvent()) {
            // Men vi kan fortsatt nekte Signal/Delete som parent hvis du vil
            val historyById = history.associateBy { it.eventId }
            parents.forEach { parentId ->
                val parent = historyById[parentId]
                if (parent is SignalEvent || parent is DeleteEvent) {
                    throw HardDispatchException.IllegalDerivationException(
                        parentId = parentId,
                        producedEvent = producedEvent,
                        listener = listener
                    )
                }
            }
            return
        }

        // Default: kun lov å derivere fra sourceEvent
        parents.forEach { parentId ->
            if (parentId != sourceEvent.eventId) {
                throw HardDispatchException.IllegalDerivationException(
                    parentId = parentId,
                    producedEvent = producedEvent,
                    listener = listener
                )
            }
        }
    }



}
