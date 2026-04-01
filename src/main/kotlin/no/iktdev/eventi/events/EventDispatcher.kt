package no.iktdev.eventi.events

import kotlinx.coroutines.Job
import mu.KotlinLogging
import no.iktdev.eventi.models.DeleteEvent
import no.iktdev.eventi.models.DispatchResult
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.SignalEvent
import no.iktdev.eventi.registry.EventListenerRegistry
import no.iktdev.eventi.stores.EventStore
import java.util.UUID

open class EventDispatcher(val eventStore: EventStore) {

    private val log = KotlinLogging.logger {}

    open fun dispatch(referenceId: UUID, history: List<Event>, newEvents: List<Event>) {
        val deletedEventIds = history.filterIsInstance<DeleteEvent>().map { it.deletedEventId }

        val candidates = newEvents
            .validEvents(deletedEventIds)
            .replayCandidates()

        val effectiveHistory = history
            .validEvents(deletedEventIds)
            .filterNot { it is DeleteEvent }

        EventListenerRegistry.getListeners().forEach { listener ->
            for (candidate in candidates) {
                //log.debug("Evaluating candidate: ${candidate::class.simpleName} for listener ${listener::class.simpleName}")
                try {
                    val result = listener.onEvent(candidate, effectiveHistory)

                    if (result != null) {
                        validateReferenceId(result, listener)
                        validateDerivation(result, candidate, effectiveHistory, listener)
                        eventStore.persist(result)
                        onDispatched(candidate, listener, DispatchResult.Accepted)
                    } else {
                        onDispatched(candidate, listener, DispatchResult.NoResult)
                    }

                } catch (e: SoftDispatchException.UnqualifiedEntryEventException) {
                    onDispatched(candidate, listener, DispatchResult.Rejected, e.message)
                } catch (e: SoftDispatchException.SkipListenerException) {
                    onDispatched(candidate, listener, DispatchResult.Skipped, e.message)
                } catch (e: SoftDispatchException) {
                    onDispatched(candidate, listener, DispatchResult.Error, e.message)
                }
            }
        }
    }

    private fun UUID.short(): String = this.toString().substring(0, 8)
    open fun onDispatched(
        event: Event,
        listener: EventListener,
        result: DispatchResult,
        message: String? = null
    ) {
        val listenerName = listener::class.java.simpleName
        val ref = event.referenceId.short()
        val evtId = event.eventId.short()
        val evtName = event::class.java.simpleName

        val msg = buildString {
            append("[$ref] $listenerName → $evtId $evtName")
            if (!message.isNullOrBlank()) {
                append("\n  ↳ $message")
            }
        }


        when (result) {
            DispatchResult.Accepted,
            DispatchResult.NoResult,
            DispatchResult.Skipped ->
                log.debug { msg }

            DispatchResult.Rejected ->
                log.warn { msg }

            DispatchResult.Error ->
                log.error { msg }
        }
    }

    fun List<Event>.replayCandidates(): List<Event> {
        val derivedFromIds = this
            .filterNot { it is SignalEvent }      // ignorer signaler som “forbrukere”
            .mapNotNull { it.metadata.derivedFromId }
            .flatten()
            .toSet()

        return this
            .filterNot { it is SignalEvent }      // signaler er fortsatt ikke kandidater
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
