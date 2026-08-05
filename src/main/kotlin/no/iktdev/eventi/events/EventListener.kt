package no.iktdev.eventi.events

import mu.KotlinLogging
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.registry.EventListenerRegistry

abstract class EventListener: EventListenerImplementation {
    private val log = KotlinLogging.logger {}

    init {
        EventListenerRegistry.registerListener(this)

        if (allowDerivativeOnHistoricalEvent()) {
            log.warn(
                "Listener ${this::class.simpleName} has enabled historical derivation. " +
                        "This bypasses strict derivation validation and should be used with caution."
            )
        }
    }


    /**
     * Allows this listener to accept events that are derived from events
     * not present in the current dispatch input.
     *
     * WARNING: Enabling this breaks strict derivation guarantees and should
     * only be used for advanced listeners such as replay, migration or
     * compensation handlers.
     */
    open fun allowDerivativeOnHistoricalEvent(): Boolean = false

}

interface EventListenerImplementation {

    /**
     * Called when a new event occurs in the system.
     *
     * @param event The specific event being evaluated/processed right now.
     * @param history The effective, valid history of events for this reference (includes both previous history and the current [event]) with the exclusion of events marked as deleted.
     * @return A new [Event] to be produced as a result of this event, or `null` if no event should be emitted.
     */
    fun onEvent(event: Event, history: List<Event>): Event?
}