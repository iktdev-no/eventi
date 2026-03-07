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
    fun onEvent(event: Event, history: List<Event>): Event?
}