package no.iktdev.eventi.events

import no.iktdev.eventi.models.Event

abstract class EventListener: EventListenerImplementation {
    init {
        EventListenerRegistry.registerListener(this)
    }

}

interface EventListenerImplementation {
    fun onEvent(event: Event, history: List<Event>): Event?
}