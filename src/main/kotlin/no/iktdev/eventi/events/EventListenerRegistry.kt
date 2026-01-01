package no.iktdev.eventi.events

import no.iktdev.eventi.ListenerOrder
import no.iktdev.eventi.ListenerRegistryImplementation

object EventListenerRegistry : ListenerRegistryImplementation<EventListener>() {
    override fun getListeners(): List<EventListener> {
        return super.getListeners()
            .sortedBy { it::class.java.getAnnotation(ListenerOrder::class.java)?.value ?: Int.MAX_VALUE }
    }
}