package no.iktdev.eventi.registry

import no.iktdev.eventi.ListenerOrder
import no.iktdev.eventi.ListenerRegistryImplementation
import no.iktdev.eventi.events.EventListener

object EventListenerRegistry : ListenerRegistryImplementation<EventListener>() {
    override fun getListeners(): List<EventListener> {
        return super.getListeners()
            .sortedBy { it::class.java.getAnnotation(ListenerOrder::class.java)?.value ?: Int.MAX_VALUE }
    }
}