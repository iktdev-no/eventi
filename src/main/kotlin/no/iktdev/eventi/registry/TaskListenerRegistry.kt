package no.iktdev.eventi.registry

import no.iktdev.eventi.ListenerOrder
import no.iktdev.eventi.ListenerRegistryImplementation
import no.iktdev.eventi.tasks.TaskListener

object TaskListenerRegistry: ListenerRegistryImplementation<TaskListener>() {
    override fun getListeners(): List<TaskListener> {
        return super.getListeners()
            .sortedBy { it::class.java.getAnnotation(ListenerOrder::class.java)?.value ?: Int.MAX_VALUE }
    }
}