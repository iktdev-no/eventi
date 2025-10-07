package no.iktdev.eventi.events

object EventListenerRegistry {
    private val listeners = mutableListOf<EventListener>()

    fun registerListener(listener: EventListener) {
        listeners.add(listener)
    }

    fun getListeners(): List<EventListener> = listeners.toList()
}