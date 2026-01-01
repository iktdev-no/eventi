package no.iktdev.eventi

abstract class ListenerRegistryImplementation<T> {
    private val listeners = mutableListOf<T>()

    fun registerListener(listener: T) {
        listeners.add(listener)
    }

    open fun getListeners(): List<T> = listeners.toList()
}