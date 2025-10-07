package no.iktdev.eventi.events

import no.iktdev.eventi.models.Event

object EventTypeRegistry {
    private val types = mutableMapOf<String, Class<out Event>>()

    fun register(clazz: Class<out Event>) {
        types[clazz.simpleName] = clazz
    }
    fun register(clazzes: List<Class<out Event>>) {
        clazzes.forEach { clazz ->
            types[clazz.simpleName] = clazz
        }
    }

    fun resolve(name: String): Class<out Event>? = types[name]

    fun all(): Map<String, Class<out Event>> = types.toMap()
}


abstract class EventTypeRegistration {
    init {
        definedTypes().forEach { clazz ->
            EventTypeRegistry.register(clazz)
        }
    }

    protected abstract fun definedTypes(): List<Class<out Event>>
}
