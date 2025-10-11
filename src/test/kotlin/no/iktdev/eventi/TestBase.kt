package no.iktdev.eventi

import no.iktdev.eventi.events.EchoEvent
import no.iktdev.eventi.events.EventTypeRegistry
import no.iktdev.eventi.events.StartEvent

open class TestBase {

    val eventStore = InMemoryEventStore()
    val taskStore = InMemoryTaskStore()

    fun registerEventTypes() {
        EventTypeRegistry.register(listOf(StartEvent::class.java, EchoEvent::class.java))
    }

    init {
        registerEventTypes()
    }

}