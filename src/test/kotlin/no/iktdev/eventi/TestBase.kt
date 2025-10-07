package no.iktdev.eventi

import no.iktdev.eventi.events.EchoEvent
import no.iktdev.eventi.events.EventTypeRegistration
import no.iktdev.eventi.events.StartEvent
import no.iktdev.eventi.models.Event

open class TestBase {

    val store = InMemoryEventStore()

    class DefaultTestEvents() : EventTypeRegistration() {
        override fun definedTypes(): List<Class<out Event>> {
            return listOf(
                EchoEvent::class.java,
                StartEvent::class.java
            )
        }
    }

    init {
        DefaultTestEvents()
    }

}