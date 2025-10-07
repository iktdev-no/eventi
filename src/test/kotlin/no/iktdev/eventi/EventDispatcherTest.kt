package no.iktdev.eventi

import no.iktdev.eventi.ZDS.toEvent
import no.iktdev.eventi.ZDS.toPersisted
import no.iktdev.eventi.events.EventDispatcher
import no.iktdev.eventi.events.EventListener
import no.iktdev.eventi.events.EventListenerRegistry
import no.iktdev.eventi.events.EventTypeRegistry
import no.iktdev.eventi.models.DeleteEvent
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.testUtil.wipe
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.UUID

class EventDispatcherTest: TestBase() {
    val dispatcher = EventDispatcher(store)

    class DerivedEvent(): Event()
    class TriggerEvent(): Event() {
        fun usingReferenceId(id: UUID) = apply { referenceId = id }
    }
    class OtherEvent(): Event()


    @BeforeEach
    fun setup() {
        EventTypeRegistry.wipe()
        EventListenerRegistry.wipe()
        // Verifiser at det er tomt

        EventTypeRegistry.register(listOf(
            DerivedEvent::class.java,
            TriggerEvent::class.java,
            OtherEvent::class.java
        ))
    }


    @Test
    fun `should produce one event and stop`() {
        val listener = ProducingListener()

        val trigger = TriggerEvent()
        dispatcher.dispatch(trigger.referenceId, listOf(trigger))

        val produced = store.all().firstOrNull()
        assertNotNull(produced)

        val event = produced!!.toEvent()
        assertEquals(trigger.eventId, event.metadata.derivedFromId)
        assertTrue(event is DerivedEvent)
    }

    @Test
    fun `should skip already derived events`() {
        val listener = ProducingListener()

        val trigger = TriggerEvent()
        val derived = DerivedEvent().derivedOf(trigger).toPersisted(1L, LocalDateTime.now())
        store.save(derived.toEvent()) // simulate prior production

        dispatcher.dispatch(trigger.referenceId, listOf(trigger, derived.toEvent()))

        assertEquals(1, store.all().size) // no new event produced
    }

    @Test
    fun `should pass full context to listener`() {
        val listener = ContextCapturingListener()

        val e1 = TriggerEvent()
        val e2 = OtherEvent()
        dispatcher.dispatch(e1.referenceId, listOf(e1, e2))

        assertEquals(2, listener.context.size)
    }

    @Test
    fun `should behave deterministically across replays`() {
        val listener = ProducingListener()

        val trigger = TriggerEvent()
        dispatcher.dispatch(trigger.referenceId, listOf(trigger))
        val replayContext = listOf(trigger) + store.all().map { it.toEvent() }

        dispatcher.dispatch(trigger.referenceId, replayContext)

        assertEquals(1, store.all().size) // no duplicate
    }

    @Test
    fun `should not deliver deleted events as candidates`() {
        val dispatcher = EventDispatcher(store)
        val received = mutableListOf<Event>()
        object : EventListener() {
            override fun onEvent(event: Event, history: List<Event>): Event? {
                received += event
                return null
            }
        }
        // Original hendelse
        val original = TriggerEvent()

        // Slettehendelse som peker på original
        val deleted = object : DeleteEvent() {
            override var deletedEventId = original.eventId
        }

        // Dispatch med begge hendelser
        dispatcher.dispatch(original.referenceId, listOf(original, deleted))

        // Verifiser at original ikke ble levert som kandidat
        assertFalse(received.contains(original)) {
            "Original hendelse ble levert til lytteren selv om den var slettet"
        }

        // Verifiser at slett-hendelsen finnes i konteksten
        assertTrue(received.any() { it is DeleteEvent }) {
            "DeleteEvent skal leveres som kandidat"
        }
    }

    @Test
    fun `should deliver DeleteEvent to listeners that react to it`() {
        val received = mutableListOf<Event>()
        val listener = object : EventListener() {
            override fun onEvent(event: Event, context: List<Event>): Event? {
                if (event is DeleteEvent) received += event
                return null
            }
        }

        val deleted = object : DeleteEvent() {
            override var deletedEventId = UUID.randomUUID()
        }
        dispatcher.dispatch(deleted.referenceId, listOf(deleted))

        assertTrue(received.contains(deleted))
    }



    // --- Test helpers ---

    class ProducingListener : EventListener() {
        override fun onEvent(event: Event, context: List<Event>): Event? {
            return if (event is TriggerEvent) DerivedEvent().derivedOf(event) else null
        }
    }

    class ContextCapturingListener : EventListener() {
        var context: List<Event> = emptyList()
        override fun onEvent(event: Event, context: List<Event>): Event? {
            this.context = context
            return null
        }
    }

}