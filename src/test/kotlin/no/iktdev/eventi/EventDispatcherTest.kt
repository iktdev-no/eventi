package no.iktdev.eventi

import no.iktdev.eventi.ZDS.toEvent
import no.iktdev.eventi.ZDS.toPersisted
import no.iktdev.eventi.events.EventDispatcher
import no.iktdev.eventi.events.EventListener
import no.iktdev.eventi.events.EventListenerRegistry
import no.iktdev.eventi.events.EventTypeRegistry
import no.iktdev.eventi.models.DeleteEvent
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.SignalEvent
import no.iktdev.eventi.testUtil.wipe
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.util.UUID

@DisplayName(
    """
EventDispatcher
Når hendelser dispatches til lyttere
Hvis hendelsene inneholder avledede, slettede eller nye events
Så skal dispatcheren håndtere filtrering, replays og historikk korrekt
"""
)
class EventDispatcherTest : TestBase() {

    val dispatcher = EventDispatcher(eventStore)

    class DerivedEvent : Event()
    class TriggerEvent : Event()
    class OtherEvent : Event()
    class DummyEvent : Event() {
    }

    @BeforeEach
    fun setup() {
        EventTypeRegistry.wipe()
        EventListenerRegistry.wipe()

        EventTypeRegistry.register(
            listOf(
                DerivedEvent::class.java,
                TriggerEvent::class.java,
                OtherEvent::class.java,
                DummyEvent::class.java
            )
        )
    }

    @Test
    @DisplayName(
        """
    Når en TriggerEvent dispatches
    Hvis en lytter produserer én DerivedEvent
    Så skal kun én ny event produseres og prosessen stoppe
    """
    )
    fun shouldProduceOneEventAndStop() {
        ProducingListener()

        val trigger = TriggerEvent()
        dispatcher.dispatch(trigger.referenceId, listOf(trigger))

        val produced = eventStore.all().firstOrNull()
        assertNotNull(produced)

        val event = produced!!.toEvent()
        assertThat(event!!.metadata.derivedFromId).hasSize(1)
        assertThat(event.metadata.derivedFromId).contains(trigger.eventId)
        assertTrue(event is DerivedEvent)
    }

    @Test
    @DisplayName(
        """
    Når en event allerede har avledet en DerivedEvent
    Hvis dispatcheren replays historikken
    Så skal ikke DerivedEvent produseres på nytt
    """
    )
    fun shouldSkipAlreadyDerivedEvents() {
        ProducingListener()

        val trigger = TriggerEvent()
        val derived = DerivedEvent().derivedOf(trigger).toPersisted(1L, MyTime.utcNow())

        eventStore.persist(derived!!.toEvent()!!) // simulate prior production

        dispatcher.dispatch(trigger.referenceId, listOf(trigger, derived.toEvent()!!))

        assertEquals(1, eventStore.all().size)
    }

    @Test
    @DisplayName(
        """
    Når flere events dispatches
    Hvis en lytter mottar en event
    Så skal hele historikken leveres i context
    """
    )
    fun shouldPassFullContextToListener() {
        val listener = ContextCapturingListener()

        val e1 = TriggerEvent()
        val e2 = OtherEvent()
        dispatcher.dispatch(e1.referenceId, listOf(e1, e2))

        assertEquals(2, listener.context.size)
    }

    @Test
    @DisplayName(
        """
    Når en replay skjer
    Hvis en event allerede har produsert en DerivedEvent
    Så skal ikke DerivedEvent produseres på nytt
    """
    )
    fun shouldBehaveDeterministicallyAcrossReplays() {
        ProducingListener()

        val trigger = TriggerEvent()
        dispatcher.dispatch(trigger.referenceId, listOf(trigger))
        val replayContext = listOf(trigger) + eventStore.all().mapNotNull { it.toEvent() }

        dispatcher.dispatch(trigger.referenceId, replayContext)

        assertEquals(1, eventStore.all().size)
    }

    @Test
    @DisplayName(
        """
    Når en DeleteEvent peker på en tidligere event
    Hvis dispatcheren filtrerer kandidater
    Så skal slettede events ikke leveres som kandidater
    """
    )
    fun shouldNotDeliverDeletedEventsAsCandidates() {
        val dispatcher = EventDispatcher(eventStore)
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
        val deleted = object : DeleteEvent(original.eventId) {
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
    @DisplayName(
        """
    Når en DeleteEvent dispatches alene
    Hvis en lytter reagerer på DeleteEvent
    Så skal DeleteEvent leveres som kandidat
    """
    )
    fun shouldDeliverDeleteEventToListenersThatReactToIt() {
        val received = mutableListOf<Event>()
        object : EventListener() {
            override fun onEvent(event: Event, history: List<Event>): Event? {
                if (event is DeleteEvent) received += event
                return null
            }
        }

        val deleted = object : DeleteEvent(UUID.randomUUID()) {}
        dispatcher.dispatch(deleted.referenceId, listOf(deleted))

        assertTrue(received.contains(deleted))
    }

    @Test
    @DisplayName(
        """
    Når en event har avledet en ny event
    Hvis dispatcheren replays historikken
    Så skal ikke original-eventen leveres som kandidat igjen
    """
    )
    fun shouldNotRedeliverEventsThatHaveProducedDerivedEvents() {
        ProducingListener()

        val trigger = TriggerEvent()
        // Første dispatch: trigger produserer en DerivedEvent
        dispatcher.dispatch(trigger.referenceId, listOf(trigger))

        val produced = eventStore.all().mapNotNull { it.toEvent() }
        assertEquals(1, produced.size)
        val derived = produced.first()
        assertTrue(derived is DerivedEvent)

        // Replay: nå har vi både trigger og derived i konteksten
        val replayContext = listOf(trigger, derived)
        dispatcher.dispatch(trigger.referenceId, replayContext)

        // Verifiser at ingen nye events ble produsert
        assertEquals(1, eventStore.all().size) {
            "TriggerEvent skal ikke leveres som kandidat igjen når den allerede har avledet en DerivedEvent"
        }
    }

    @Test
    @DisplayName(
        """
    Når en DeleteEvent slettet en tidligere event
    Hvis dispatcheren bygger historikk
    Så skal slettede events ikke være med i history
    """
    )
    fun historyShouldExcludeDeletedEvents() {
        val dispatcher = EventDispatcher(eventStore)

        val original = TriggerEvent()
        val deleted = object : DeleteEvent(original.eventId) {}

        var receivedHistory: List<Event> = emptyList()

        object : EventListener() {
            override fun onEvent(event: Event, history: List<Event>): Event? {
                receivedHistory = history
                return null
            }
        }

        dispatcher.dispatch(original.referenceId, listOf(original, deleted))

        assertFalse(receivedHistory.contains(original))
        assertFalse(receivedHistory.contains(deleted))
    }

    @Test
    @DisplayName(
        """
    Når en DeleteEvent slettet en event
    Hvis andre events fortsatt er gyldige
    Så skal history kun inneholde de ikke-slettede events
    """
    )
    fun historyShouldKeepNonDeletedEvents() {
        val dispatcher = EventDispatcher(eventStore)

        val e1 = TriggerEvent()
        val e2 = OtherEvent()
        val deleted = object : DeleteEvent(e1.eventId) {}

        var receivedHistory: List<Event> = emptyList()

        object : EventListener() {
            override fun onEvent(event: Event, history: List<Event>): Event? {
                receivedHistory = history
                return null
            }
        }

        dispatcher.dispatch(e1.referenceId, listOf(e1, e2, deleted))

        assertTrue(receivedHistory.contains(e2))
        assertFalse(receivedHistory.contains(e1))
        assertFalse(receivedHistory.contains(deleted))
    }

    @Test
    @DisplayName(
        """
    Når en DeleteEvent er kandidat
    Hvis historikken kun inneholder slettede events
    Så skal history være tom
    """
    )
    fun deleteEventShouldBeDeliveredButHistoryEmpty() {
        val dispatcher = EventDispatcher(eventStore)

        val original = TriggerEvent()
        val deleted = object : DeleteEvent(original.eventId) {}

        var receivedEvent: Event? = null
        var receivedHistory: List<Event> = emptyList()

        object : EventListener() {
            override fun onEvent(event: Event, history: List<Event>): Event? {
                receivedEvent = event
                receivedHistory = history
                return null
            }
        }

        dispatcher.dispatch(original.referenceId, listOf(original, deleted))

        assertTrue(receivedEvent is DeleteEvent)
        assertTrue(receivedHistory.isEmpty())
    }

    @Test
    @DisplayName(
    """
    Når en SignalEvent dispatches
    Hvis SignalEvent ikke skal være kandidat
    Så skal den ikke leveres til lyttere, men fortsatt være i historikken
    """
    )
    fun shouldNotDeliverSignalEventAsCandidate() {
        // Arrange
        class TestSignalEvent : SignalEvent()
        EventTypeRegistry.register(listOf(TestSignalEvent::class.java,))

        val received = mutableListOf<Event>()
        var finalHistory: List<Event>? = null
        object : EventListener() {
            override fun onEvent(event: Event, history: List<Event>): Event? {
                received += event
                finalHistory = history
                return null
            }
        }

        val refId = UUID.randomUUID()
        val trigger = TriggerEvent().usingReferenceId(refId)
        val signal = TestSignalEvent().usingReferenceId(refId)

        // Act
        dispatcher.dispatch(trigger.referenceId, listOf(trigger, signal))

        // Assert
        // 1) TriggerEvent skal leveres
        assertTrue(received.any { it is TriggerEvent }) {
            "TriggerEvent skal leveres som kandidat"
        }

        // 2) SignalEvent skal IKKE leveres
        assertFalse(received.any { it is TestSignalEvent }) {
            "SignalEvent skal ikke leveres som kandidat"
        }

        assertNotNull(finalHistory)
        assertTrue(finalHistory!!.any { it is TestSignalEvent }) {
            "SignalEvent skal være i historikken selv om den ikke er kandidat"
        }
    }


    // --- Test helpers ---

    class ProducingListener : EventListener() {
        override fun onEvent(event: Event, history: List<Event>): Event? {
            return if (event is TriggerEvent) DerivedEvent().derivedOf(event) else null
        }
    }

    class ContextCapturingListener : EventListener() {
        var context: List<Event> = emptyList()
        override fun onEvent(event: Event, history: List<Event>): Event? {
            this.context = history
            return null
        }
    }

}