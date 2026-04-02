package no.iktdev.eventi

import no.iktdev.eventi.serialization.ZDS.toEvent
import no.iktdev.eventi.serialization.ZDS.toPersisted
import no.iktdev.eventi.events.EventDispatcher
import no.iktdev.eventi.events.EventListener
import no.iktdev.eventi.events.HardDispatchException
import no.iktdev.eventi.lifecycle.LifecycleStore
import no.iktdev.eventi.registry.EventTypeRegistry
import no.iktdev.eventi.models.DeleteEvent
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.SignalEvent
import no.iktdev.eventi.registry.EventListenerRegistry
import no.iktdev.eventi.registry.TaskTypeRegistry
import no.iktdev.eventi.testUtil.wipe
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.*
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
    val lifecycleStore = LifecycleStore()
    val dispatcher = EventDispatcher(eventStore, lifecycleStore)

    class DerivedEvent : Event()
    class TriggerEvent : Event()
    class OnHoldSignalEvent: SignalEvent()
    class OtherEvent : Event()
    class DummyEvent : Event()

    @BeforeEach
    fun setup() {
        EventTypeRegistry.wipe()
        EventListenerRegistry.wipe()

        EventTypeRegistry.register(
            listOf(
                DerivedEvent::class.java,
                TriggerEvent::class.java,
                OtherEvent::class.java,
                DummyEvent::class.java,
                OnHoldSignalEvent::class.java
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

        val trigger = TriggerEvent().newReferenceId()
        dispatcher.dispatch(trigger.referenceId, listOf(trigger), listOf(trigger))

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

        val trigger = TriggerEvent().newReferenceId()
        val derived = DerivedEvent().derivedOf(trigger).toPersisted(1L, MyTime.utcNow())

        eventStore.persist(derived!!.toEvent()!!) // simulate prior production

        dispatcher.dispatch(trigger.referenceId, listOf(trigger, derived.toEvent()!!), listOf(trigger, derived.toEvent()!!))

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

        val e1 = TriggerEvent().newReferenceId()
        val e2 = OtherEvent().newReferenceId()
        dispatcher.dispatch(e1.referenceId, listOf(e1, e2), listOf(e1, e2))

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
        val referenceId = UUID.randomUUID()

        ProducingListener()

        val trigger = TriggerEvent().usingReferenceId(referenceId)
        dispatcher.dispatch(trigger.referenceId, listOf(trigger), listOf(trigger))

        val replayContext = listOf(trigger) + eventStore.all().mapNotNull { it.toEvent() }

        dispatcher.dispatch(trigger.referenceId, replayContext, replayContext)

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
        val referenceId = UUID.randomUUID()

        val dispatcher = EventDispatcher(eventStore, lifecycleStore)
        val received = mutableListOf<Event>()

        object : EventListener() {
            override fun onEvent(event: Event, history: List<Event>): Event? {
                received += event
                return null
            }
        }

        val original = TriggerEvent().usingReferenceId(referenceId)

        // Slettehendelse som peker på original
        val deleted = object : DeleteEvent(original.eventId) {}.apply { newReferenceId() }

        dispatcher.dispatch(original.referenceId, listOf(original, deleted), listOf(original, deleted))

        assertFalse(received.contains(original))
        assertTrue(received.any { it is DeleteEvent })
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
        val referenceId = UUID.randomUUID()

        object : EventListener() {
            override fun onEvent(event: Event, history: List<Event>): Event? {
                if (event is DeleteEvent) received += event
                return null
            }
        }

        val deleted = object : DeleteEvent(UUID.randomUUID()) {}.apply { usingReferenceId(referenceId) }
        dispatcher.dispatch(deleted.referenceId, listOf(deleted), listOf(deleted))

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

        val trigger = TriggerEvent().newReferenceId()
        dispatcher.dispatch(trigger.referenceId, listOf(trigger), listOf(trigger))

        val produced = eventStore.all().mapNotNull { it.toEvent() }
        assertEquals(1, produced.size)
        val derived = produced.first()
        assertTrue(derived is DerivedEvent)

        // Replay: nå har vi både trigger og derived i konteksten
        val replayContext = listOf(trigger, derived)
        dispatcher.dispatch(trigger.referenceId, replayContext, replayContext)


        assertEquals(1, eventStore.all().size)
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
        val dispatcher = EventDispatcher(eventStore, lifecycleStore)

        val original = TriggerEvent().newReferenceId()
        val deleted = object : DeleteEvent(original.eventId) {}.apply { usingReferenceId(original.referenceId) }

        var receivedHistory: List<Event> = emptyList()

        object : EventListener() {
            override fun onEvent(event: Event, history: List<Event>): Event? {
                receivedHistory = history
                return null
            }
        }

        dispatcher.dispatch(original.referenceId, listOf(original, deleted), listOf(original, deleted))

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
        val dispatcher = EventDispatcher(eventStore, lifecycleStore)
        val referenceId = UUID.randomUUID()
        val e1 = TriggerEvent().usingReferenceId(referenceId)
        val e2 = OtherEvent().usingReferenceId(referenceId)
        val deleted = object : DeleteEvent(e1.eventId){}.newReferenceId()

        var receivedHistory: List<Event> = emptyList()

        object : EventListener() {
            override fun onEvent(event: Event, history: List<Event>): Event? {
                receivedHistory = history
                return null
            }
        }

        dispatcher.dispatch(e1.referenceId, listOf(e1, e2, deleted), listOf(e1, e2, deleted))

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
        val dispatcher = EventDispatcher(eventStore, lifecycleStore)

        val original = TriggerEvent().newReferenceId()
        val deleted = object : DeleteEvent(original.eventId) {}.apply { newReferenceId() }

        var receivedEvent: Event? = null
        var receivedHistory: List<Event> = emptyList()

        object : EventListener() {
            override fun onEvent(event: Event, history: List<Event>): Event? {
                receivedEvent = event
                receivedHistory = history
                return null
            }
        }

        dispatcher.dispatch(original.referenceId, listOf(original, deleted), listOf(original, deleted))

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
        EventTypeRegistry.register(listOf(TestSignalEvent::class.java))

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

        dispatcher.dispatch(refId, listOf(trigger, signal), listOf(trigger, signal))

        assertTrue(received.any { it is TriggerEvent })
        assertFalse(received.any { it is TestSignalEvent })
        assertNotNull(finalHistory)
        assertTrue(finalHistory!!.any { it is TestSignalEvent })
    }

    @Test
    @DisplayName(
        """
    Når et event er slettet
    Hvis det finnes et tidligere ikke-signal, ikke-derived event
    Så skal replayCandidates() velge dette eventet som kandidat
    """
    )
    fun replayShouldSelectLastValidEventWhenLaterEventIsDeleted() {
        val dispatcher = EventDispatcher(eventStore, lifecycleStore)
        val referenceId = UUID.randomUUID()

        val received = mutableListOf<Event>()

        // Lytter som fanger replay-kandidater
        object : EventListener() {
            override fun onEvent(event: Event, history: List<Event>): Event? {
                received += event
                return null
            }
        }

        val trigger = TriggerEvent().usingReferenceId(referenceId)
        val derived = DerivedEvent().derivedOf(trigger)
        val deleted = object : DeleteEvent(derived.eventId) {}.apply { usingReferenceId(referenceId) }

        dispatcher.dispatch(referenceId, listOf(trigger, derived, deleted), listOf(trigger, derived, deleted))

        assertTrue(received.any { it.eventId == trigger.eventId })
        assertFalse(received.any { it.eventId == derived.eventId })
        assertTrue(received.any { it.eventId == deleted.eventId } || received.any { it.eventId == trigger.eventId })
    }

    @Test
    @DisplayName(
        """
    Når en lytter produserer en derived event fra en historisk hendelse
    Hvis lytteren ikke tillater historisk derivation
    Så skal IllegalDerivationException kastes
    """
    )
    fun shouldThrowWhenDerivingFromHistoricalEventWithoutOptIn() {
        val dispatcher = EventDispatcher(eventStore, lifecycleStore)
        val referenceId = UUID.randomUUID()

        // Historikk
        val e1 = TriggerEvent().usingReferenceId(referenceId)
        val e2 = OtherEvent().usingReferenceId(referenceId)
        val e3 = DummyEvent().usingReferenceId(referenceId)

        // Dispatch-kandidat
        val e4 = TriggerEvent().usingReferenceId(referenceId)

        // Lytter som produserer en ulovlig derived event
        object : EventListener() {
            override fun onEvent(event: Event, history: List<Event>): Event? {
                if (event == e4) {
                    return DerivedEvent().derivedOf(e2)
                }
                return null
            }
        }

        val context = listOf(e1, e2, e3, e4)

        assertThrows(HardDispatchException.IllegalDerivationException::class.java) {
            dispatcher.dispatch(referenceId, context, listOf(e4))
        }
    }

    @Test
    @DisplayName(
        """
    Når en lytter produserer en derived event fra en historisk hendelse
    Hvis lytteren tillater historisk derivation
    Så skal dispatch fullføre uten feil
    """
    )
    fun shouldAllowHistoricalDerivationWhenOptInIsEnabled() {
        val dispatcher = EventDispatcher(eventStore, lifecycleStore)
        val referenceId = UUID.randomUUID()

        // Historikk
        val e1 = TriggerEvent().usingReferenceId(referenceId)
        val e2 = OtherEvent().usingReferenceId(referenceId)
        val e3 = DummyEvent().usingReferenceId(referenceId)

        // Dispatch-kandidat
        val e4 = TriggerEvent().usingReferenceId(referenceId)

        // Lytter som tillater historisk derivation
        object : EventListener() {
            override fun allowDerivativeOnHistoricalEvent(): Boolean = true

            override fun onEvent(event: Event, history: List<Event>): Event? {
                if (event == e4) {
                    return DerivedEvent().derivedOf(e2)
                }
                return null
            }
        }

        val context = listOf(e1, e2, e3, e4)

        dispatcher.dispatch(referenceId, context, listOf(e4))

        val produced = eventStore.all().mapNotNull { it.toEvent() }
        assertEquals(1, produced.size)
        assertTrue(produced.first() is DerivedEvent)
        assertTrue(produced.first().metadata.derivedFromId!!.contains(e2.eventId))
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
