package no.iktdev.eventi.events

import no.iktdev.eventi.ListenerOrder
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.testUtil.wipe
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

@DisplayName("""
EventListenerRegistry
Når lyttere registreres med og uten @ListenerOrder
Hvis registry sorterer dem etter annotasjonen
Så skal rekkefølgen være deterministisk og korrekt
""")
class EventListenerRegistryTest {

    @ListenerOrder(1)
    class MockTest1 : EventListener() {
        override fun onEvent(event: Event, history: List<Event>): Event? = null
    }

    @ListenerOrder(2)
    class MockTest2 : EventListener() {
        override fun onEvent(event: Event, history: List<Event>): Event? = null
    }

    @ListenerOrder(3)
    class MockTest3 : EventListener() {
        override fun onEvent(event: Event, history: List<Event>): Event? = null
    }

    class MockTestRandom : EventListener() {
        override fun onEvent(event: Event, history: List<Event>): Event? = null
    }

    @BeforeEach
    fun clear() {
        EventListenerRegistry.wipe()
    }

    @Test
    @DisplayName("""
    Når flere lyttere registreres i vilkårlig rekkefølge
    Hvis noen har @ListenerOrder og andre ikke
    Så skal registry returnere dem sortert etter order, og usorterte sist
    """)
    fun validateOrder() {
        MockTestRandom()
        MockTest1()
        MockTest2()
        MockTest3()

        val listeners = EventListenerRegistry.getListeners()

        assertThat(listeners.map { it::class.simpleName }).containsExactly(
            MockTest1::class.simpleName,   // @ListenerOrder(1)
            MockTest2::class.simpleName,   // @ListenerOrder(2)
            MockTest3::class.simpleName,   // @ListenerOrder(3)
            MockTestRandom::class.simpleName // no annotation → goes last
        )
    }
}
