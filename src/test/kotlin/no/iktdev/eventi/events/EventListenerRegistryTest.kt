package no.iktdev.eventi.events

import no.iktdev.eventi.ListenerOrder
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.testUtil.wipe
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class EventListenerRegistryTest {

    @ListenerOrder(1)
    class MockTest1() : EventListener() {
        override fun onEvent(event: Event, history: List<Event>): Event? {
            return null
        }
    }

    @ListenerOrder(2)
    class MockTest2() : EventListener() {
        override fun onEvent(event: Event, history: List<Event>): Event? {
            return null
        }
    }

    @ListenerOrder(3)
    class MockTest3() : EventListener() {
        override fun onEvent(event: Event, history: List<Event>): Event? {
            return null
        }
    }

    class MockTestRandom() : EventListener() {
        override fun onEvent(event: Event, history: List<Event>): Event? {
            return null
        }
    }

    @BeforeEach
    fun clear() {
        EventListenerRegistry.wipe()
    }

    @Test
    fun validateOrder() {
        MockTestRandom()
        MockTest1()
        MockTest2()
        MockTest3()
        val listeners = EventListenerRegistry.getListeners()
        // Assert
        assertThat(listeners.map { it::class.simpleName }).containsExactly(
            MockTest1::class.simpleName, // @ListenerOrder(1)
            MockTest2::class.simpleName, // @ListenerOrder(2)
            MockTest3::class.simpleName, // @ListenerOrder(3)
            MockTestRandom::class.simpleName // no annotation → goes last
        )
    }

}