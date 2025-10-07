package no.iktdev.eventi

import no.iktdev.eventi.events.EchoEvent
import no.iktdev.eventi.events.EventTypeRegistry
import no.iktdev.eventi.events.StartEvent
import no.iktdev.eventi.testUtil.wipe
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

class EventTypeRegistryTest: TestBase() {

    @BeforeEach
    fun setup() {
        EventTypeRegistry.wipe()
        // Verifiser at det er tomt
        assertNull(EventTypeRegistry.resolve("SomeEvent"))
    }


    @Test
    @DisplayName("Test EventTypeRegistry registration")
    fun scenario1() {
        DefaultTestEvents()
        assertThat(EventTypeRegistry.resolve("EchoEvent")).isEqualTo(EchoEvent::class.java)
        assertThat(EventTypeRegistry.resolve("StartEvent")).isEqualTo(StartEvent::class.java)
    }

}