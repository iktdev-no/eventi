package no.iktdev.eventi

import no.iktdev.eventi.ZDS.toEvent
import no.iktdev.eventi.ZDS.toPersisted
import no.iktdev.eventi.events.EchoEvent
import no.iktdev.eventi.events.EventTypeRegistry
import no.iktdev.eventi.testUtil.wipe
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.time.LocalDateTime

class ZDSTest {

    @BeforeEach
    fun setup() {
        EventTypeRegistry.wipe()
        // Verifiser at det er tomt
        assertNull(EventTypeRegistry.resolve("SomeEvent"))
    }

    @Test
    @DisplayName("Test ZDS")
    fun scenario1() {
        EventTypeRegistry.register(EchoEvent::class.java)

        val echo = EchoEvent("hello")
        val persisted = echo.toPersisted(id = 1L, persistedAt = LocalDateTime.now())

        val restored = persisted.toEvent()
        assert(restored is EchoEvent)
        assert((restored as EchoEvent).data == "hello")

    }

}