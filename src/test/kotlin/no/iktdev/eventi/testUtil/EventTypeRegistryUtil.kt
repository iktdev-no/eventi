package no.iktdev.eventi.testUtil

import no.iktdev.eventi.events.EventTypeRegistry
import no.iktdev.eventi.models.Event
import org.junit.jupiter.api.Assertions.assertNull
import java.lang.reflect.Field

fun EventTypeRegistry.wipe() {
    val field: Field = EventTypeRegistry::class.java
        .superclass
        .getDeclaredField("types")
    field.isAccessible = true

    // Tøm map’en
    val typesMap = field.get(EventTypeRegistry) as MutableMap<*, *>
    (typesMap as MutableMap<String, Class<out Event>>).clear()

    // Verifiser at det er tomt
    assertNull(EventTypeRegistry.resolve("ANnonExistingEvent"))
}