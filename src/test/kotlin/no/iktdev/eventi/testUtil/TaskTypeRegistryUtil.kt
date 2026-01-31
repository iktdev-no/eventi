package no.iktdev.eventi.testUtil

import no.iktdev.eventi.events.EventTypeRegistry
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.Task
import no.iktdev.eventi.tasks.TaskTypeRegistry
import org.junit.jupiter.api.Assertions.assertNull
import java.lang.reflect.Field

fun TaskTypeRegistry.wipe() {
    val field: Field = TaskTypeRegistry::class.java
        .superclass
        .getDeclaredField("types")
    field.isAccessible = true

    // Tøm map’en
    val typesMap = field.get(TaskTypeRegistry) as MutableMap<*, *>
    @Suppress("UNCHECKED_CAST")
    (typesMap as MutableMap<String, Class<out Task>>).clear()

    // Verifiser at det er tomt
    assertNull(TaskTypeRegistry.resolve("ANnonExistingEvent"))
}