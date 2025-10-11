package no.iktdev.eventi.testUtil

import no.iktdev.eventi.events.EventListener
import no.iktdev.eventi.events.EventListenerRegistry
import no.iktdev.eventi.tasks.TaskListener
import no.iktdev.eventi.tasks.TaskListenerRegistry
import org.assertj.core.api.Assertions.assertThat
import java.lang.reflect.Field

fun TaskListenerRegistry.wipe() {
    val field: Field = TaskListenerRegistry::class.java
        .superclass
        .getDeclaredField("listeners")
    field.isAccessible = true

    // Tøm map’en
    val mutableList = field.get(TaskListenerRegistry) as MutableList<*>
    (mutableList as MutableList<Class<out TaskListener<*>>>).clear()

    // Verifiser at det er tomt
    assertThat(TaskListenerRegistry.getListeners().isEmpty())
}