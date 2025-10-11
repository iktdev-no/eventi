package no.iktdev.eventi.testUtil

import no.iktdev.eventi.events.EventListenerRegistry
import no.iktdev.eventi.events.EventTypeRegistry
import no.iktdev.eventi.tasks.TaskListenerRegistry
import no.iktdev.eventi.tasks.TaskTypeRegistry
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow

class UtilTest {

    @Test
    @DisplayName("Test wipe function")
    fun scenario1() {
        assertDoesNotThrow {
            EventTypeRegistry.wipe()
            EventListenerRegistry.wipe()
            TaskListenerRegistry.wipe()
            TaskTypeRegistry.wipe()
        }
    }
}