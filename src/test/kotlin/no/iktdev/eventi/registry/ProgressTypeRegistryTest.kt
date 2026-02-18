package no.iktdev.eventi.registry

import no.iktdev.eventi.TestBase
import no.iktdev.eventi.events.EchoEvent
import no.iktdev.eventi.events.StartEvent
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.Progress
import no.iktdev.eventi.testUtil.wipe
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.lang.reflect.Field

class ProgressTypeRegistryTest: TestBase() {
    class EncodeProgress(override val progress: Int, override val message: String): Progress()
    class ConvertProgress(override val progress: Int, override val message: String): Progress()

    @BeforeEach
    fun setup() {
        ProgressTypeRegistry.wipe()
        // Verifiser at det er tomt
        assertNull(ProgressTypeRegistry.resolve("SomeEvent"))
    }


    @Test
    @DisplayName("Test ProgressTypeRegistry registration")
    fun scenario1() {
        ProgressTypeRegistry.register(EncodeProgress::class.java, ConvertProgress::class.java)
        assertThat(ProgressTypeRegistry.resolve("EncodeProgress")).isEqualTo(EncodeProgress::class.java)
        assertThat(ProgressTypeRegistry.resolve("ConvertProgress")).isEqualTo(ConvertProgress::class.java)
    }

}

fun ProgressTypeRegistry.wipe() {
    val field: Field = ProgressTypeRegistry::class.java
        .superclass
        .getDeclaredField("types")
    field.isAccessible = true

    // Tøm map’en
    val typesMap = field.get(ProgressTypeRegistry) as MutableMap<*, *>
    @Suppress("UNCHECKED_CAST")
    (typesMap as MutableMap<String, Class<out Event>>).clear()

    // Verifiser at det er tomt
    assertNull(ProgressTypeRegistry.resolve("ANnonExistingEvent"))
}