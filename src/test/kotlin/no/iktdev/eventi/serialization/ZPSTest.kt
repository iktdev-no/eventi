package no.iktdev.eventi.serialization

import no.iktdev.eventi.models.Progress
import no.iktdev.eventi.models.ProgressEnvelope
import no.iktdev.eventi.registry.ProgressTypeRegistry
import no.iktdev.eventi.registry.wipe
import no.iktdev.eventi.serialization.ZPS.toEnvelope
import no.iktdev.eventi.serialization.ZPS.toProgress
import no.iktdev.eventi.serialization.ZPS.toJsonEnvelope
import no.iktdev.eventi.serialization.ZPS.requireAs
import no.iktdev.eventi.testUtil.wipe
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

@DisplayName("""
ZPS – Serialization/Deserialization System
Når Progress-objekter pakkes i en envelope og gjenopprettes
Hvis type-registrene er korrekt konfigurert
Så skal ZPS kunne serialisere og deserialisere objektene uten tap av data
""")
class ZPSTest {

    @BeforeEach
    fun setup() {
        ProgressTypeRegistry.wipe()
        assertNull(ProgressTypeRegistry.resolve("SomeProgress"))
    }

    // --- Dummy progress types for testing ---

    data class SimpleProgress(
        override val progress: Int,
        override val message: String,
        val extra: String
    ) : Progress()

    data class AdvancedProgress(
        override val progress: Int,
        override val message: String,
        val details: Map<String, Any>,
        val count: Int
    ) : Progress()

    @Test
    @DisplayName("""
    Når et Progress-objekt pakkes i en envelope via ZPS
    Hvis typen er registrert i ProgressTypeRegistry
    Så skal det kunne gjenopprettes som riktig Progress-type med samme data
    """)
    fun scenario1() {
        ProgressTypeRegistry.register(SimpleProgress::class.java)

        val original = SimpleProgress(
            progress = 42,
            message = "Halfway there",
            extra = "metadata"
        )

        val envelope = original.toEnvelope()
        val restored = envelope.toProgress()

        assertTrue(restored is SimpleProgress)
        restored.requireAs<SimpleProgress>().also {
            assertEquals(42, it.progress)
            assertEquals("Halfway there", it.message)
            assertEquals("metadata", it.extra)
        }
    }

    @Test
    @DisplayName("""
    Når et Progress-objekt serialiseres til JSON via toJsonEnvelope()
    Hvis typen er registrert
    Så skal det kunne rekonstrueres fra JSON → Envelope → Progress
    """)
    fun scenario2() {
        ProgressTypeRegistry.register(AdvancedProgress::class.java)

        val original = AdvancedProgress(
            progress = 88,
            message = "Encoding segments",
            details = mapOf("segment" to 3, "fps" to 29.97),
            count = 7
        )

        val jsonEnvelope = original.toJsonEnvelope()

        // Deserialize JSON → Envelope
        val envelope = WGson.gson.fromJson(jsonEnvelope, ProgressEnvelope::class.java)

        // Envelope → Progress
        val restored = envelope.toProgress()

        assertTrue(restored is AdvancedProgress)
        restored.requireAs<AdvancedProgress>().also {
            assertEquals(88, it.progress)
            assertEquals("Encoding segments", it.message)
            assertEquals(7, it.count)
            assertEquals(3.0, it.details["segment"])
            assertEquals(29.97, it.details["fps"])
        }
    }
}
