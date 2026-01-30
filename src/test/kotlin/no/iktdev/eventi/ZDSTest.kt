package no.iktdev.eventi

import no.iktdev.eventi.ZDS.toEvent
import no.iktdev.eventi.ZDS.toPersisted
import no.iktdev.eventi.ZDS.toTask
import no.iktdev.eventi.events.EchoEvent
import no.iktdev.eventi.events.EventTypeRegistry
import no.iktdev.eventi.models.Task
import no.iktdev.eventi.tasks.TaskTypeRegistry
import no.iktdev.eventi.testUtil.wipe
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

@DisplayName("""
ZDS – Serialization/Deserialization System
Når Event- og Task-objekter persisteres og gjenopprettes
Hvis type-registrene er korrekt konfigurert
Så skal ZDS kunne serialisere og deserialisere objektene uten tap av data
""")
class ZDSTest {

    @BeforeEach
    fun setup() {
        EventTypeRegistry.wipe()
        TaskTypeRegistry.wipe()

        // Verifiser at det er tomt
        assertNull(EventTypeRegistry.resolve("SomeEvent"))
    }

    @Test
    @DisplayName("""
    Når et Event-objekt persisteres via ZDS
    Hvis typen er registrert i EventTypeRegistry
    Så skal det kunne gjenopprettes som riktig Event-type med samme data
    """)
    fun scenario1() {
        EventTypeRegistry.register(EchoEvent::class.java)

        val echo = EchoEvent("hello")
        val persisted = echo.toPersisted(id = 1L)

        val restored = persisted!!.toEvent()
        assert(restored is EchoEvent)
        assert((restored as EchoEvent).data == "hello")
    }

    data class TestTask(
        val data: String?
    ) : Task()

    @Test
    @DisplayName("""
    Når et Task-objekt persisteres via ZDS
    Hvis typen er registrert i TaskTypeRegistry
    Så skal det kunne gjenopprettes som riktig Task-type med metadata intakt
    """)
    fun scenario2() {
        TaskTypeRegistry.register(TestTask::class.java)

        val task = TestTask("Potato")
            .newReferenceId()

        val persisted = task.toPersisted(id = 1L)

        val restored = persisted!!.toTask()
        assert(restored is TestTask)
        assert((restored as TestTask).data == "Potato")
        assert(restored.metadata.created == task.metadata.created)
        assert(restored.metadata.derivedFromId == task.metadata.derivedFromId)
    }
}
