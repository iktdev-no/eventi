package no.iktdev.eventi.events

import kotlinx.coroutines.joinAll
import kotlinx.coroutines.test.runTest
import no.iktdev.eventi.EventDispatcherTest.DerivedEvent
import no.iktdev.eventi.EventDispatcherTest.OtherEvent
import no.iktdev.eventi.EventDispatcherTest.TriggerEvent
import no.iktdev.eventi.TestBase
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.testUtil.wipe
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

@DisplayName("""
SequenceDispatchQueue
Når mange referenceId-er skal dispatches parallelt
Hvis køen har begrenset samtidighet
Så skal alle events prosesseres uten tap
""")
class SequenceDispatchQueueTest : TestBase() {

    @BeforeEach
    fun setup() {
        EventTypeRegistry.wipe()
        EventListenerRegistry.wipe()

        EventTypeRegistry.register(
            listOf(
                DerivedEvent::class.java,
                TriggerEvent::class.java,
                OtherEvent::class.java
            )
        )
    }

    @Test
    @DisplayName("""
    Når 100 forskjellige referenceId-er dispatches
    Hvis køen har en maks samtidighet på 8
    Så skal alle referenceId-er bli prosessert nøyaktig én gang
    """)
    fun shouldDispatchAllReferenceIdsWithLimitedConcurrency() = runTest {
        val dispatcher = EventDispatcher(eventStore)
        val queue = SequenceDispatchQueue(maxConcurrency = 8)

        val dispatched = ConcurrentHashMap.newKeySet<UUID>()

        EventListenerRegistry.registerListener(
            object : EventListener() {
                override fun onEvent(event: Event, context: List<Event>): Event? {
                    dispatched += event.referenceId
                    Thread.sleep(50) // simuler tung prosessering
                    return null
                }
            }
        )

        val referenceIds = (1..100).map { UUID.randomUUID() }

        val jobs = referenceIds.mapNotNull { refId ->
            val e = TriggerEvent().usingReferenceId(refId)
            eventStore.persist(e)
            queue.dispatch(refId, listOf(e), dispatcher)
        }

        jobs.joinAll()

        assertEquals(100, dispatched.size)
    }
}
