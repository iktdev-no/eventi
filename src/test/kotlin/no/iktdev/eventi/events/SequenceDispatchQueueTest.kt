package no.iktdev.eventi.events

import kotlinx.coroutines.joinAll
import kotlinx.coroutines.test.runTest
import no.iktdev.eventi.EventDispatcherTest
import no.iktdev.eventi.EventDispatcherTest.DerivedEvent
import no.iktdev.eventi.EventDispatcherTest.OtherEvent
import no.iktdev.eventi.EventDispatcherTest.TriggerEvent
import no.iktdev.eventi.TestBase
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.testUtil.wipe
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class SequenceDispatchQueueTest: TestBase() {

    @BeforeEach
    fun setup() {
        EventTypeRegistry.wipe()
        EventListenerRegistry.wipe()
        // Verifiser at det er tomt

        EventTypeRegistry.register(listOf(
            DerivedEvent::class.java,
            TriggerEvent::class.java,
            OtherEvent::class.java
        ))
    }


    @Test
    fun `should dispatch all referenceIds with limited concurrency`() = runTest {
        val dispatcher = EventDispatcher(store)
        val queue = SequenceDispatchQueue(maxConcurrency = 8)

        val dispatched = ConcurrentHashMap.newKeySet<UUID>()

        EventListenerRegistry.registerListener(object : EventListener() {
            override fun onEvent(event: Event, context: List<Event>): Event? {
                dispatched += event.referenceId
                Thread.sleep(50) // simuler tung prosessering
                return null
            }
        })

        val referenceIds = (1..100).map { UUID.randomUUID() }

        val jobs = referenceIds.mapNotNull { refId ->
            val e = TriggerEvent().usingReferenceId(refId)
            store.save(e)
            queue.dispatch(refId, listOf(e), dispatcher)
        }

        jobs.joinAll()

        assertEquals(100, dispatched.size)
    }


}