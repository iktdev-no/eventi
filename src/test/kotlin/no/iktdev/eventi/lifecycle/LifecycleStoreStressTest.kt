package no.iktdev.eventi.lifecycle

import no.iktdev.eventi.lifecycle.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import java.time.Instant

class LifecycleStoreStressTest {

    @Test
    fun `lifecycle store handles millions of entries without OOM or overflow`() {
        val capacity = 1000
        val store = LifecycleStore(capacity)

        assertDoesNotThrow {
            repeat(5_000_000) { i ->
                store.add(
                    TaskPollerCycleStart(
                        timestamp = Instant.ofEpochMilli(i.toLong()) // garantert unik
                    )
                )

                // sjekk hver 100k
                if (i % 100_000 == 0 && i > capacity) {
                    val all = store.getAll()

                    // bufferen skal være full
                    assertEquals(
                        capacity,
                        all.size,
                        "LifecycleStore did not fill to capacity"
                    )
                }
            }
        }

        // sluttverifisering
        val final = store.getAll()
        assertEquals(capacity, final.size)
        assertTrue(final.first().timestamp.toEpochMilli() > 0)
    }
}
