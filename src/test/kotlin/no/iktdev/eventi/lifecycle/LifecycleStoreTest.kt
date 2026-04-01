package no.iktdev.eventi.lifecycle

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import java.time.Instant
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class LifecycleStoreTest {

    /* ---------------------------------------------------------
       1. BASIC CAPACITY TEST
       --------------------------------------------------------- */

    @Test
    fun `store should never exceed capacity`() {
        val capacity = 1000
        val store = LifecycleStore(capacity)

        repeat(10_000) { i ->
            store.add(
                TaskPollerCycleStart(
                    timestamp = Instant.ofEpochMilli(i.toLong())
                )
            )
        }

        val all = store.getAll()
        assertEquals(capacity, all.size, "Store exceeded capacity")
    }


    /* ---------------------------------------------------------
       2. STRESS TEST (5 MILLION ENTRIES)
       --------------------------------------------------------- */

    @Test
    fun `store handles millions of entries without OOM or overflow`() {
        val capacity = 2000
        val store = LifecycleStore(capacity)

        assertDoesNotThrow {
            repeat(5_000_000) { i ->
                store.add(
                    TaskPollerCycleStart(
                        timestamp = Instant.ofEpochMilli(i.toLong())
                    )
                )
            }
        }

        val all = store.getAll()
        assertEquals(capacity, all.size)
    }


    /* ---------------------------------------------------------
       3. CONCURRENCY TEST
       --------------------------------------------------------- */

    @Test
    fun `store handles concurrent writers`() {
        val capacity = 5000
        val store = LifecycleStore(capacity)

        val pool = Executors.newFixedThreadPool(8)

        repeat(1_000_000) { i ->
            pool.submit {
                store.add(
                    TaskPollerCycleStart(
                        timestamp = Instant.ofEpochMilli(i.toLong())
                    )
                )
            }
        }

        pool.shutdown()
        pool.awaitTermination(30, TimeUnit.SECONDS)

        val all = store.getAll()
        assertEquals(capacity, all.size, "Concurrent writes exceeded capacity")
    }


    /* ---------------------------------------------------------
       4. REF FILTER TEST
       --------------------------------------------------------- */

    @Test
    fun `getForRef returns only entries for that ref`() {
        val store = LifecycleStore(1000)

        val refA = UUID.randomUUID()
        val refB = UUID.randomUUID()

        repeat(500) {
            store.add(
                RefDispatchCompleted(
                    timestamp = Instant.now(),
                    ref = refA
                )
            )
        }

        repeat(300) {
            store.add(
                RefDispatchCompleted(
                    timestamp = Instant.now(),
                    ref = refB
                )
            )
        }

        val aEntries = store.getForRef(refA)
        val bEntries = store.getForRef(refB)

        assertTrue(aEntries.all { it is RefDispatchCompleted && it.ref == refA })
        assertTrue(bEntries.all { it is RefDispatchCompleted && it.ref == refB })
    }


    /* ---------------------------------------------------------
       5. TIMELINE ORDER TEST
       --------------------------------------------------------- */

    @Test
    fun `entries maintain correct chronological order`() {
        val store = LifecycleStore(1000)

        repeat(1000) { i ->
            store.add(
                TaskPollerCycleStart(
                    timestamp = Instant.ofEpochMilli(i.toLong())
                )
            )
        }

        val all = store.getAll()
        val timestamps = all.map { it.timestamp.toEpochMilli() }

        assertEquals(timestamps.sorted(), timestamps, "Timeline is not ordered")
    }


    /* ---------------------------------------------------------
       6. LIGHT MEMORY PRESSURE TEST
       --------------------------------------------------------- */

    @Test
    fun `store does not leak memory`() {
        val capacity = 2000
        val store = LifecycleStore(capacity)

        val before = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()

        repeat(2_000_000) { i ->
            store.add(
                TaskPollerCycleStart(
                    timestamp = Instant.ofEpochMilli(i.toLong())
                )
            )
        }

        System.gc()
        Thread.sleep(200)

        val after = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()

        // Memory should not grow unbounded
        assertTrue(after < before * 3, "Possible memory leak detected")
    }
}
