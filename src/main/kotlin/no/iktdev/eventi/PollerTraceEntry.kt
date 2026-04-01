package no.iktdev.eventi

import java.time.Instant
import java.util.UUID
import java.util.ArrayDeque

sealed interface PollerTraceEntry {
    val timestamp: Instant
}

data class GlobalTrace(
    override val timestamp: Instant,
    val lastSeenBefore: Instant,
    val lastSeenAfter: Instant,
    val maxSeenThisRound: Instant?,
    val refsProcessed: List<UUID>,
    val refsDeferred: List<UUID>
) : PollerTraceEntry

data class RefTrace(
    override val timestamp: Instant,
    val ref: UUID,
    val seen: List<Pair<Instant, Long>>,
    val newForRef: List<Pair<Instant, Long>>,
    val watermarkBefore: Pair<Instant, Long>,
    val watermarkAfter: Pair<Instant, Long>?,
    val busy: Boolean,
    val dispatched: Boolean
) : PollerTraceEntry

class PollerTraceStore(private val capacity: Int = 500) {

    private val buffer = ArrayDeque<PollerTraceEntry>(capacity)

    @Synchronized
    fun add(entry: PollerTraceEntry) {
        if (buffer.size >= capacity) buffer.removeFirst()
        buffer.addLast(entry)
    }

    @Synchronized
    fun getAll(): List<PollerTraceEntry> = buffer.toList()

    @Synchronized
    fun getForRef(ref: UUID): List<RefTrace> =
        buffer.filterIsInstance<RefTrace>().filter { it.ref == ref }
}
