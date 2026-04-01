package no.iktdev.eventi.lifecycle

import java.util.ArrayDeque
import java.util.UUID

class LifecycleStore(private val capacity: Int = 50_000) {

    private val buffer = ArrayDeque<LifecycleEntry>(capacity)

    @Synchronized
    fun add(entry: LifecycleEntry) {
        if (buffer.size >= capacity) {
            buffer.removeFirst()
        }
        buffer.addLast(entry)
    }

    @Synchronized
    fun getAll(): List<LifecycleEntry> =
        buffer.toList()

    @Synchronized
    fun getForRef(ref: UUID): List<LifecycleEntry> =
        buffer.filter {
            when (it) {
                is RefFiltered -> it.ref == ref
                is RefBusy -> it.ref == ref
                is RefDispatchStarted -> it.ref == ref
                is RefDispatchCompleted -> it.ref == ref
                is RefWatermarkUpdated -> it.ref == ref
                is DispatchQueueAcquired -> it.ref == ref
                is DispatchQueueReleased -> it.ref == ref
                is DispatchQueueSkipped -> it.ref == ref
                is ListenerInvoked -> it.ref == ref
                is ListenerResult -> it.ref == ref
                else -> false
            }
        }
}
