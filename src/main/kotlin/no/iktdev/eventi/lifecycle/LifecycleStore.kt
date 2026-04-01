package no.iktdev.eventi.lifecycle

import java.util.ArrayDeque
import java.util.UUID

interface ILifecycleStore {
    fun add(entry: LifecycleEntry)
    fun getAll(): List<LifecycleEntry>
    fun getForRef(ref: UUID): List<LifecycleEntry>
}


class LifecycleStore(private val capacity: Int = 50_000): ILifecycleStore {

    private val buffer = ArrayDeque<LifecycleEntry>(capacity)

    @Synchronized
    override fun add(entry: LifecycleEntry) {
        if (buffer.size >= capacity) {
            buffer.removeFirst()
        }
        buffer.addLast(entry)
    }

    @Synchronized
    override fun getAll(): List<LifecycleEntry> =
        buffer.toList()

    @Synchronized
    override fun getForRef(ref: UUID): List<LifecycleEntry> =
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
