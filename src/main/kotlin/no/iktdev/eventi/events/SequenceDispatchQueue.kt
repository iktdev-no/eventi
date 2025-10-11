package no.iktdev.eventi.events

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import no.iktdev.eventi.models.Event
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class SequenceDispatchQueue(
    private val maxConcurrency: Int = 8,
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.Default + SupervisorJob())
) {
    private val semaphore = Semaphore(maxConcurrency)
    private val active = ConcurrentHashMap.newKeySet<UUID>()

    fun _scope(): CoroutineScope {
        return scope
    }

    fun isProcessing(referenceId: UUID): Boolean = referenceId in active

    fun dispatch(referenceId: UUID, events: List<Event>, dispatcher: EventDispatcher): Job? {
        if (!active.add(referenceId)) return null // already processing

        return scope.launch {
            try {
                semaphore.acquire()
                try {
                    dispatcher.dispatch(referenceId, events)
                } catch (e: Exception) {
                    println("Dispatch failed for $referenceId: ${e.message}")
                } finally {
                    semaphore.release()
                }
            } finally {
                active.remove(referenceId)
            }
        }
    }
}
