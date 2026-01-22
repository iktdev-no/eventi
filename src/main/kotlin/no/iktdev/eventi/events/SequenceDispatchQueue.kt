package no.iktdev.eventi.events

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import mu.KotlinLogging
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

    private val log = KotlinLogging.logger {}


    fun isProcessing(referenceId: UUID): Boolean = referenceId in active

    fun dispatch(referenceId: UUID, events: List<Event>, dispatcher: EventDispatcher): Job? {
        if (!active.add(referenceId)) {
            log.debug {"⚠️ Already processing $referenceId, skipping dispatch"}
            return null
        }
        log.debug {"▶️ Starting dispatch for $referenceId with ${events.size} events"}


        return scope.launch {
            try {
                log.debug {"⏳ Waiting for semaphore for $referenceId"}
                semaphore.acquire()
                log.debug {"🔓 Acquired semaphore for $referenceId"}

                try {
                    dispatcher.dispatch(referenceId, events)
                } catch (e: Exception) {
                    log.error("Dispatch failed for $referenceId: ${e.message}")
                    e.printStackTrace()
                } finally {

                    semaphore.release()
                    log.debug {"✅ Released semaphore for $referenceId"}
                }
            } finally {
                active.remove(referenceId)
                log.debug {"🏁 Finished dispatch for $referenceId"}
            }
        }
    }
}
