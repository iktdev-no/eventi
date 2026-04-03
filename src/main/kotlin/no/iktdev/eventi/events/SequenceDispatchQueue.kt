package no.iktdev.eventi.events

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import mu.KotlinLogging
import no.iktdev.eventi.MyTime
import no.iktdev.eventi.lifecycle.DispatchExceptionEntry
import no.iktdev.eventi.lifecycle.DispatchQueueAcquired
import no.iktdev.eventi.lifecycle.DispatchQueueReleased
import no.iktdev.eventi.lifecycle.DispatchQueueSkipped
import no.iktdev.eventi.lifecycle.ILifecycleStore
import no.iktdev.eventi.lifecycle.LifecycleStore
import no.iktdev.eventi.lifecycle.RefDispatchCompleted
import no.iktdev.eventi.lifecycle.RefDispatchStarted
import no.iktdev.eventi.models.Event
import org.jetbrains.annotations.VisibleForTesting
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

open class SequenceDispatchQueue(
    private val maxConcurrency: Int = 8,
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.Default + SupervisorJob()),
    private val lifecycleStore: ILifecycleStore
) {
    private val semaphore = Semaphore(maxConcurrency)
    private val active = ConcurrentHashMap.newKeySet<UUID>()

    fun activeRefs(): Set<UUID> = active.toSet()

    @VisibleForTesting
    internal fun scope(): CoroutineScope {
        return scope
    }

    private val log = KotlinLogging.logger {}


    open fun isProcessing(referenceId: UUID): Boolean = referenceId in active

    open fun dispatch(referenceId: UUID, history: List<Event>, newEvents: List<Event>, dispatcher: EventDispatcher): Job? {
        if (!active.add(referenceId)) {
            lifecycleStore.add(
                DispatchQueueSkipped(
                    timestamp = MyTime.utcNow(),
                    ref = referenceId
                )
            )
            log.debug {"⚠️ Already processing $referenceId, skipping dispatch"}
            return null
        }

        // Dispatch starter
        lifecycleStore.add(
            RefDispatchStarted(
                timestamp = MyTime.utcNow(),
                ref = referenceId,
                historyCount = history.size,
                newCount = newEvents.size
            )
        )
        log.debug {"▶️ Starting dispatch for $referenceId with ${history.size} events"}

        return scope.launch(start = CoroutineStart.UNDISPATCHED) {
            try {
                // Venter på semaphore
                lifecycleStore.add(
                    DispatchQueueAcquired(
                        timestamp = MyTime.utcNow(),
                        ref = referenceId
                    )
                )
                log.debug {"⏳ Waiting for semaphore for $referenceId"}
                semaphore.acquire()
                log.debug {"🔓 Acquired semaphore for $referenceId"}

                try {
                    dispatcher.dispatch(referenceId, history, newEvents)
                } catch (e: Exception) {
                    log.error("Dispatch failed for $referenceId: ${e.message}")
                    e.printStackTrace()
                    lifecycleStore.add(
                        DispatchExceptionEntry(
                            timestamp = MyTime.utcNow(),
                            ref = referenceId,
                            exception = "${e::class.java.simpleName}: ${e.message}",
                            stacktrace = e.stackTraceToString()
                        )
                    )
                } finally {
                    semaphore.release()
                    lifecycleStore.add(
                        DispatchQueueReleased(
                            timestamp = MyTime.utcNow(),
                            ref = referenceId
                        )
                    )
                    log.debug {"✅ Released semaphore for $referenceId"}
                }
            } finally {
                active.remove(referenceId)
                lifecycleStore.add(
                    RefDispatchCompleted(
                        timestamp = MyTime.utcNow(),
                        ref = referenceId
                    )
                )
                log.debug {"🏁 Finished dispatch for $referenceId"}
            }
        }
    }

}
