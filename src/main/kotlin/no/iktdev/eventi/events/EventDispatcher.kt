package no.iktdev.eventi.events

import kotlinx.coroutines.Job
import mu.KotlinLogging
import no.iktdev.eventi.MyTime
import no.iktdev.eventi.lifecycle.ILifecycleStore
import no.iktdev.eventi.lifecycle.LifecycleStore
import no.iktdev.eventi.lifecycle.ListenerFatalError
import no.iktdev.eventi.lifecycle.ListenerResult
import no.iktdev.eventi.models.DeleteEvent
import no.iktdev.eventi.models.DispatchResult
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.SignalEvent
import no.iktdev.eventi.registry.EventListenerRegistry
import no.iktdev.eventi.stores.EventStore
import java.util.UUID

open class EventDispatcher(val eventStore: EventStore, private val lifecycleStore: ILifecycleStore) {

    private val log = KotlinLogging.logger {}

    open fun dispatch(referenceId: UUID, history: List<Event>, newEvents: List<Event>) {
        val deletedEventIds = (history + newEvents)
            .filterIsInstance<DeleteEvent>()
            .map { it.deletedEventId }

        val effectiveHistory = history
            .validEventsRecursive(deletedEventIds)
            .filterNot { it is DeleteEvent }

        var candidates = newEvents
            .validEventsRecursive(deletedEventIds)
            .replayCandidates()

        // Fallback: kun når alle nye er signaler → bruk historikk som “nytt”
        if (candidates.isEmpty() && newEvents.all { it is SignalEvent }) {
            candidates = effectiveHistory
                .replayCandidates()
        }

        EventListenerRegistry.getListeners().forEach { listener ->

            if (listener.allowDerivativeOnHistoricalEvent() &&
                lifecycleStore.hasAccepted(referenceId, listener::class.java.simpleName)
            ) {
                log.debug { "🔁 Listener ${listener::class.simpleName} already accepted for $referenceId → skipping further candidates" }
                return@forEach
            }

            for (candidate in candidates) {
                //log.debug("Evaluating candidate: ${candidate::class.simpleName} for listener ${listener::class.simpleName}")
                try {
                    val result = listener.onEvent(candidate, effectiveHistory)
                    if (result?.hasReferenceIdBeenSet() == false) {
                        log.warn { "ReferenceId is missing on produced event when dispatched ${candidate::class.simpleName}, setting this as parent for produced event!" }
                        result.apply { derivedOf(candidate) }
                    }

                    if (result != null) {
                        validateReferenceId(result, listener)
                        validateDerivation(result, candidate, effectiveHistory, listener)
                        eventStore.persist(result)
                        //producedEvents.add(result)
                        lifecycleStore.add(
                            ListenerResult(
                                timestamp = MyTime.utcNow(),
                                ref = referenceId,
                                listener = listener::class.java.simpleName,
                                result = "Accepted"
                            )
                        )
                        onDispatched(candidate, listener, DispatchResult.Accepted)
                        if (listener.allowDerivativeOnHistoricalEvent()) {
                            log.debug { "⏹️ Listener ${listener::class.simpleName} requested historical derivation → adding listener to accepted list" }
                            break
                        }
                    } else {
                        lifecycleStore.add(
                            ListenerResult(
                                timestamp = MyTime.utcNow(),
                                ref = referenceId,
                                listener = listener::class.java.simpleName,
                                result = "NoResult"
                            )
                        )
                        onDispatched(candidate, listener, DispatchResult.NoResult)
                    }

                } catch (e: SoftDispatchException.UnqualifiedEntryEventException) {
                    lifecycleStore.add(
                        ListenerResult(
                            timestamp = MyTime.utcNow(),
                            ref = referenceId,
                            listener = listener::class.java.simpleName,
                            result = "Rejected: ${e.message}"
                        )
                    )
                    onDispatched(candidate, listener, DispatchResult.Skipped, e.message)
                } catch (e: SoftDispatchException.SkipListenerException) {
                    lifecycleStore.add(
                        ListenerResult(
                            timestamp = MyTime.utcNow(),
                            ref = referenceId,
                            listener = listener::class.java.simpleName,
                            result = "Skipped: ${e.message}"
                        )
                    )
                    onDispatched(candidate, listener, DispatchResult.Skipped, e.message)
                } catch (e: SoftDispatchException) {
                    lifecycleStore.add(
                        ListenerResult(
                            timestamp = MyTime.utcNow(),
                            ref = referenceId,
                            listener = listener::class.java.simpleName,
                            result = "Error: ${e.message}"
                        )
                    )
                    onDispatched(candidate, listener, DispatchResult.Error, e.message)
                } catch (e: Exception) {

                    // ⭐ NYTT: Fatal exception
                    lifecycleStore.add(
                        ListenerFatalError(
                            timestamp = MyTime.utcNow(),
                            ref = referenceId,
                            listener = listener::class.java.simpleName,
                            eventName = candidate::class.java.simpleName,
                            exception = "${e::class.java.simpleName}: ${e.message}"
                        )
                    )

                    // Du kan velge å logge dette hardt
                    log.error(e) { "Fatal exception in listener ${listener::class.java.simpleName}" }

                    // Og du kan velge hva du gjør videre:
                    // - rethrow (stopper dispatch)
                    // - swallow (fortsetter)
                    // - wrap i HardDispatchException
                    throw e
                }
            }
        }

        // producedEvents.forEach { eventStore.persist(it) }
    }

    private fun UUID.short(): String = this.toString().substring(0, 8)
    open fun onDispatched(
        event: Event,
        listener: EventListener,
        result: DispatchResult,
        message: String? = null
    ) {
        val listenerName = listener::class.java.simpleName
        val ref = event.referenceId.short()
        val evtId = event.eventId.short()
        val evtName = event::class.java.simpleName

        val msg = buildString {
            append("[$ref] $listenerName → $evtId $evtName")
            if (!message.isNullOrBlank()) {
                append("\n  ↳ $message")
            }
        }


        when (result) {
            DispatchResult.Accepted,
            DispatchResult.NoResult,
            DispatchResult.Skipped ->
                log.debug { msg }

            DispatchResult.Rejected ->
                log.warn { msg }

            DispatchResult.Error ->
                log.error { msg }
        }
    }

    fun List<Event>.replayCandidates(): List<Event> {
        val derivedFromIds = this
            .filterNot { it is SignalEvent }      // ignorer signaler som “forbrukere”
            .mapNotNull { it.metadata.derivedFromId }
            .flatten()
            .toSet()

        return this
            .filterNot { it is SignalEvent }      // signaler er fortsatt ikke kandidater
            .filter { it.eventId !in derivedFromIds }
    }

    fun List<Event>.validEvents(deletedEventIds: List<UUID>): List<Event> {
        return this.filter { event ->
            val id = event.eventId
            val parents = event.metadata.derivedFromId ?: emptyList()

            id !in deletedEventIds && parents.none { it in deletedEventIds }
        }
    }

    fun List<Event>.validEventsRecursive(deletedEventIds: List<UUID>): List<Event> {
        if (deletedEventIds.isEmpty()) return this

        val childrenByParent = this
            .flatMap { evt ->
                (evt.metadata.derivedFromId ?: emptyList()).map { parentId ->
                    parentId to evt.eventId
                }
            }
            .groupBy({ it.first }, { it.second })

        val toDelete = mutableSetOf<UUID>()
        val queue = ArrayDeque<UUID>()

        queue.addAll(deletedEventIds)
        toDelete.addAll(deletedEventIds)

        while (queue.isNotEmpty()) {
            val current = queue.removeFirst()
            val children = childrenByParent[current] ?: emptyList()
            for (child in children) {
                if (child !in toDelete) {
                    toDelete.add(child)
                    queue.add(child)
                }
            }
        }

        return this.filter { it.eventId !in toDelete }
    }




    /**
     * Validates that referenceId (a lateinit) is initialized.
     * If not, throws a clear and actionable exception.
     */
    private fun validateReferenceId(event: Event, listener: Any) {
        try {
            // Accessing lateinit will throw if not initialized
            event.referenceId
        } catch (e: UninitializedPropertyAccessException) {
            throw IllegalStateException(
                "Listener ${listener::class.simpleName} attempted to persist " +
                        "${event::class.simpleName} (${event.eventId}) without initializing referenceId"
            )
        }
    }

    private fun validateDerivation(
        producedEvent: Event,
        sourceEvent: Event,
        history: List<Event>,
        listener: EventListener
    ) {
        val parents = producedEvent.metadata.derivedFromId ?: emptyList()
        if (parents.isEmpty()) return

        // 🚫 NYT TILLEGG: Forhindre at et event derivere fra samme type
        if (producedEvent::class == sourceEvent::class) {
            throw IllegalStateException(
                "Invalid derivation: ${producedEvent::class.simpleName} cannot derive from same event type. " +
                        "This would create a recursive event chain (spinlock) with no natural termination."
            )
        }

        // Lytteren har eksplisitt opt-in → da er alt i historikken lov
        if (listener.allowDerivativeOnHistoricalEvent()) {
            // Men vi kan fortsatt nekte Signal/Delete som parent hvis du vil
            val historyById = history.associateBy { it.eventId }
            parents.forEach { parentId ->
                val parent = historyById[parentId]
                if (parent is SignalEvent || parent is DeleteEvent) {
                    throw HardDispatchException.IllegalDerivationException(
                        parentId = parentId,
                        producedEvent = producedEvent,
                        listener = listener
                    )
                }
            }
            return
        }

        // Default: kun lov å derivere fra sourceEvent
        parents.forEach { parentId ->
            if (parentId != sourceEvent.eventId) {
                throw HardDispatchException.IllegalDerivationException(
                    parentId = parentId,
                    producedEvent = producedEvent,
                    listener = listener
                )
            }
        }
    }



}
