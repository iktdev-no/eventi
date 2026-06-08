package no.iktdev.eventi.models

import org.jetbrains.annotations.VisibleForTesting
import java.util.UUID

@Suppress("UNCHECKED_CAST")
abstract class Event {
    lateinit var referenceId: UUID
        protected set
    var eventId: UUID = UUID.randomUUID()
        private set
    var metadata: Metadata = Metadata()
        protected set

    protected open fun <T : Event> self(): T = this as T

    fun producedFrom(task: Task): Event = self<Event>().apply {
        referenceId = task.referenceId
        val derivedFromIds: MutableList<UUID> = mutableListOf()
        task.metadata.derivedFromId?.let { derivedFromIds.addAll(it) }
        derivedFromIds.add(task.taskId)
        metadata = Metadata().derivedFromEventId(derivedFromIds.toSet())
    }

    fun derivedOf(vararg events: Event) =
        derivedOf(events.toList())


    fun derivedOf(events: List<Event>) = self<Event>().apply {
        referenceId = events.first().referenceId
        metadata = Metadata()
            .derivedFromEventId(events.map { it.eventId }.toSet())
    }

    fun newReferenceId() = self<Event>().apply {
        referenceId = UUID.randomUUID()
    }

    fun usingReferenceId(refId: UUID) = self<Event>().apply {
        referenceId = refId
    }

    @VisibleForTesting
    internal fun withEventId(eventId: UUID) = self<Event>().apply {
        this.eventId = eventId
    }

    @VisibleForTesting
    internal fun withEventId(eventId: String) = self<Event>().apply {
        this.eventId = UUID.fromString(eventId)
    }

    @VisibleForTesting
    internal fun withMetadata(metadata: Metadata) = self<Event>().apply {
        this.metadata = metadata
    }
}

inline fun <reified T> Event.requireAs(): T {
    return this as? T ?: throw IllegalArgumentException("Expected ${T::class.java.name}, got ${this::class.java.name}")
}

abstract class DeleteEvent(
    open val deletedEventId: UUID
) : Event()

abstract class SignalEvent(): Event()


