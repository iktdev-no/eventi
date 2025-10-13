package no.iktdev.eventi.models

import java.util.UUID

@Suppress("UNCHECKED_CAST")
abstract class Event {
    var referenceId: UUID = UUID.randomUUID()
        protected set
    var eventId: UUID = UUID.randomUUID()
        private set
    var metadata: Metadata = Metadata()
        protected set

    protected open fun <T : Event> self(): T = this as T

    fun producedFrom(task: Task): Event = self<Event>().apply {
        referenceId = task.referenceId
        val derivedIds = task.metadata.derivedFromId ?: emptySet()
        metadata = Metadata().derivedFromEventId(derivedIds)
    }

    fun derivedOf(vararg event: Event) = self<Event>().apply {
        referenceId = event.first().referenceId
        metadata = Metadata().derivedFromEventId(*event.map { it.eventId }.toTypedArray())
    }

    fun newReferenceId() = self<Event>().apply {
        referenceId = UUID.randomUUID()
    }

    fun usingReferenceId(refId: UUID) = self<Event>().apply {
        referenceId = refId
    }
}

inline fun <reified T> Event.requireAs(): T {
    return this as? T ?: throw IllegalArgumentException("Expected ${T::class.java.name}, got ${this::class.java.name}")
}

abstract class DeleteEvent: Event() {
    open lateinit var deletedEventId: UUID
}



