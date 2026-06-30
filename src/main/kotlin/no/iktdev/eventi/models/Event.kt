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

    open fun derivedOf(vararg events: Event) =
        derivedOf(events.toList())


    open fun derivedOf(events: List<Event>) = self<Event>().apply {
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


    internal fun hasReferenceIdBeenSet(): Boolean {
        return this::referenceId.isInitialized
    }
}




inline fun <reified T> Event.requireAs(): T {
    return this as? T ?: throw IllegalArgumentException("Expected ${T::class.java.name}, got ${this::class.java.name}")
}

open class DeleteEvent(
    open val deletedEventId: UUID
) : Event() {
    /**
     * Does only assign referenceId, no real difference between this and "usingReferenceId"
     */
    override fun derivedOf(vararg events: Event): Event {
        this.referenceId = events.first().referenceId
        return this
    }

    /**
     * Does only assign referenceId, no real difference between this and "usingReferenceId"
     */
    override fun derivedOf(events: List<Event>): Event {
        this.referenceId = events.first().referenceId
        return this
    }
}

abstract class SignalEvent(): Event()

abstract class TaskCratedEvent(): Event()
abstract class SingleTaskCratedEvent(val taskId: UUID): TaskCratedEvent() {
    override fun derivedOf(vararg events: Event): SingleTaskCratedEvent {
        this.referenceId = events.first().referenceId
        this.metadata = Metadata()
            .derivedFromEventId(events.map { it.eventId }.toSet())
        return this
    }

    override fun derivedOf(events: List<Event>): SingleTaskCratedEvent =
        derivedOf(*events.toTypedArray())
}

data class MultiTaskIdentity(val taskId: UUID, val identity: String)
abstract class MultiTaskCreatedEvent(val taskIds: Set<MultiTaskIdentity>): TaskCratedEvent() {
    override fun derivedOf(vararg events: Event): MultiTaskCreatedEvent {
        this.referenceId = events.first().referenceId
        this.metadata = Metadata()
            .derivedFromEventId(events.map { it.eventId }.toSet())
        return this
    }

    override fun derivedOf(events: List<Event>): MultiTaskCreatedEvent =
        derivedOf(*events.toTypedArray())
}