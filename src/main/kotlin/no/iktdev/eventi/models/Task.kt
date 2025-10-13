package no.iktdev.eventi.models

import java.time.LocalDateTime
import java.util.UUID


abstract class Task {
    lateinit var referenceId: UUID
        protected set
    val taskId: UUID = UUID.randomUUID()
    var metadata: Metadata = Metadata()
        protected set


    protected open fun <T : Task> self(): T = this as T

    fun newReferenceId() = self<Task>().apply {
        referenceId = UUID.randomUUID()
    }

    fun derivedOf(event: Event) = self<Task>().apply {
        referenceId = event.referenceId
        metadata = Metadata().derivedFromEventId(event.eventId)
    }
}

inline fun <reified T> Task.requireAs(): T {
    return this as? T ?: throw IllegalArgumentException("Expected ${T::class.java.name}, got ${this::class.java.name}")
}