package no.iktdev.eventi.models

import java.time.LocalDateTime
import java.util.UUID


abstract class Task {
    lateinit var referenceId: UUID
        protected set
    val taskId: UUID = UUID.randomUUID()
    var metadata: Metadata = Metadata()
        protected set

    fun newReferenceId() = apply {
        this.referenceId = UUID.randomUUID()
    }

    fun derivedOf(event: Event) = apply {
        this.referenceId = event.referenceId
        this.metadata = Metadata(derivedFromId = event.eventId)
    }
}

inline fun <reified T> Task.requireAs(): T {
    return this as? T ?: throw IllegalArgumentException("Expected ${T::class.java.name}, got ${this::class.java.name}")
}