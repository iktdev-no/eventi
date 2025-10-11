package no.iktdev.eventi.models

import java.util.UUID

abstract class Event {
    var referenceId: UUID = UUID.randomUUID()
        protected set
    var eventId: UUID = UUID.randomUUID()
        private set
    var metadata: Metadata = Metadata()
        protected set

    fun derivedOf(event: Event) = apply {
        this.referenceId = event.referenceId
        this.metadata = Metadata(derivedFromId = event.eventId)
    }

    fun producedFrom(task: Task) = apply {
        this.referenceId = task.referenceId
        this.metadata = Metadata(derivedFromId = task.taskId)
    }

    fun newReferenceId() = apply {
        this.referenceId = UUID.randomUUID()
    }

    fun usingReferenceId(refId: UUID) = apply {
        this.referenceId = refId
    }

}

abstract class DeleteEvent: Event() {
    open lateinit var deletedEventId: UUID
}


