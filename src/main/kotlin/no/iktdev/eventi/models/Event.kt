package no.iktdev.eventi.models

import java.time.LocalDateTime
import java.util.UUID

abstract class Event {
    var referenceId: UUID = UUID.randomUUID()
        protected set
    var eventId: UUID = UUID.randomUUID()
        private set
    var metadata: Metadata = Metadata()
        protected set

    @Transient
    open val data: Any? = null

    fun derivedOf(event: Event) = apply {
        this.referenceId = event.referenceId
        this.metadata = Metadata(derivedFromId = event.eventId)
    }

}

abstract class DeleteEvent: Event() {
    open lateinit var deletedEventId: UUID
}


open class Metadata(
    val created: LocalDateTime = LocalDateTime.now(), val derivedFromId: UUID? = null
) {}


