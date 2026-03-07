package no.iktdev.eventi.events

import no.iktdev.eventi.models.Event
import java.util.UUID

sealed class HardDispatchException(
    message: String,
    val event: Event? = null,
    val listener: EventListener? = null
) : Exception(message) {

    class IllegalDerivationException(
        parentId: UUID,
        producedEvent: Event,
        listener: EventListener
    ) : HardDispatchException(
        "Illegal derivation in listener ${listener::class.java.name}: " +
                "Event ${producedEvent::class.java.name} (${producedEvent.eventId}) " +
                "claims to be derived from $parentId, but that event is not present in the " +
                "current dispatch input and historical derivation is not allowed.",
        event = producedEvent,
        listener = listener
    )
}
