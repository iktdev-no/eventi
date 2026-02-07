package no.iktdev.eventi.events

import no.iktdev.eventi.models.Event

sealed class SoftDispatchException(
    message: String,
    val eventType: Class<out Event>? = null
) : RuntimeException(message) {

    class MissingEventException(
        missingType: Class<out Event>
    ) : SoftDispatchException(
        message = "Missing required event: ${missingType.simpleName}",
        eventType = missingType
    )

    class SkipListenerException(
        reason: String,
        currentEvent: Class<out Event>
    ) : SoftDispatchException(
        message = "Listener skipped: $reason",
        eventType = currentEvent
    )

    class ForcedListenerEjectionException(
        reason: String,
        currentEvent: Class<out Event>
    ) : SoftDispatchException(
        message = "Listener forcibly ejected: $reason",
        eventType = currentEvent
    )
}
