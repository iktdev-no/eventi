package no.iktdev.eventi.stores

import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.store.PersistedEvent
import no.iktdev.eventi.serialization.ZDS.toEvent
import java.time.Instant
import java.util.UUID

interface EventStore {
    fun getPersistedEventsAfter(timestamp: Instant): List<PersistedEvent>
    fun getPersistedEventsFor(referenceId: UUID): List<PersistedEvent>
    fun persist(event: Event)
    fun getEventInSequence(referenceId: UUID, eventId: UUID): Event?
}

