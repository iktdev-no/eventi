package no.iktdev.eventi.stores

import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.store.PersistedEvent
import java.time.Instant
import java.util.UUID

interface EventStore {
    fun getPersistedEventsAfter(timestamp: Instant): List<PersistedEvent>
    fun getPersistedEventsFor(referenceId: UUID): List<PersistedEvent>
    fun persist(event: Event)
}

