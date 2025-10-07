package no.iktdev.eventi.stores

import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.store.PersistedEvent
import java.time.LocalDateTime
import java.util.UUID

interface EventStore {
    fun getPersistedEventsAfter(timestamp: LocalDateTime): List<PersistedEvent>
    fun getPersistedEventsFor(referenceId: UUID): List<PersistedEvent>
    fun save(event: Event)
}

