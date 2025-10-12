package no.iktdev.eventi

import no.iktdev.eventi.ZDS.toPersisted
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.store.PersistedEvent
import no.iktdev.eventi.stores.EventStore
import java.time.LocalDateTime
import java.util.UUID

class InMemoryEventStore : EventStore {
    private val persisted = mutableListOf<PersistedEvent>()
    private var nextId = 1L

    override fun getPersistedEventsAfter(timestamp: LocalDateTime): List<PersistedEvent> =
        persisted.filter { it.persistedAt > timestamp }

    override fun getPersistedEventsFor(referenceId: UUID): List<PersistedEvent> =
        persisted.filter { it.referenceId == referenceId }

    override fun persist(event: Event) {
        val persistedEvent = event.toPersisted(nextId++, LocalDateTime.now())
        persisted += persistedEvent!!
    }

    fun all(): List<PersistedEvent> = persisted
    fun clear() { persisted.clear(); nextId = 1L }
}
