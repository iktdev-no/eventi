package no.iktdev.eventi.models.store

import java.time.Instant
import java.util.UUID

data class PersistedEvent(
    val id: Long,
    val referenceId: UUID,
    val eventId: UUID,
    val event: String,
    val data: String,
    val persistedAt: Instant
)