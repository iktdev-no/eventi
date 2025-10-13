package no.iktdev.eventi.models

import java.time.LocalDateTime
import java.util.UUID

class Metadata {
    val created: LocalDateTime = LocalDateTime.now()
    var derivedFromId: Set<UUID>? = null
        private set
    fun derivedFromEventId(vararg id: UUID) = apply {
        derivedFromId = id.toSet()
    }
}
