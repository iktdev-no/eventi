package no.iktdev.eventi.models

import no.iktdev.eventi.MyTime
import java.time.Instant
import java.util.UUID

class Metadata {
    val created: Instant = MyTime.utcNow()
    var derivedFromId: Set<UUID>? = null
        private set
    fun derivedFromEventId(vararg id: UUID) = apply {
        derivedFromId = id.toSet()
    }
    fun derivedFromEventId(ids: Set<UUID>) = apply {
        derivedFromId = ids
    }
}
