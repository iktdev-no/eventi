package no.iktdev.eventi.models

import java.time.LocalDateTime
import java.util.UUID

open class Metadata(
    val created: LocalDateTime = LocalDateTime.now(),
    val derivedFromId: UUID? = null
) {}
