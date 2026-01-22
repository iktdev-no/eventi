package no.iktdev.eventi

import java.time.Clock
import java.time.LocalDateTime

object MyTime {
    private val clock: Clock = Clock.systemUTC()

    @JvmStatic
    fun UtcNow(): LocalDateTime =
        LocalDateTime.now(clock)
}
