package no.iktdev.eventi

import java.time.Clock
import java.time.Instant

object MyTime {
    private val clock: Clock = Clock.systemUTC()

    @JvmStatic
    fun utcNow(): Instant =
        Instant.now(clock)

}

