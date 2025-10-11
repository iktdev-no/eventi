package no.iktdev.eventi.testUtil

import java.time.Duration


fun Duration.multiply(factor: Int): Duration {
    return Duration.ofNanos(this.toNanos() * factor)
}