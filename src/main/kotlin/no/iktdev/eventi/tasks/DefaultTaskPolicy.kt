package no.iktdev.eventi.tasks

import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.minutes

object DefaultTaskPolicy : TaskPolicy() {
    override fun heartbeatInterval(): Duration = 5.minutes
    override fun abandonTimeout(): Duration = 10.minutes
    override fun cleanupInterval(): Duration = 7.days
}