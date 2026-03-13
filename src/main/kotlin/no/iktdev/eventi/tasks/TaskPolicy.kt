package no.iktdev.eventi.tasks

import mu.KotlinLogging
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

abstract class TaskPolicy {
    private val log = KotlinLogging.logger {}

    abstract fun heartbeatInterval(): Duration
    abstract fun abandonTimeout(): Duration
    abstract fun cleanupInterval(): Duration

    // Minstegrenser
    open fun minHeartbeat() = 5.seconds
    open fun minTimeout() = 1.minutes
    open fun minCleanup() = 5.minutes

    fun validate(): PolicyValidationResult {
        val hb = heartbeatInterval()
        val timeout = abandonTimeout()
        val cleanup = cleanupInterval()

        val errors = mutableListOf<String>()
        val warnings = mutableListOf<String>()

        // Hard errors
        if (hb < minHeartbeat())
            errors += "Heartbeat interval $hb is below minimum ${minHeartbeat()}"

        if (timeout < minTimeout())
            errors += "Abandon timeout $timeout is below minimum ${minTimeout()}"

        if (hb >= timeout)
            errors += "Heartbeat interval $hb must be smaller than abandon timeout $timeout"

        if (cleanup < minCleanup())
            errors += "Cleanup interval $cleanup is below minimum ${minCleanup()}"

        // Soft warnings
        if (hb < 30.seconds)
            warnings += "Heartbeat interval $hb is very low and may cause unnecessary load."

        if (timeout < 3.minutes)
            warnings += "Abandon timeout $timeout is very low and may release active tasks prematurely."

        if (cleanup < 10.minutes)
            warnings += "Cleanup interval $cleanup is very low and may delete files too early."

        // Log warnings (optional)
        warnings.forEach { log.warn(it) }

        return PolicyValidationResult(
            isValid = errors.isEmpty(),
            errors = errors,
            warnings = warnings
        )
    }


}
