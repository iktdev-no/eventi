package no.iktdev.eventi.models.diagnostics

import no.iktdev.eventi.events.EventListener
import no.iktdev.eventi.models.Event

enum class LogType {
    Accepted,
    Rejected,
    Ignored,
    Skipped,
    Error
}

private const val RESET = "\u001B[0m"
private const val RED = "\u001B[31m"
private const val GREEN = "\u001B[32m"
private const val YELLOW = "\u001B[33m"
private const val BLUE = "\u001B[34m"
private const val CYAN = "\u001B[36m"
private const val MAGENTA = "\u001B[35m"


fun colorFor(type: LogType): String = when (type) {
    LogType.Accepted -> GREEN
    LogType.Ignored -> YELLOW
    LogType.Skipped -> CYAN
    LogType.Rejected -> RED
    LogType.Error -> MAGENTA
}


data class DispatchLog(
    val event: Event,
    val listener: EventListener,
    val type: LogType,
    val message: String? = null
) {
    fun getTypeStyled(): String {
        val color = colorFor(type)
        return "$color[${type.name}]$RESET"
    }

}

fun MutableList<DispatchLog>.accept(event: Event, listener: EventListener) {
    this.add(DispatchLog(event, listener, LogType.Accepted))
}

fun MutableList<DispatchLog>.ignored(event: Event, listener: EventListener, message: String? = null) {
    this.add(DispatchLog(event, listener, LogType.Ignored, message))
}

fun MutableList<DispatchLog>.rejected(event: Event, listener: EventListener, message: String? = null) {
    this.add(DispatchLog(event, listener, LogType.Rejected, message))
}
fun MutableList<DispatchLog>.skipped(event: Event, listener: EventListener, message: String? = null) {
    this.add(DispatchLog(event, listener, LogType.Skipped, message))
}
fun MutableList<DispatchLog>.error(event: Event, listener: EventListener, message: String? = null) {
    this.add(DispatchLog(event, listener, LogType.Error, message))
}

