package no.iktdev.eventi.events

import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.Metadata
import java.util.UUID

class StartEvent(): Event() {
}

class EchoEvent(var data: String): Event() {
}

class MarcoEvent(val data: Boolean): Event() {
}