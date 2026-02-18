package no.iktdev.eventi.models

abstract class Progress {
    abstract val progress: Int
    abstract val message: String?
}

data class ProgressEnvelope(
    val type: String,
    val data: String
)
