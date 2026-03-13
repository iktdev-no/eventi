package no.iktdev.eventi.tasks

data class PolicyValidationResult(
    val isValid: Boolean,
    val errors: List<String>,
    val warnings: List<String>
)
