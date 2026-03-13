package no.iktdev.eventi.tasks

object GlobalTaskPolicy {
    private var _policy: TaskPolicy? = null

    val policy: TaskPolicy
        get() = _policy ?: DefaultTaskPolicy

    fun set(policy: TaskPolicy): PolicyValidationResult {
        val result = policy.validate()

        if (!result.isValid) {
            // Ikke sett policyen hvis den er ugyldig
            return result
        }

        _policy = policy
        return result
    }
}
