package no.iktdev.eventi.models

import java.util.UUID


abstract class Task {
    lateinit var referenceId: UUID
        protected set
    var taskId: UUID = UUID.randomUUID()
        protected set
    var metadata: Metadata = Metadata()
        protected set

    var state: TaskState = TaskState()
        internal set

    protected open fun <T : Task> self(): T = this as T

    fun newReferenceId() = self<Task>().apply {
        referenceId = UUID.randomUUID()
    }

    fun derivedOf(event: Event) = self<Task>().apply {
        if (!event.isReferenceIdInitialized())
            throw IllegalStateException("Incoming event does not contain a defined referenceId")
        referenceId = event.referenceId
        metadata = Metadata().derivedFromEventId(event.eventId)
    }

    internal fun reUseTaskId(taskId: UUID) = self<Task>().apply {
        this.taskId = taskId
    }

    fun usingReferenceId(refId: UUID) = self<Task>().apply {
        referenceId = refId
    }
}

inline fun <reified T> Task.requireAs(): T {
    return this as? T ?: throw IllegalArgumentException("Expected ${T::class.java.name}, got ${this::class.java.name}")
}