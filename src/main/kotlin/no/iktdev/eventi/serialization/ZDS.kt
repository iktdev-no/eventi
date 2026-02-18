package no.iktdev.eventi.serialization

import no.iktdev.eventi.MyTime
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.Task
import no.iktdev.eventi.models.store.PersistedEvent
import no.iktdev.eventi.models.store.PersistedTask
import no.iktdev.eventi.models.store.TaskStatus
import no.iktdev.eventi.registry.EventTypeRegistry
import no.iktdev.eventi.registry.TaskTypeRegistry
import java.time.Instant

object ZDS {
    private val gson = WGson.gson

    fun Event.toPersisted(id: Long, persistedAt: Instant = MyTime.utcNow()): PersistedEvent? {
        val payloadJson = gson.toJson(this)
        val eventName = this::class.simpleName ?: run {
            throw IllegalStateException("Missing class name for event: $this")
        }
        return PersistedEvent(
            id = id,
            referenceId = referenceId,
            eventId = eventId,
            event = eventName,
            data = payloadJson,
            persistedAt = persistedAt
        )
    }

    /**
     * Convert a PersistedEvent back to its original Event type using the event type registry and Gson for deserialization.
     */
    fun PersistedEvent.toEvent(): Event? {
        val clazz = EventTypeRegistry.resolve(event)
            ?: run {
                throw IllegalStateException("Missing class name for event: $this")
            }
        return gson.fromJson(data, clazz)
    }

    fun Task.toPersisted(id: Long, status: TaskStatus = TaskStatus.Pending, persistedAt: Instant = MyTime.utcNow()): PersistedTask? {
        val payloadJson = gson.toJson(this)
        val taskName = this::class.simpleName ?: run {
            throw IllegalStateException("Missing class name for task: $this")
        }
        return PersistedTask(
            id = id,
            referenceId = referenceId,
            taskId = taskId,
            task = taskName,
            data = payloadJson,
            status = status,
            claimed = false,
            consumed = false,
            claimedBy = null,
            lastCheckIn = null,
            persistedAt = persistedAt
        )
    }

    fun PersistedTask.toTask(): Task? {
        val clazz = TaskTypeRegistry.resolve(task)
            ?: run {
                //error("Unknown task type: $task")
                return null
            }
        return gson.fromJson(data, clazz)
    }


}