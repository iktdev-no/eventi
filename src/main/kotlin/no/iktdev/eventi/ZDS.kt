package no.iktdev.eventi

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonDeserializationContext
import com.google.gson.JsonDeserializer
import com.google.gson.JsonElement
import com.google.gson.JsonPrimitive
import com.google.gson.JsonSerializationContext
import com.google.gson.JsonSerializer
import no.iktdev.eventi.events.EventTypeRegistry
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.Task
import no.iktdev.eventi.models.store.PersistedEvent
import no.iktdev.eventi.models.store.PersistedTask
import no.iktdev.eventi.models.store.TaskStatus
import no.iktdev.eventi.tasks.TaskTypeRegistry
import java.lang.reflect.Type
import java.time.Instant
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object ZDS {
    val gson = WGson.gson

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



    object WGson {
        val gson = GsonBuilder()
            .registerTypeAdapter(Instant::class.java, InstantAdapter())
            // hvis du fortsatt har LocalDateTime et sted:
            .registerTypeAdapter(LocalDateTime::class.java, LocalDateTimeAdapter())
            .create()

        fun toJson(data: Any?): String =
            gson.toJson(data)

        class InstantAdapter : JsonSerializer<Instant>, JsonDeserializer<Instant> {
            override fun serialize(
                src: Instant,
                typeOfSrc: Type,
                context: JsonSerializationContext
            ): JsonElement =
                JsonPrimitive(src.toString()) // ISO-8601, UTC

            override fun deserialize(
                json: JsonElement,
                typeOfT: Type,
                context: JsonDeserializationContext
            ): Instant =
                Instant.parse(json.asString)
        }

        class LocalDateTimeAdapter : JsonSerializer<LocalDateTime>, JsonDeserializer<LocalDateTime> {
            private val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME

            override fun serialize(
                src: LocalDateTime,
                typeOfSrc: Type,
                context: JsonSerializationContext
            ): JsonElement =
                JsonPrimitive(src.format(formatter))

            override fun deserialize(
                json: JsonElement,
                typeOfT: Type,
                context: JsonDeserializationContext
            ): LocalDateTime =
                LocalDateTime.parse(json.asString, formatter)
        }
    }

}