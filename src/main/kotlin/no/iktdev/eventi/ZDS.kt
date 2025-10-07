package no.iktdev.eventi

import com.google.gson.GsonBuilder
import com.google.gson.JsonDeserializationContext
import com.google.gson.JsonDeserializer
import com.google.gson.JsonElement
import com.google.gson.JsonPrimitive
import com.google.gson.JsonSerializationContext
import com.google.gson.JsonSerializer
import no.iktdev.eventi.events.EventTypeRegistry
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.store.PersistedEvent
import java.lang.reflect.Type
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object ZDS {
    val gson = WGson.gson

    fun Event.toPersisted(id: Long, persistedAt: LocalDateTime): PersistedEvent {
        val payloadJson = gson.toJson(this)
        val eventName = this::class.simpleName ?: error("Missing class name")
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
    fun PersistedEvent.toEvent(): Event {
        val clazz = EventTypeRegistry.resolve(event)
            ?: error("Unknown event type: $event")
        return gson.fromJson(data, clazz)
    }


    object WGson {
        val gson = GsonBuilder()
            .registerTypeAdapter(LocalDateTime::class.java, LocalDateTimeAdapter())
            .create()
        fun toJson(data: Any?): String {
            return gson.toJson(data)
        }

        class LocalDateTimeAdapter : JsonSerializer<LocalDateTime>, JsonDeserializer<LocalDateTime> {
            private val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME

            override fun serialize(
                src: LocalDateTime, typeOfSrc: Type, context: JsonSerializationContext
            ): JsonElement {
                return JsonPrimitive(src.format(formatter))
            }

            override fun deserialize(
                json: JsonElement, typeOfT: Type, context: JsonDeserializationContext
            ): LocalDateTime {
                return LocalDateTime.parse(json.asString, formatter)
            }
        }
    }
}