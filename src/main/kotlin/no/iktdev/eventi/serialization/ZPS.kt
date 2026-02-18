package no.iktdev.eventi.serialization

import com.google.gson.GsonBuilder
import no.iktdev.eventi.models.ProgressEnvelope
import no.iktdev.eventi.models.Progress
import no.iktdev.eventi.registry.ProgressTypeRegistry

object ZPS {
    private val gson = WGson.gson

    fun Progress.toEnvelope(): ProgressEnvelope {
        val json = gson.toJson(this)
        val typeName = this::class.simpleName
            ?: error("Missing class name for progress payload: $this")

        return ProgressEnvelope(
            type = typeName,
            data = json
        )
    }


    fun ProgressEnvelope.toProgress(): Progress {
        val clazz = ProgressTypeRegistry.resolve(type)
            ?: error("Unknown progress payload type: $type")

        return gson.fromJson(data, clazz)
    }

    fun Progress.toJsonEnvelope(): String =
        gson.toJson(this.toEnvelope())

    inline fun <reified T : Progress> Progress.requireAs(): T {
        return this as? T
            ?: error("Expected ${T::class.java.name}, got ${this::class.java.name}")
    }

}
