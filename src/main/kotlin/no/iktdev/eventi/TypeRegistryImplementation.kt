package no.iktdev.eventi

abstract class TypeRegistryImplementation<T> {
    protected open val types = mutableMapOf<String, Class<out T>>()

    open fun register(clazz: Class<out T>) {
        types[clazz.simpleName] = clazz
    }
    open fun register(clazzes: List<Class<out T>>) {
        clazzes.forEach { clazz ->
            types[clazz.simpleName] = clazz
        }
    }

    open fun resolve(name: String): Class<out T>? = types[name]

    open fun all(): Map<String, Class<out T>> = types.toMap()
}