package no.iktdev.eventi

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class ListenerOrder(val value: Int)
