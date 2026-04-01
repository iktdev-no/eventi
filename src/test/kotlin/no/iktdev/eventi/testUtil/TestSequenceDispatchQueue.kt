package no.iktdev.eventi.testUtil

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import no.iktdev.eventi.events.SequenceDispatchQueue
import no.iktdev.eventi.lifecycle.LifecycleStore

class TestSequenceDispatchQueue(
    maxConcurrency: Int,
    dispatcher: CoroutineDispatcher,
    lifecycleStore: LifecycleStore
) : SequenceDispatchQueue(
    maxConcurrency,
    CoroutineScope(dispatcher + SupervisorJob()),
    lifecycleStore
)
