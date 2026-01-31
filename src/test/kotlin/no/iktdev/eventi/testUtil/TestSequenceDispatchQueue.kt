package no.iktdev.eventi.testUtil

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import no.iktdev.eventi.events.SequenceDispatchQueue

class TestSequenceDispatchQueue(
    maxConcurrency: Int,
    dispatcher: CoroutineDispatcher
) : SequenceDispatchQueue(
    maxConcurrency,
    CoroutineScope(dispatcher + SupervisorJob())
)
