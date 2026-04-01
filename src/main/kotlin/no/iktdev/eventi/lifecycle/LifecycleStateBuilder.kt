package no.iktdev.eventi.lifecycle

import no.iktdev.eventi.MyTime
import no.iktdev.eventi.events.EventPollerImplementation
import no.iktdev.eventi.events.SequenceDispatchQueue
import no.iktdev.eventi.tasks.TaskPollerImplementation
import no.iktdev.eventi.stores.TaskStore
import java.time.Instant

class LifecycleStateBuilder(
    private val eventPoller: EventPollerImplementation,
    private val taskPoller: TaskPollerImplementation
) {
    suspend fun build(): CurrentState {
        val eventState = eventPoller.eventState()
        val taskState = taskPoller.taskState()

        return CurrentState(
            lastSeenTime = eventState.lastSeenTime,
            backoffMillis = eventState.backoffMillis,
            nextPollExpectedAt = eventState.nextPollExpectedAt,
            activeRefs = eventState.activeRefs,
            refStates = eventState.refStates,

            taskBackoffMillis = taskState.taskBackoffMillis,
            nextTaskPollExpectedAt = taskState.nextTaskPollExpectedAt,
            pendingTasks = taskState.pendingTasks,
            taskStates = taskState.taskStates
        )
    }
}

