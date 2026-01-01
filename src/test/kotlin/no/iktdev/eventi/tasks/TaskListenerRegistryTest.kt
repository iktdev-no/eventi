package no.iktdev.eventi.tasks

import no.iktdev.eventi.ListenerOrder
import no.iktdev.eventi.events.EventListener
import no.iktdev.eventi.events.EventListenerRegistry
import no.iktdev.eventi.models.Event
import no.iktdev.eventi.models.Task
import no.iktdev.eventi.testUtil.wipe
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class TaskListenerRegistryTest {


    @ListenerOrder(1)
    class MockTest1() : TaskListener() {
        override fun getWorkerId(): String {
            TODO("Not yet implemented")
        }
        override fun supports(task: Task): Boolean {
            TODO("Not yet implemented")
        }
        override suspend fun onTask(task: Task): Event? {
            TODO("Not yet implemented")
        }
    }

    @ListenerOrder(2)
    class MockTest2() : TaskListener() {
        override fun getWorkerId(): String {
            TODO("Not yet implemented")
        }
        override fun supports(task: Task): Boolean {
            TODO("Not yet implemented")
        }
        override suspend fun onTask(task: Task): Event? {
            TODO("Not yet implemented")
        }
    }

    @ListenerOrder(3)
    class MockTest3() : TaskListener() {
        override fun getWorkerId(): String {
            TODO("Not yet implemented")
        }
        override fun supports(task: Task): Boolean {
            TODO("Not yet implemented")
        }
        override suspend fun onTask(task: Task): Event? {
            TODO("Not yet implemented")
        }
    }

    class MockTestRandom() : TaskListener() {
        override fun getWorkerId(): String {
            TODO("Not yet implemented")
        }
        override fun supports(task: Task): Boolean {
            TODO("Not yet implemented")
        }
        override suspend fun onTask(task: Task): Event? {
            TODO("Not yet implemented")
        }
    }

    @BeforeEach
    fun clear() {
        TaskListenerRegistry.wipe()
    }

    @Test
    fun validateOrder() {
        MockTestRandom()
        MockTest1()
        MockTest2()
        MockTest3()
        val listeners = TaskListenerRegistry.getListeners()
        // Assert
        assertThat(listeners.map { it::class.simpleName }).containsExactly(
            MockTest1::class.simpleName, // @ListenerOrder(1)
            MockTest2::class.simpleName, // @ListenerOrder(2)
            MockTest3::class.simpleName, // @ListenerOrder(3)
            MockTestRandom::class.simpleName // no annotation → goes last
        )
    }


}