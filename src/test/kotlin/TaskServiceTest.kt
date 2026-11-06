
package ru.demo.task

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.slf4j.LoggerFactory
import ru.demo.raft.RaftNode
import ru.demo.raft.RaftProperties

class TaskServiceTest {

    private val logger = LoggerFactory.getLogger(TaskServiceTest::class.java)

    private data class Fixture(
        val service: TaskService,
        val store: TaskStore,
        val node: RaftNode,
        val peerClient: TaskPeerClient,
        val peer2: RaftProperties.Peer,
        val peer3: RaftProperties.Peer
    )

    private fun createFixture(
        becomeLeader: Boolean = true,
        synchronize: Boolean = true
    ): Fixture {
        val node = RaftNode(
            nodeId = 1,
            electionTimeoutMin = 10_000,
            electionTimeoutMax = 10_000
        )

        if (becomeLeader) {
            val term = node.startElection()
            node.addVote(voterId = 2, electionTerm = term)
            node.becomeLeader()
        }

        val peer1 = RaftProperties.Peer(
            id = 1,
            url = "http://localhost:8081"
        )
        val peer2 = RaftProperties.Peer(
            id = 2,
            url = "http://localhost:8082"
        )
        val peer3 = RaftProperties.Peer(
            id = 3,
            url = "http://localhost:8083"
        )

        val properties = RaftProperties(
            nodeId = 1,
            peers = listOf(peer1, peer2, peer3)
        )

        val store = TaskStore()
        val peerClient = Mockito.mock(TaskPeerClient::class.java)

        `when`(peerClient.isPeerAvailable(peer2)).thenReturn(true)
        `when`(peerClient.isPeerAvailable(peer3)).thenReturn(true)

        `when`(peerClient.getTasks(peer2)).thenReturn(emptyList())
        `when`(peerClient.getTasks(peer3)).thenReturn(emptyList())

        val service = TaskService(
            taskStore = store,
            raftNode = node,
            raftProperties = properties,
            taskPeerClient = peerClient
        )

        if (becomeLeader && synchronize) {
            service.synchronizeTasksFromPeers()
        }

        return Fixture(
            service = service,
            store = store,
            node = node,
            peerClient = peerClient,
            peer2 = peer2,
            peer3 = peer3
        )
    }

    @Test
    fun `leader successfully creates task`() {
        logger.info("TEST START: leader successfully creates task")

        val fixture = createFixture()

        val task = fixture.service.createTask("Test task")

        assertEquals("Test task", task.description)
        assertEquals(TaskStatus.PENDING, task.status)
        assertEquals(1, task.assignedNodeId)
        assertNotNull(fixture.store.findById(task.id))

        logger.info(
            "TEST PASSED: task created. taskId={}, assignedNode={}",
            task.id,
            task.assignedNodeId
        )
    }

    @Test
    fun `follower cannot create task`() {
        logger.info("TEST START: follower cannot create task")

        val fixture = createFixture(
            becomeLeader = false,
            synchronize = false
        )

        assertThrows(IllegalStateException::class.java) {
            fixture.service.createTask("Test task")
        }

        assertTrue(fixture.store.findAll().isEmpty())

        logger.info("TEST PASSED: follower creation rejected")
    }

    @Test
    fun `leader cannot create task before synchronization`() {
        logger.info("TEST START: leader cannot create task before synchronization")

        val fixture = createFixture(synchronize = false)

        assertThrows(IllegalStateException::class.java) {
            fixture.service.createTask("Test task")
        }

        assertTrue(fixture.store.findAll().isEmpty())

        logger.info("TEST PASSED: task creation blocked before synchronization")
    }

    @Test
    fun `task description cannot be blank`() {
        logger.info("TEST START: task description cannot be blank")

        val fixture = createFixture()

        assertThrows(IllegalArgumentException::class.java) {
            fixture.service.createTask("   ")
        }

        assertTrue(fixture.store.findAll().isEmpty())

        logger.info("TEST PASSED: blank task description rejected")
    }

    @Test
    fun `round robin distributes tasks between three nodes`() {
        logger.info("TEST START: round robin distributes tasks between three nodes")

        val fixture = createFixture()

        val first = fixture.service.createTask("Task 1")
        val second = fixture.service.createTask("Task 2")
        val third = fixture.service.createTask("Task 3")

        assertEquals(1, first.assignedNodeId)
        assertEquals(2, second.assignedNodeId)
        assertEquals(3, third.assignedNodeId)

        logger.info(
            "TEST PASSED: round robin assignments={}",
            listOf(
                first.assignedNodeId,
                second.assignedNodeId,
                third.assignedNodeId
            )
        )
    }

    @Test
    fun `created task is replicated to other nodes`() {
        logger.info("TEST START: created task is replicated to other nodes")

        val fixture = createFixture()

        val task = fixture.service.createTask("Replication test")

        verify(fixture.peerClient, times(1))
            .sendTask(fixture.peer2, task)

        verify(fixture.peerClient, times(1))
            .sendTask(fixture.peer3, task)

        logger.info(
            "TEST PASSED: task {} replicated to nodes 2 and 3",
            task.id
        )
    }

    @Test
    fun `get all tasks returns locally stored tasks`() {
        logger.info("TEST START: get all tasks returns locally stored tasks")

        val fixture = createFixture()

        fixture.service.createTask("Task 1")
        fixture.service.createTask("Task 2")

        val tasks = fixture.service.getAllTasks()

        assertEquals(2, tasks.size)

        logger.info("TEST PASSED: retrieved {} tasks", tasks.size)
    }

    @Test
    fun `get tasks by node returns only assigned tasks`() {
        logger.info("TEST START: get tasks by node returns only assigned tasks")

        val fixture = createFixture()

        fixture.service.createTask("Task 1")
        fixture.service.createTask("Task 2")
        fixture.service.createTask("Task 3")

        val tasks = fixture.service.getTasksByNode(2)

        assertEquals(1, tasks.size)
        assertTrue(tasks.all { it.assignedNodeId == 2 })

        logger.info(
            "TEST PASSED: retrieved {} tasks assigned to node 2",
            tasks.size
        )
    }

    @Test
    fun `get tasks by status returns tasks with requested status`() {
        logger.info("TEST START: get tasks by status returns requested status")

        val fixture = createFixture()

        val task = fixture.service.createTask("Task 1")
        fixture.store.update(
            task.copy(status = TaskStatus.COMPLETED)
        )

        val completedTasks =
            fixture.service.getTasksByStatus(TaskStatus.COMPLETED)

        assertEquals(1, completedTasks.size)
        assertEquals(TaskStatus.COMPLETED, completedTasks.first().status)

        logger.info(
            "TEST PASSED: retrieved {} completed tasks",
            completedTasks.size
        )
    }

    @Test
    fun `synchronization collects tasks from peers`() {
        logger.info("TEST START: synchronization collects tasks from peers")

        val node = RaftNode(1, 10_000, 10_000)
        val term = node.startElection()
        node.addVote(2, term)
        node.becomeLeader()

        val peer1 = RaftProperties.Peer(1, "http://localhost:8081")
        val peer2 = RaftProperties.Peer(2, "http://localhost:8082")
        val peer3 = RaftProperties.Peer(3, "http://localhost:8083")

        val remoteTask2 = Task(
            description = "Task from node 2",
            assignedNodeId = 2
        )
        val remoteTask3 = Task(
            description = "Task from node 3",
            assignedNodeId = 3
        )

        val peerClient = Mockito.mock(TaskPeerClient::class.java)

        `when`(peerClient.getTasks(peer2))
            .thenReturn(listOf(remoteTask2))

        `when`(peerClient.getTasks(peer3))
            .thenReturn(listOf(remoteTask3))

        val store = TaskStore()
        val service = TaskService(
            taskStore = store,
            raftNode = node,
            raftProperties = RaftProperties(
                nodeId = 1,
                peers = listOf(peer1, peer2, peer3)
            ),
            taskPeerClient = peerClient
        )

        service.synchronizeTasksFromPeers()

        val tasks = service.getAllTasks()

        assertEquals(2, tasks.size)
        assertTrue(tasks.any { it.id == remoteTask2.id })
        assertTrue(tasks.any { it.id == remoteTask3.id })

        verify(peerClient).sendTask(peer2, remoteTask2)
        verify(peerClient).sendTask(peer2, remoteTask3)
        verify(peerClient).sendTask(peer3, remoteTask2)
        verify(peerClient).sendTask(peer3, remoteTask3)

        logger.info(
            "TEST PASSED: synchronization collected {} tasks and replicated them to peers",
            tasks.size
        )
    }
}
