package ru.demo.task

import org.springframework.stereotype.Service
import ru.demo.raft.NodeState
import ru.demo.raft.RaftNode
import ru.demo.raft.RaftProperties
import ru.demo.task.dto.TaskResponse
import java.util.concurrent.atomic.AtomicInteger

@Service
class TaskService(
    private val taskStore: TaskStore,
    private val raftNode: RaftNode,
    private val raftProperties: RaftProperties,
    private val taskPeerClient: TaskPeerClient
) {

    private val roundRobinCounter = AtomicInteger(0)

    fun createTask(description: String): Task {
        require(description.isNotBlank()) {
            "Task description must not be blank"
        }

        check(raftNode.state == NodeState.LEADER) {
            "Only the leader can distribute tasks"
        }

        val availableNodes = raftProperties.peers
            .map { it.id }
            .sorted()

        check(availableNodes.isNotEmpty()) {
            "No nodes configured in the cluster"
        }

        val index = Math.floorMod(
            roundRobinCounter.getAndIncrement(),
            availableNodes.size
        )

        val assignedNodeId = availableNodes[index]

        val task = Task(
            description = description,
            assignedNodeId = assignedNodeId
        )

        if (assignedNodeId == raftNode.nodeId) {
            taskStore.save(task)
        } else {
            val peer = raftProperties.peers.firstOrNull {
                it.id == assignedNodeId
            } ?: throw IllegalStateException(
                "Node $assignedNodeId not found in cluster configuration"
            )

            taskPeerClient.sendTask(peer, task)
        }

        return task
    }

    fun getAllTasks(): List<Task> {
        return taskStore.findAll()
    }

    fun getTasksByStatus(status: TaskStatus): List<Task> {
        return taskStore.findAllByStatus(status)
    }

    fun toResponse(task: Task): TaskResponse {
        return TaskResponse(
            id = task.id,
            description = task.description,
            status = task.status,
            assignedNodeId = task.assignedNodeId,
            createdAt = task.createdAt
        )
    }
}