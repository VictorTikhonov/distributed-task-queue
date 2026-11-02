package ru.demo.task

import org.slf4j.LoggerFactory
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
    private val logger = LoggerFactory.getLogger(TaskService::class.java)

    private val roundRobinCounter = AtomicInteger(0)

    @Volatile
    private var synchronizedTerm: Int = -1

    /**
     * Создаёт задачу на лидере,
     * назначает её доступному узлу через Round Robin
     * и реплицирует на остальные доступные узлы.
     */
    @Synchronized
    fun createTask(description: String): Task {
        require(description.isNotBlank()) {
            "Task description must not be blank"
        }

        check(raftNode.state == NodeState.LEADER) {
            "Only the leader can distribute tasks"
        }

        check(synchronizedTerm == raftNode.currentTerm) {
            "Leader is not ready: task synchronization is not completed"
        }

        // Определяем доступные узлы.
        val availableNodes = raftProperties.peers
            .filter { peer ->
                peer.id == raftNode.nodeId ||
                        taskPeerClient.isPeerAvailable(peer)
            }
            .map { it.id }
            .sorted()

        check(availableNodes.isNotEmpty()) {
            "No available nodes in the cluster"
        }

        // Round Robin только между доступными узлами.
        val index = Math.floorMod(
            roundRobinCounter.getAndIncrement(),
            availableNodes.size
        )

        val assignedNodeId = availableNodes[index]

        val task = Task(
            description = description,
            assignedNodeId = assignedNodeId
        )

        // Сначала сохраняем задачу локально.
        taskStore.save(task)

        // Реплицируем задачу на остальные доступные узлы.
        val peers = raftProperties.peers.filter {
            it.id != raftNode.nodeId
        }

        var successfulReplications = 0

        peers.forEach { peer ->
            try {
                taskPeerClient.sendTask(peer, task)
                successfulReplications++
            } catch (exception: Exception) {
                logger.warn(
                    "Node {}: Replication to node {} failed",
                    raftNode.nodeId,
                    peer.id
                )
            }
        }

        // Проверяем наличие большинства копий.
        val majority = raftProperties.peers.size / 2 + 1

        check(successfulReplications + 1 >= majority) {
            "Task was not replicated to a cluster majority"
        }

        logger.info(
            "Node {}: Task {} created. Assigned node={}, replicated to {}/{} peers",
            raftNode.nodeId,
            task.id,
            assignedNodeId,
            successfulReplications,
            peers.size
        )

        return task
    }

    /**
     * Вызывается после избрания нового лидера.
     * Собирает задачи с доступных узлов,
     * объединяет их и распространяет полный список.
     */
    @Synchronized
    fun synchronizeTasksFromPeers() {
        if (raftNode.state != NodeState.LEADER) {
            return
        }

        val synchronizationTerm = raftNode.currentTerm
        synchronizedTerm = -1

        logger.info(
            "Node {}: Starting task synchronization. Term={}",
            raftNode.nodeId,
            synchronizationTerm
        )

        val peers = raftProperties.peers.filter {
            it.id != raftNode.nodeId
        }

        val reachablePeers = mutableListOf<RaftProperties.Peer>()

        // Получаем задачи с доступных узлов.
        peers.forEach { peer ->
            try {
                val remoteTasks = taskPeerClient.getTasks(peer)

                taskStore.saveAll(remoteTasks)
                reachablePeers.add(peer)

                logger.info(
                    "Node {}: Received {} tasks from node {}",
                    raftNode.nodeId,
                    remoteTasks.size,
                    peer.id
                )
            } catch (exception: Exception) {
                logger.warn(
                    "Node {}: Node {} is unavailable during synchronization",
                    raftNode.nodeId,
                    peer.id
                )
            }
        }

        // Проверяем, что доступно большинство кластера.
        val majority = raftProperties.peers.size / 2 + 1

        if (reachablePeers.size + 1 < majority) {
            logger.error(
                "Node {}: Task synchronization failed. Available nodes={}, required majority={}",
                raftNode.nodeId,
                reachablePeers.size + 1,
                majority
            )
            return
        }

        val allTasks = taskStore.findAll()

        // Восстанавливаем позицию Round Robin.
        roundRobinCounter.set(allTasks.size)

        // Распространяем объединённый список задач
        // на все доступные узлы.
        reachablePeers.forEach { peer ->
            allTasks.forEach { task ->
                try {
                    taskPeerClient.sendTask(peer, task)
                } catch (exception: Exception) {
                    logger.error(
                        "Node {}: Failed to replicate task {} to node {}",
                        raftNode.nodeId,
                        task.id,
                        peer.id,
                        exception
                    )
                }
            }
        }

        // Проверяем, что лидер не изменился во время синхронизации.
        if (
            raftNode.state == NodeState.LEADER &&
            raftNode.currentTerm == synchronizationTerm
        ) {
            synchronizedTerm = synchronizationTerm

            logger.info(
                "Node {}: Task synchronization completed. Total tasks={}, reachable peers={}/{}",
                raftNode.nodeId,
                allTasks.size,
                reachablePeers.size,
                peers.size
            )
        } else {
            logger.warn(
                "Node {}: Leadership changed during task synchronization. Term={}",
                raftNode.nodeId,
                synchronizationTerm
            )
        }
    }

    fun getTasksByNode(nodeId: Int): List<Task> {
        return taskStore.findAll()
            .filter { it.assignedNodeId == nodeId }
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
