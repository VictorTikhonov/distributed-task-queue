package ru.demo.task

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.web.client.RestClient
import ru.demo.raft.RaftProperties
import ru.demo.task.dto.InternalTaskRequest

@Component
class TaskPeerClient(
    builder: RestClient.Builder
) {
    private val logger = LoggerFactory.getLogger(TaskPeerClient::class.java)
    private val client = builder.build()

    fun sendTask(
        peer: RaftProperties.Peer,
        task: Task
    ) {
        val request = InternalTaskRequest(
            id = task.id,
            description = task.description,
            status = task.status,
            assignedNodeId = task.assignedNodeId,
            createdAt = task.createdAt
        )

        try {
            client.post()
                .uri("${peer.url}/internal/tasks")
                .body(request)
                .retrieve()
                .toBodilessEntity()

            logger.info(
                "Task {} successfully sent to node {}",
                task.id,
                peer.id
            )
        } catch (exception: Exception) {
            logger.error(
                "Failed to send task {} to node {}",
                task.id,
                peer.id,
                exception
            )
            throw IllegalStateException(
                "Failed to deliver task to node ${peer.id}",
                exception
            )
        }
    }
}