package ru.demo.task.dto

import ru.demo.task.TaskStatus
import java.time.Instant
import java.util.UUID

data class InternalTaskRequest(
    val id: UUID,
    val description: String,
    val status: TaskStatus,
    val assignedNodeId: Int,
    val createdAt: Instant
)