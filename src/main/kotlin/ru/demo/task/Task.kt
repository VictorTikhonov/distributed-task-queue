package ru.demo.task

import java.time.Instant
import java.util.UUID

data class Task(
    val id: UUID = UUID.randomUUID(),
    val description: String,
    val status: TaskStatus = TaskStatus.PENDING,
    val assignedNodeId: Int,
    val createdAt: Instant = Instant.now()
)