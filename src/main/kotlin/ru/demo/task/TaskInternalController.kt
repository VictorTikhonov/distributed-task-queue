package ru.demo.task

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.*
import ru.demo.task.dto.InternalTaskRequest

@RestController
@RequestMapping("/internal/tasks")
class TaskInternalController(
    private val taskStore: TaskStore
) {
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    fun receiveTask(@RequestBody request: InternalTaskRequest): Task {
        val task = Task(
            id = request.id,
            description = request.description,
            status = request.status,
            assignedNodeId = request.assignedNodeId,
            createdAt = request.createdAt
        )

        return taskStore.save(task)
    }

    @GetMapping
    fun getTasks(): List<Task> = taskStore.findAll()
}
