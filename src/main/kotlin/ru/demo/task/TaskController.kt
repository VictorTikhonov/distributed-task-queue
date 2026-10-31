package ru.demo.task

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.*
import ru.demo.task.dto.CreateTaskRequest
import ru.demo.task.dto.TaskResponse

@RestController
@RequestMapping("/api/tasks")
class TaskController(
    private val taskService: TaskService
) {

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    fun createTask(
        @RequestBody request: CreateTaskRequest
    ): TaskResponse {
        val task = taskService.createTask(request.description)
        return taskService.toResponse(task)
    }

    @GetMapping
    fun getAllTasks(): List<TaskResponse> {
        return taskService.getAllTasks()
            .map { taskService.toResponse(it) }
    }

    @GetMapping("/status/{status}")
    fun getTasksByStatus(
        @PathVariable status: TaskStatus
    ): List<TaskResponse> {
        return taskService.getTasksByStatus(status)
            .map { taskService.toResponse(it) }
    }
}