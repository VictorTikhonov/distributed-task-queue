package ru.demo.task

import org.springframework.stereotype.Component
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

@Component
class TaskStore {

    private val tasks = ConcurrentHashMap<UUID, Task>()

    fun save(task: Task): Task {
        tasks[task.id] = task
        return task
    }

    fun findById(id: UUID): Task? {
        return tasks[id]
    }

    fun findAll(): List<Task> {
        return tasks.values.sortedBy { it.createdAt }
    }

    fun update(task: Task): Task {
        tasks[task.id] = task
        return task
    }

    fun delete(id: UUID): Boolean {
        return tasks.remove(id) != null
    }

    fun findAllByStatus(status: TaskStatus): List<Task> {
        return tasks.values.filter { it.status == status }
    }

    fun saveAll(newTasks: List<Task>) {
        newTasks.forEach { save(it) }
    }
}