package ru.demo

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class RaftTaskQueueApplication

fun main(args: Array<String>) {
    runApplication<RaftTaskQueueApplication>(*args)
}