package ru.demo.raft.dto

data class HeartbeatRequest(
    val leaderId: Int,
    val term: Int
)