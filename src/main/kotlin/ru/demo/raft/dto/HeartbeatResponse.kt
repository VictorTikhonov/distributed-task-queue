package ru.demo.raft.dto

data class HeartbeatResponse(
    val term: Int,
    val success: Boolean
)