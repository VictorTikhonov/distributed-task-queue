package ru.demo.raft.dto

data class RequestVoteResponse(
    val term: Int,
    val voteGranted: Boolean
)