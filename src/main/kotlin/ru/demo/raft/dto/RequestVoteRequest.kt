package ru.demo.raft.dto

data class RequestVoteRequest(
    val candidateId: Int,
    val term: Int
)