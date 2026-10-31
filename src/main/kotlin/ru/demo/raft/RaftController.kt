package ru.demo.raft

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import ru.demo.raft.dto.HeartbeatRequest
import ru.demo.raft.dto.HeartbeatResponse
import ru.demo.raft.dto.RequestVoteRequest
import ru.demo.raft.dto.RequestVoteResponse

@RestController
@RequestMapping("/internal/raft")
class RaftController(
    private val node: RaftNode
) {

    @PostMapping("/request-vote")
    fun requestVote(
        @RequestBody request: RequestVoteRequest
    ): RequestVoteResponse {
        val granted = node.handleRequestVote(
            candidateId = request.candidateId,
            candidateTerm = request.term
        )

        return RequestVoteResponse(
            term = node.currentTerm,
            voteGranted = granted
        )
    }

    @PostMapping("/heartbeat")
    fun heartbeat(
        @RequestBody request: HeartbeatRequest
    ): HeartbeatResponse {
        val success = node.receiveHeartbeat(
            incomingLeaderId = request.leaderId,
            leaderTerm = request.term
        )

        return HeartbeatResponse(
            term = node.currentTerm,
            success = success
        )
    }

    @GetMapping("/status")
    fun status(): Map<String, Any?> {
        return mapOf(
            "nodeId" to node.nodeId,
            "state" to node.state,
            "currentTerm" to node.currentTerm,
            "votedFor" to node.votedFor,
            "leaderId" to node.leaderId,
            "votesReceived" to node.getVotesCount()
        )
    }
}