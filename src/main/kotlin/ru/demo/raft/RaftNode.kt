package ru.demo.raft

class RaftNode(
    val nodeID: Int,
) {
    var state: NodeState = NodeState.FOLLOWER
        private set

    var currentTerm: Int = 0
        private set

    // D узла, за который голосовали в текущем терме
    var votedFor: Int? = null
        private set

    // Множество ID узлов, которые проголосовали за кандидата
    val votesReceived: MutableSet<Int> = mutableSetOf()

    @Synchronized
    fun startElection() {
        currentTerm++
        state = NodeState.CANDIDATE

        votedFor = nodeID

        votesReceived.clear()
        votesReceived.add(nodeID)
    }

    @Synchronized
    fun handleRequestVote(
        candidateId: Int,
        candidateTerm: Int,
    ): Boolean {

        if (currentTerm > candidateTerm) {
            return false
        }

        if (currentTerm < candidateTerm) {
            currentTerm = candidateTerm
            state = NodeState.FOLLOWER
            votedFor = null
            votesReceived.clear()
        }

        if (votedFor == null || votedFor == candidateId) {
            votedFor = candidateId
            return true
        }

        return false
    }

    fun becomeLeader() {
        if (state == NodeState.CANDIDATE) {
            state = NodeState.LEADER
        }
    }
}