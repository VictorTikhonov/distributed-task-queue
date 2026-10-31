
package ru.demo.raft

class RaftNode(
    val nodeID: Int,
) {
    var state: NodeState = NodeState.FOLLOWER
        private set

    var currentTerm: Int = 0
        private set

    // ID кандидата, за которого проголосовал этот узел
    var votedFor: Int? = null
        private set

    // ID узлов, проголосовавших за этого кандидата
    val votesReceived: MutableSet<Int> = mutableSetOf()

    // ID известного лидера
    var leaderId: Int? = null
        private set

    @Synchronized
    fun startElection() {
        currentTerm++
        state = NodeState.CANDIDATE

        votedFor = nodeID
        leaderId = null

        votesReceived.clear()
        votesReceived.add(nodeID)
    }

    @Synchronized
    fun handleRequestVote(
        candidateId: Int,
        candidateTerm: Int,
    ): Boolean {

        // Запрос из устаревшего терма
        if (candidateTerm < currentTerm) {
            return false
        }

        // Запрос из нового терма
        if (candidateTerm > currentTerm) {
            becomeFollower(candidateTerm)
        }

        // Отдаём голос, если ещё не голосовали
        // или уже голосовали за этого кандидата
        if (votedFor == null || votedFor == candidateId) {
            votedFor = candidateId
            return true
        }

        return false
    }

    @Synchronized
    fun becomeLeader() {
        if (state != NodeState.CANDIDATE) {
            return
        }

        state = NodeState.LEADER
        leaderId = nodeID
    }

    @Synchronized
    fun receiveHeartbeat(
        leaderId: Int,
        leaderTerm: Int,
    ): Boolean {

        // Heartbeat из устаревшего терма
        if (leaderTerm < currentTerm) {
            return false
        }

        // Heartbeat из нового терма
        if (leaderTerm > currentTerm) {
            becomeFollower(leaderTerm)
        }

        // Проверяем, не известен ли нам уже другой лидер
        if (this.leaderId != null && this.leaderId != leaderId) {
            return false
        }

        // Если мы кандидат, признаём лидера
        if (state == NodeState.CANDIDATE) {
            state = NodeState.FOLLOWER
            votesReceived.clear()
        }

        // Запоминаем лидера, если ещё не знали его
        if (this.leaderId == null) {
            this.leaderId = leaderId
        }

        return true
    }

    private fun becomeFollower(newTerm: Int) {
        currentTerm = newTerm
        state = NodeState.FOLLOWER

        votedFor = null
        leaderId = null

        votesReceived.clear()
    }
}