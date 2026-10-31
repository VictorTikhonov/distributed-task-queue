
package ru.demo.raft

import org.slf4j.LoggerFactory
import java.util.concurrent.ThreadLocalRandom


class RaftNode(
    val nodeId: Int,
    private val electionTimeoutMin: Long,
    private val electionTimeoutMax: Long
) {
    private val logger = LoggerFactory.getLogger(RaftNode::class.java)

    @Volatile
    var state: NodeState = NodeState.FOLLOWER
        private set

    @Volatile
    var currentTerm: Int = 0
        private set

    @Volatile
    var votedFor: Int? = null
        private set

    @Volatile
    var leaderId: Int? = null
        private set

    private val votesReceived = mutableSetOf<Int>()

    @Volatile
    private var electionDeadlineNanos = nextElectionDeadline()

    init {
        require(electionTimeoutMin > 0) {
            "Election timeout minimum must be positive"
        }
        require(electionTimeoutMax >= electionTimeoutMin) {
            "Election timeout maximum must be >= minimum"
        }
    }

    private fun nextElectionDeadline(): Long {
        val timeout = ThreadLocalRandom.current().nextLong(
            electionTimeoutMin,
            electionTimeoutMax + 1
        )

        return System.nanoTime() + timeout * 1_000_000
    }

    @Synchronized
    fun startElection(): Int {
        currentTerm++
        state = NodeState.CANDIDATE
        votedFor = nodeId
        leaderId = null

        votesReceived.clear()
        votesReceived.add(nodeId)

        resetElectionTimeout()

        logger.info(
            "Node {}: Started election. Term={}",
            nodeId,
            currentTerm
        )

        return currentTerm
    }

    @Synchronized
    fun handleRequestVote(
        candidateId: Int,
        candidateTerm: Int
    ): Boolean {
        if (candidateTerm < currentTerm) {
            return false
        }

        if (candidateTerm > currentTerm) {
            becomeFollower(candidateTerm)
        }

        val canVote = votedFor == null || votedFor == candidateId

        if (canVote) {
            votedFor = candidateId
            resetElectionTimeout()

            logger.info(
                "Node {}: Voted for Node {}. Term={}",
                nodeId,
                candidateId,
                currentTerm
            )

            return true
        }

        return false
    }

    @Synchronized
    fun receiveHeartbeat(
        incomingLeaderId: Int,
        leaderTerm: Int
    ): Boolean {
        if (leaderTerm < currentTerm) {
            return false
        }

        if (leaderTerm > currentTerm) {
            becomeFollower(leaderTerm)
        }

        // Не принимаем heartbeat от другого лидера в том же терме.
        if (state == NodeState.LEADER && incomingLeaderId != nodeId) {
            return false
        }

        if (leaderId != null && leaderId != incomingLeaderId) {
            return false
        }

        // Получение heartbeat подтверждает наличие действующего лидера.
        if (state != NodeState.FOLLOWER) {
            logger.info(
                "Node {}: Became FOLLOWER. Leader={}, Term={}",
                nodeId,
                incomingLeaderId,
                currentTerm
            )
        }

        state = NodeState.FOLLOWER
        leaderId = incomingLeaderId

        resetElectionTimeout()

        return true
    }

    @Synchronized
    fun addVote(voterId: Int, electionTerm: Int): Boolean {
        if (state != NodeState.CANDIDATE || currentTerm != electionTerm) {
            return false
        }

        // Set не позволяет одному узлу проголосовать дважды.
        return votesReceived.add(voterId)
    }

    @Synchronized
    fun hasMajority(totalNodes: Int): Boolean {
        return votesReceived.size > totalNodes / 2
    }

    @Synchronized
    fun becomeLeader(): Boolean {
        if (state != NodeState.CANDIDATE) {
            return false
        }

        state = NodeState.LEADER
        leaderId = nodeId

        logger.info(
            "Node {}: Became LEADER. Term={}",
            nodeId,
            currentTerm
        )

        return true
    }

    @Synchronized
    fun becomeFollower(newTerm: Int) {
        if (newTerm < currentTerm) {
            return
        }

        val previousState = state

        currentTerm = newTerm
        state = NodeState.FOLLOWER
        votedFor = null
        leaderId = null

        votesReceived.clear()
        resetElectionTimeout()

        if (previousState != NodeState.FOLLOWER) {
            logger.info(
                "Node {}: Became FOLLOWER. Term={}",
                nodeId,
                currentTerm
            )
        }
    }

    @Synchronized
    fun updateTerm(newTerm: Int) {
        if (newTerm > currentTerm) {
            becomeFollower(newTerm)
        }
    }

    @Synchronized
    fun resetElectionTimeout() {
        electionDeadlineNanos = nextElectionDeadline()
    }

    fun isElectionTimeoutExpired(): Boolean {
        return System.nanoTime() >= electionDeadlineNanos
    }

    @Synchronized
    fun getVotesCount(): Int {
        return votesReceived.size
    }
}
