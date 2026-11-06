
package ru.demo.raft

import org.slf4j.LoggerFactory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RaftNodeTest {

    private val logger = LoggerFactory.getLogger(RaftNodeTest::class.java)

    private fun createNode(
        nodeId: Int = 1,
        timeoutMin: Long = 10_000,
        timeoutMax: Long = 10_000
    ) = RaftNode(
        nodeId = nodeId,
        electionTimeoutMin = timeoutMin,
        electionTimeoutMax = timeoutMax
    )

    @Test
    fun `new node starts as follower`() {
        logger.info("TEST START: new node starts as follower")

        val node = createNode()

        assertEquals(NodeState.FOLLOWER, node.state)
        assertEquals(0, node.currentTerm)
        assertNull(node.votedFor)
        assertNull(node.leaderId)

        logger.info("TEST PASSED: node starts as FOLLOWER, term=0")
    }

    @Test
    fun `start election changes state to candidate`() {
        logger.info("TEST START: start election changes state to candidate")

        val node = createNode()

        val term = node.startElection()

        assertEquals(NodeState.CANDIDATE, node.state)
        assertEquals(1, term)
        assertEquals(1, node.currentTerm)
        assertEquals(1, node.votedFor)
        assertEquals(1, node.getVotesCount())

        logger.info("TEST PASSED: node became CANDIDATE, term={}", term)
    }

    @Test
    fun `start election increments term`() {
        logger.info("TEST START: start election increments term")

        val node = createNode()

        val firstTerm = node.startElection()
        val secondTerm = node.startElection()

        assertEquals(1, firstTerm)
        assertEquals(2, secondTerm)
        assertEquals(2, node.currentTerm)

        logger.info(
            "TEST PASSED: election term incremented from {} to {}",
            firstTerm,
            secondTerm
        )
    }

    @Test
    fun `candidate votes for itself`() {
        logger.info("TEST START: candidate votes for itself")

        val node = createNode(nodeId = 2)

        node.startElection()

        assertEquals(2, node.votedFor)
        assertEquals(1, node.getVotesCount())

        logger.info("TEST PASSED: candidate voted for itself")
    }

    @Test
    fun `candidate becomes leader after receiving majority`() {
        logger.info("TEST START: candidate becomes leader after majority")

        val node = createNode()
        val term = node.startElection()

        assertTrue(node.addVote(voterId = 2, electionTerm = term))
        assertTrue(node.hasMajority(totalNodes = 3))
        assertTrue(node.becomeLeader())

        assertEquals(NodeState.LEADER, node.state)
        assertEquals(1, node.leaderId)

        logger.info(
            "TEST PASSED: candidate became LEADER, votes={}",
            node.getVotesCount()
        )
    }

    @Test
    fun `candidate does not have majority with only its own vote`() {
        logger.info("TEST START: candidate does not have majority with one vote")

        val node = createNode()

        node.startElection()

        assertFalse(node.hasMajority(totalNodes = 3))
        assertEquals(1, node.getVotesCount())
        assertEquals(NodeState.CANDIDATE, node.state)

        logger.info("TEST PASSED: one vote is not a majority")
    }

    @Test
    fun `duplicate vote is not counted twice`() {
        logger.info("TEST START: duplicate vote is not counted twice")

        val node = createNode()
        val term = node.startElection()

        assertTrue(node.addVote(voterId = 2, electionTerm = term))
        assertFalse(node.addVote(voterId = 2, electionTerm = term))

        assertEquals(2, node.getVotesCount())

        logger.info("TEST PASSED: duplicate vote ignored")
    }

    @Test
    fun `vote from previous term is rejected`() {
        logger.info("TEST START: vote from previous term is rejected")

        val node = createNode()
        val term = node.startElection()

        assertFalse(node.addVote(voterId = 2, electionTerm = term - 1))
        assertEquals(1, node.getVotesCount())

        logger.info("TEST PASSED: vote from old term rejected")
    }

    @Test
    fun `vote is rejected when node is not candidate`() {
        logger.info("TEST START: vote is rejected when node is not candidate")

        val node = createNode()

        assertFalse(node.addVote(voterId = 2, electionTerm = 0))
        assertEquals(0, node.getVotesCount())

        logger.info("TEST PASSED: non-candidate rejected vote")
    }

    @Test
    fun `node rejects vote request with lower term`() {
        logger.info("TEST START: node rejects vote request with lower term")

        val node = createNode()
        node.startElection()

        val result = node.handleRequestVote(
            candidateId = 2,
            candidateTerm = 0
        )

        assertFalse(result)
        assertEquals(1, node.currentTerm)
        assertEquals(1, node.votedFor)

        logger.info("TEST PASSED: outdated vote request rejected")
    }

    @Test
    fun `node grants vote in a new term`() {
        logger.info("TEST START: node grants vote in a new term")

        val node = createNode()

        val result = node.handleRequestVote(
            candidateId = 2,
            candidateTerm = 1
        )

        assertTrue(result)
        assertEquals(1, node.currentTerm)
        assertEquals(2, node.votedFor)
        assertEquals(NodeState.FOLLOWER, node.state)

        logger.info("TEST PASSED: vote granted to candidate 2")
    }

    @Test
    fun `node does not grant votes to different candidates in same term`() {
        logger.info("TEST START: node does not vote for different candidates in same term")

        val node = createNode()

        val firstVote = node.handleRequestVote(
            candidateId = 2,
            candidateTerm = 1
        )

        val secondVote = node.handleRequestVote(
            candidateId = 3,
            candidateTerm = 1
        )

        assertTrue(firstVote)
        assertFalse(secondVote)
        assertEquals(2, node.votedFor)

        logger.info("TEST PASSED: second candidate rejected")
    }

    @Test
    fun `node can grant vote again to same candidate in same term`() {
        logger.info("TEST START: node can grant vote again to same candidate in same term")

        val node = createNode()

        val firstVote = node.handleRequestVote(
            candidateId = 2,
            candidateTerm = 1
        )

        val secondVote = node.handleRequestVote(
            candidateId = 2,
            candidateTerm = 1
        )

        assertTrue(firstVote)
        assertTrue(secondVote)
        assertEquals(2, node.votedFor)

        logger.info("TEST PASSED: repeated vote for the same candidate granted")
    }

    @Test
    fun `higher term resets node to follower`() {
        logger.info("TEST START: higher term resets node to follower")

        val node = createNode()

        node.startElection()
        node.updateTerm(5)

        assertEquals(NodeState.FOLLOWER, node.state)
        assertEquals(5, node.currentTerm)
        assertNull(node.votedFor)
        assertNull(node.leaderId)
        assertEquals(0, node.getVotesCount())

        logger.info("TEST PASSED: node reset to FOLLOWER, term=5")
    }

    @Test
    fun `node ignores lower term update`() {
        logger.info("TEST START: node ignores lower term update")

        val node = createNode()

        node.startElection()
        node.updateTerm(0)

        assertEquals(NodeState.CANDIDATE, node.state)
        assertEquals(1, node.currentTerm)

        logger.info("TEST PASSED: lower term update ignored")
    }

    @Test
    fun `heartbeat makes candidate follower`() {
        logger.info("TEST START: heartbeat makes candidate follower")

        val node = createNode()
        node.startElection()

        val result = node.receiveHeartbeat(
            incomingLeaderId = 2,
            leaderTerm = 1
        )

        assertTrue(result)
        assertEquals(NodeState.FOLLOWER, node.state)
        assertEquals(2, node.leaderId)

        logger.info("TEST PASSED: candidate became FOLLOWER, leader=2")
    }

    @Test
    fun `heartbeat with higher term makes node follower`() {
        logger.info("TEST START: heartbeat with higher term makes node follower")

        val node = createNode()
        node.startElection()

        val result = node.receiveHeartbeat(
            incomingLeaderId = 2,
            leaderTerm = 3
        )

        assertTrue(result)
        assertEquals(NodeState.FOLLOWER, node.state)
        assertEquals(3, node.currentTerm)
        assertEquals(2, node.leaderId)

        logger.info("TEST PASSED: higher-term heartbeat accepted")
    }

    @Test
    fun `node rejects heartbeat with lower term`() {
        logger.info("TEST START: node rejects heartbeat with lower term")

        val node = createNode()
        node.startElection()

        val result = node.receiveHeartbeat(
            incomingLeaderId = 2,
            leaderTerm = 0
        )

        assertFalse(result)
        assertEquals(NodeState.CANDIDATE, node.state)
        assertEquals(1, node.currentTerm)

        logger.info("TEST PASSED: outdated heartbeat rejected")
    }

    @Test
    fun `node rejects heartbeat from another leader in same term`() {
        logger.info("TEST START: node rejects heartbeat from another leader in same term")

        val node = createNode()

        node.receiveHeartbeat(
            incomingLeaderId = 2,
            leaderTerm = 1
        )

        val result = node.receiveHeartbeat(
            incomingLeaderId = 3,
            leaderTerm = 1
        )

        assertFalse(result)
        assertEquals(2, node.leaderId)

        logger.info("TEST PASSED: heartbeat from competing leader rejected")
    }

    @Test
    fun `leader rejects heartbeat from another leader in same term`() {
        logger.info("TEST START: leader rejects heartbeat from another leader")

        val node = createNode()
        val term = node.startElection()

        node.addVote(voterId = 2, electionTerm = term)
        node.becomeLeader()

        val result = node.receiveHeartbeat(
            incomingLeaderId = 2,
            leaderTerm = term
        )

        assertFalse(result)
        assertEquals(NodeState.LEADER, node.state)

        logger.info("TEST PASSED: current leader retained its state")
    }

    @Test
    fun `leader can receive its own heartbeat`() {
        logger.info("TEST START: leader can receive its own heartbeat")

        val node = createNode()
        val term = node.startElection()

        node.addVote(voterId = 2, electionTerm = term)
        node.becomeLeader()

        val result = node.receiveHeartbeat(
            incomingLeaderId = 1,
            leaderTerm = term
        )

        assertTrue(result)
        assertEquals(NodeState.LEADER, node.state)

        logger.info("TEST PASSED: leader retained LEADER state")
    }

    @Test
    fun `leader can become follower in higher term`() {
        logger.info("TEST START: leader can become follower in higher term")

        val node = createNode()
        val term = node.startElection()

        node.addVote(voterId = 2, electionTerm = term)
        node.becomeLeader()

        node.updateTerm(2)

        assertEquals(NodeState.FOLLOWER, node.state)
        assertEquals(2, node.currentTerm)
        assertNull(node.leaderId)

        logger.info("TEST PASSED: leader stepped down in higher term")
    }

    @Test
    fun `become leader does nothing when node is not candidate`() {
        logger.info("TEST START: become leader does nothing when node is not candidate")

        val node = createNode()

        assertFalse(node.becomeLeader())
        assertEquals(NodeState.FOLLOWER, node.state)

        logger.info("TEST PASSED: follower did not become leader")
    }

    @Test
    fun `election timeout is not expired immediately`() {
        logger.info("TEST START: election timeout is not expired immediately")

        val node = createNode(
            timeoutMin = 10_000,
            timeoutMax = 10_000
        )

        assertFalse(node.isElectionTimeoutExpired())

        logger.info("TEST PASSED: election timeout has not expired")
    }
}
