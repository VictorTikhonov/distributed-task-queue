
package ru.demo.raft

import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Service
import ru.demo.raft.dto.HeartbeatRequest
import ru.demo.raft.dto.RequestVoteRequest
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

@Service
class RaftService(
    private val node: RaftNode,
    private val properties: RaftProperties,
    private val peerClient: RaftPeerClient
) {
    private val logger = LoggerFactory.getLogger(RaftService::class.java)

    private val scheduler: ScheduledExecutorService =
        Executors.newScheduledThreadPool(8)

    private val peers = properties.peers.filter {
        it.id != properties.nodeId
    }

    private val totalNodes = properties.peers.size

    @Volatile
    private var clusterInitialized = false

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        require(totalNodes >= 3) {
            "Raft cluster must contain at least 3 nodes"
        }

        require(properties.peers.any { it.id == properties.nodeId }) {
            "Current node ID is missing from raft.peers"
        }

        logger.info(
            "Node {}: Waiting for cluster initialization. Cluster size={}",
            node.nodeId,
            totalNodes
        )

        // Сначала ждём доступности большинства узлов.
        scheduler.scheduleWithFixedDelay(
            { initializeCluster() },
            0,
            100,
            TimeUnit.MILLISECONDS
        )
    }

    private fun initializeCluster() {
        if (clusterInitialized) {
            return
        }

        try {
            val availablePeers = peers.count { peer ->
                peerClient.isPeerAvailable(peer)
            }

            // Считаем текущий узел доступным самому себе.
            val availableNodes = availablePeers + 1

            if (availableNodes > totalNodes / 2) {
                clusterInitialized = true

                logger.info(
                    "Node {}: Cluster initialized. Available nodes={}/{}",
                    node.nodeId,
                    availableNodes,
                    totalNodes
                )

                // Начинаем отсчёт election timeout после
                // обнаружения доступного большинства.
                node.resetElectionTimeout()

                startRaftTimers()
            }
        } catch (exception: Exception) {
            logger.error(
                "Node {}: Cluster initialization failed",
                node.nodeId,
                exception
            )
        }
    }

    private fun startRaftTimers() {
        // Проверка election timeout каждые 10 мс.
        scheduler.scheduleAtFixedRate(
            { checkElectionTimeout() },
            10,
            10,
            TimeUnit.MILLISECONDS
        )

        // Heartbeat каждые 50 мс.
        scheduler.scheduleAtFixedRate(
            { sendHeartbeats() },
            50,
            properties.heartbeatInterval,
            TimeUnit.MILLISECONDS
        )

        logger.info(
            "Node {}: Raft timers started. Heartbeat interval={} ms",
            node.nodeId,
            properties.heartbeatInterval
        )
    }

    private fun checkElectionTimeout() {
        try {
            if (node.state == NodeState.LEADER) {
                return
            }

            if (node.isElectionTimeoutExpired()) {
                startElection()
            }
        } catch (exception: Exception) {
            logger.error(
                "Node {}: Election check failed",
                node.nodeId,
                exception
            )
        }
    }

    private fun startElection() {
        val electionTerm = node.startElection()

        val request = RequestVoteRequest(
            candidateId = node.nodeId,
            term = electionTerm
        )

        logger.info(
            "Node {}: Requesting votes. Term={}",
            node.nodeId,
            electionTerm
        )

        peers.forEach { peer ->
            CompletableFuture.runAsync({
                val response = peerClient.requestVote(peer, request)
                    ?: return@runAsync

                // Если обнаружен более высокий term,
                // кандидат становится follower.
                if (response.term > node.currentTerm) {
                    node.updateTerm(response.term)
                    return@runAsync
                }

                if (response.voteGranted) {
                    val voteAdded = node.addVote(
                        voterId = peer.id,
                        electionTerm = electionTerm
                    )

                    if (voteAdded && node.hasMajority(totalNodes)) {
                        if (node.becomeLeader()) {
                            sendHeartbeats()
                        }
                    }
                }
            }, scheduler)
        }
    }

    private fun sendHeartbeats() {
        if (node.state != NodeState.LEADER) {
            return
        }

        val request = HeartbeatRequest(
            leaderId = node.nodeId,
            term = node.currentTerm
        )

        peers.forEach { peer ->
            CompletableFuture.runAsync({
                val response = peerClient.sendHeartbeat(peer, request)
                    ?: return@runAsync

                if (response.term > node.currentTerm) {
                    node.updateTerm(response.term)
                }
            }, scheduler)
        }
    }

    @PreDestroy
    fun stop() {
        logger.info(
            "Node {}: Stopping Raft service",
            node.nodeId
        )

        scheduler.shutdownNow()
    }
}
