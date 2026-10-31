
package ru.demo.raft

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.web.client.RestClient
import ru.demo.raft.dto.HeartbeatRequest
import ru.demo.raft.dto.HeartbeatResponse
import ru.demo.raft.dto.RequestVoteRequest
import ru.demo.raft.dto.RequestVoteResponse
import java.util.concurrent.ConcurrentHashMap

@Component
class RaftPeerClient(
    builder: RestClient.Builder
) {
    private val logger = LoggerFactory.getLogger(RaftPeerClient::class.java)

    private val client = builder.build()

    private val peerAvailability = ConcurrentHashMap<Int, Boolean>()

    fun requestVote(
        peer: RaftProperties.Peer,
        request: RequestVoteRequest
    ): RequestVoteResponse? {
        return try {
            val response = client.post()
                .uri("${peer.url}/internal/raft/request-vote")
                .body(request)
                .retrieve()
                .body(RequestVoteResponse::class.java)

            updateAvailability(peer, response != null)
            response
        } catch (exception: Exception) {
            updateAvailability(peer, false)
            null
        }
    }

    fun sendHeartbeat(
        peer: RaftProperties.Peer,
        request: HeartbeatRequest
    ): HeartbeatResponse? {
        return try {
            val response = client.post()
                .uri("${peer.url}/internal/raft/heartbeat")
                .body(request)
                .retrieve()
                .body(HeartbeatResponse::class.java)

            // Узел доступен, если HTTP-запрос получил ответ.
            updateAvailability(peer, response != null)
            response
        } catch (exception: Exception) {
            updateAvailability(peer, false)
            null
        }
    }

    fun isPeerAvailable(peer: RaftProperties.Peer): Boolean {
        val available = try {
            client.get()
                .uri("${peer.url}/internal/raft/status")
                .retrieve()
                .toBodilessEntity()

            true
        } catch (exception: Exception) {
            false
        }

        updateAvailability(peer, available)
        return available
    }

    private fun updateAvailability(
        peer: RaftProperties.Peer,
        available: Boolean
    ) {
        val previous = peerAvailability.put(peer.id, available)

        if (previous == available) {
            return
        }

        if (!available) {
            logger.warn("Node {} is UNAVAILABLE", peer.id)
        } else if (previous == null) {
            logger.info("Node {} is AVAILABLE", peer.id)
        } else {
            logger.info("Node {} is AVAILABLE again", peer.id)
        }
    }
}
