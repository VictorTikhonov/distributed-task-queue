package ru.demo.raft

import org.springframework.boot.context.properties.ConfigurationProperties


@ConfigurationProperties(prefix = "raft")
data class RaftProperties(
    var nodeId: Int = 1,
    var heartbeatInterval: Long = 50,
    var electionTimeoutMin: Long = 150,
    var electionTimeoutMax: Long = 300,
    var peers: List<Peer> = emptyList()
) {
    data class Peer(
        var id: Int = 0,
        var url: String = ""
    )
}