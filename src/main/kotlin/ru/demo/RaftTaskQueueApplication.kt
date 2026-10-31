package ru.demo

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.http.client.SimpleClientHttpRequestFactory
import org.springframework.web.client.RestClient
import ru.demo.raft.RaftNode
import ru.demo.raft.RaftProperties

@SpringBootApplication
@EnableConfigurationProperties(RaftProperties::class)
class RaftTaskQueueApplication {

    @Bean
    fun raftNode(properties: RaftProperties): RaftNode {
        return RaftNode(
            nodeId = properties.nodeId,
            electionTimeoutMin = properties.electionTimeoutMin,
            electionTimeoutMax = properties.electionTimeoutMax
        )
    }

    @Bean
    fun restClientBuilder(): RestClient.Builder {
        val requestFactory = SimpleClientHttpRequestFactory().apply {
            setConnectTimeout(java.time.Duration.ofMillis(100))
            setReadTimeout(java.time.Duration.ofMillis(100))
        }

        return RestClient.builder()
            .requestFactory(requestFactory)
    }
}

fun main(args: Array<String>) {
    runApplication<RaftTaskQueueApplication>(*args)
}