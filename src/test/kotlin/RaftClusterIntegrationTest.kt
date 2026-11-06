
package ru.demo.integration

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.net.ServerSocket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.TimeUnit

class RaftClusterIntegrationTest {

    private val logger =
        LoggerFactory.getLogger(RaftClusterIntegrationTest::class.java)

    private val client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(2))
        .build()

    private val mapper = ObjectMapper().findAndRegisterModules()

    private data class NodeStatus(
        val nodeId: Int,
        val state: String,
        val currentTerm: Int,
        val leaderId: Int?
    )

    private data class TestNode(
        val id: Int,
        val port: Int,
        val process: Process
    ) {
        val url: String
            get() = "http://127.0.0.1:$port"
    }

    @Test
    fun `cluster elects leader rejects follower and preserves tasks after leader failure`() {
        logger.info("========== INTEGRATION TEST STARTED ==========")

        val ports = List(3) { findFreePort() }
        val jar = findBootJar()
        val nodes = mutableListOf<TestNode>()

        logger.info("Selected ports: {}", ports)
        logger.info("Application JAR: {}", jar.toAbsolutePath())

        try {
            // 1. Запускаем три независимых JVM-процесса.
            logger.info("STEP 1: Starting three cluster nodes")

            for (id in 1..3) {
                nodes.add(startNode(id, ports, jar))
            }

            // 2. Ждём избрания единственного лидера.
            logger.info("STEP 2: Waiting for initial leader election")

            val initialLeader = waitForLeader(nodes)

            logger.info(
                "Initial leader elected: node={}, port={}",
                initialLeader.id,
                initialLeader.port
            )

            // 3. Проверяем, что follower не принимает задачи.
            logger.info("STEP 3: Checking follower task rejection")

            val follower = nodes.first {
                it.id != initialLeader.id
            }

            val followerResponse = createTask(
                follower,
                "Task from follower"
            )

            logger.info(
                "Follower response: node={}, HTTP status={}",
                follower.id,
                followerResponse.statusCode()
            )

            assertEquals(
                500,
                followerResponse.statusCode(),
                "Follower must reject task creation"
            )

            logger.info(
                "STEP 3 PASSED: follower node {} rejected task creation",
                follower.id
            )

            // 4. Создаём 10 задач через лидера.
            logger.info("STEP 4: Creating 10 tasks through leader node {}", initialLeader.id)

            repeat(10) { index ->
                val description = "Initial task ${index + 1}"

                val response = createTask(
                    initialLeader,
                    description
                )

                logger.info(
                    "Task creation: description='{}', HTTP status={}",
                    description,
                    response.statusCode()
                )

                assertEquals(
                    201,
                    response.statusCode(),
                    "Leader must accept tasks"
                )
            }

            logger.info("STEP 4 PASSED: 10 tasks created")

            // 5. Ждём репликации всех задач на три узла.
            logger.info("STEP 5: Waiting for task replication to all nodes")

            waitUntil(Duration.ofSeconds(5)) {
                nodes.all { node ->
                    getTasks(node).size == 10
                }
            }

            nodes.forEach { node ->
                val tasks = getTasks(node)

                assertEquals(
                    10,
                    tasks.size,
                    "Node ${node.id} must have 10 tasks"
                )

                logger.info(
                    "Replication verified: node={}, taskCount={}",
                    node.id,
                    tasks.size
                )
            }

            logger.info("STEP 5 PASSED: all three nodes contain 10 tasks")

            // 6. Останавливаем текущего лидера и измеряем перевыборы.
            logger.info("STEP 6: Stopping current leader node {}", initialLeader.id)

            val failedNode = initialLeader
            val electionStart = System.nanoTime()

            failedNode.process.destroyForcibly()
            failedNode.process.waitFor(3, TimeUnit.SECONDS)
            nodes.remove(failedNode)

            logger.warn(
                "Leader process stopped: node={}, pid={}",
                failedNode.id,
                failedNode.process.pid()
            )

            // 7. Ждём избрания нового лидера.
            logger.info("STEP 7: Waiting for new leader election")

            val newLeader = waitForLeader(nodes)

            val electionTimeMs =
                (System.nanoTime() - electionStart) / 1_000_000

            logger.info(
                "New leader elected: node={}, term={}, electionTime={} ms",
                newLeader.id,
                getStatus(newLeader)?.currentTerm,
                electionTimeMs
            )

            assertNotEquals(
                failedNode.id,
                newLeader.id,
                "A different node must become leader"
            )

            assertTrue(
                electionTimeMs < 1000,
                "New leader must be elected in less than 1000 ms"
            )

            logger.info(
                "STEP 7 PASSED: new leader elected in {} ms",
                electionTimeMs
            )

            // 8. Новый лидер принимает ещё 5 задач.
            logger.info("STEP 8: Creating 5 new tasks through node {}", newLeader.id)

            repeat(5) { index ->
                val description = "New task ${index + 1}"

                val response = createTask(
                    newLeader,
                    description
                )

                logger.info(
                    "Task creation: description='{}', HTTP status={}",
                    description,
                    response.statusCode()
                )

                assertEquals(
                    201,
                    response.statusCode(),
                    "New leader must accept tasks"
                )
            }

            logger.info("STEP 8 PASSED: 5 additional tasks created")

            // 9. Проверяем наличие всех 15 задач на работающих узлах.
            logger.info("STEP 9: Waiting for replication of all 15 tasks")

            waitUntil(Duration.ofSeconds(5)) {
                nodes.all { node ->
                    getTasks(node).size == 15
                }
            }

            // 10. Проверяем количество и уникальность задач.
            logger.info("STEP 10: Checking task preservation and consistency")

            val expectedTaskIds = nodes
                .flatMap { getTasks(it) }
                .map { it["id"].asText() }
                .toSet()

            assertEquals(
                15,
                expectedTaskIds.size,
                "Cluster must contain exactly 15 unique tasks"
            )

            nodes.forEach { node ->
                val tasks = getTasks(node)

                assertEquals(
                    15,
                    tasks.size,
                    "Node ${node.id} must contain all 15 tasks"
                )

                val uniqueIds = tasks
                    .map { it["id"].asText() }
                    .toSet()

                assertEquals(
                    15,
                    uniqueIds.size,
                    "Node ${node.id} must not contain duplicate tasks"
                )

                assertEquals(
                    expectedTaskIds,
                    uniqueIds,
                    "Node ${node.id} must have the same tasks as other nodes"
                )

                logger.info(
                    "Consistency verified: node={}, taskCount={}, uniqueTaskCount={}",
                    node.id,
                    tasks.size,
                    uniqueIds.size
                )
            }

            logger.info("STEP 10 PASSED: all nodes contain the same 15 tasks")
            logger.info("========== INTEGRATION TEST PASSED ==========")

        } catch (exception: Throwable) {
            logger.error(
                "INTEGRATION TEST FAILED: {}",
                exception.message,
                exception
            )
            throw exception
        } finally {
            // Завершаем все оставшиеся процессы.
            logger.info("Cleaning up cluster processes")

            nodes.forEach { node ->
                if (node.process.isAlive) {
                    logger.info(
                        "Stopping node: id={}, pid={}",
                        node.id,
                        node.process.pid()
                    )

                    node.process.destroyForcibly()
                    node.process.waitFor(3, TimeUnit.SECONDS)
                }
            }

            logger.info("Cluster process cleanup completed")
        }
    }

    private fun startNode(
        id: Int,
        ports: List<Int>,
        jar: Path
    ): TestNode {
        val port = ports[id - 1]

        val command = mutableListOf(
            "java",
            "-jar",
            jar.toAbsolutePath().toString(),
            "--server.port=$port",
            "--raft.node-id=$id"
        )

        ports.forEachIndexed { index, peerPort ->
            val peerId = index + 1

            command.add("--raft.peers[$index].id=$peerId")
            command.add(
                "--raft.peers[$index].url=http://127.0.0.1:$peerPort"
            )
        }

        val logFile = Path.of(
            "build",
            "integration-node-$id.log"
        )

        Files.createDirectories(logFile.parent)

        val process = ProcessBuilder(command)
            .redirectErrorStream(true)
            .redirectOutput(logFile.toFile())
            .start()

        val node = TestNode(
            id = id,
            port = port,
            process = process
        )

        logger.info(
            "Node process started: id={}, port={}, pid={}, log={}",
            id,
            port,
            process.pid(),
            logFile.toAbsolutePath()
        )

        return node
    }

    private fun waitForLeader(
        nodes: List<TestNode>
    ): TestNode {
        var leader: TestNode? = null

        logger.info(
            "Checking leader status of {} nodes",
            nodes.size
        )

        waitUntil(Duration.ofSeconds(8)) {
            val statuses = nodes.mapNotNull { node ->
                getStatus(node)
            }

            val leaders = statuses.filter {
                it.state == "LEADER"
            }

            val allAgree =
                statuses.size == nodes.size &&
                        leaders.size == 1 &&
                        statuses.all {
                            it.leaderId == leaders.first().nodeId
                        }

            if (allAgree) {
                val leaderId = leaders.first().nodeId

                leader = nodes.firstOrNull {
                    it.id == leaderId
                }

                if (leader != null) {
                    logger.info(
                        "Cluster agrees on leader: node={}, term={}",
                        leaderId,
                        leaders.first().currentTerm
                    )
                }

                leader != null
            } else {
                false
            }
        }

        return requireNotNull(leader) {
            "Cluster did not elect exactly one leader"
        }
    }

    private fun getStatus(
        node: TestNode
    ): NodeStatus? {
        return try {
            val response = get(
                "${node.url}/internal/raft/status"
            )

            if (response.statusCode() != 200) {
                return null
            }

            val json = mapper.readTree(response.body())

            NodeStatus(
                nodeId = json["nodeId"].asInt(),
                state = json["state"].asText(),
                currentTerm = json["currentTerm"].asInt(),
                leaderId = json["leaderId"]
                    .takeUnless { it == null || it.isNull }
                    ?.asInt()
            )
        } catch (exception: Exception) {
            null
        }
    }

    private fun createTask(
        node: TestNode,
        description: String
    ): HttpResponse<String> {
        val body = mapper.writeValueAsString(
            mapOf("description" to description)
        )

        val request = HttpRequest.newBuilder()
            .uri(URI.create("${node.url}/api/tasks"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .timeout(Duration.ofSeconds(3))
            .build()

        return client.send(
            request,
            HttpResponse.BodyHandlers.ofString()
        )
    }

    private fun getTasks(
        node: TestNode
    ): List<JsonNode> {
        val response = get(
            "${node.url}/api/tasks"
        )

        assertEquals(
            200,
            response.statusCode(),
            "Cannot get tasks from node ${node.id}"
        )

        return mapper.readTree(response.body()).toList()
    }

    private fun get(
        url: String
    ): HttpResponse<String> {
        val request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .GET()
            .timeout(Duration.ofSeconds(2))
            .build()

        return client.send(
            request,
            HttpResponse.BodyHandlers.ofString()
        )
    }

    private fun findFreePort(): Int {
        return ServerSocket(0).use {
            it.localPort
        }
    }

    private fun findBootJar(): Path {
        val libs = Path.of("build", "libs")

        return Files.list(libs).use { files ->
            files
                .filter {
                    val name = it.fileName.toString()

                    name.endsWith(".jar") &&
                            !name.endsWith("-plain.jar")
                }
                .findFirst()
                .orElseThrow {
                    IllegalStateException(
                        "Spring Boot JAR not found in build/libs"
                    )
                }
        }
    }

    private fun waitUntil(
        timeout: Duration,
        condition: () -> Boolean
    ) {
        val deadline = System.nanoTime() + timeout.toNanos()

        while (System.nanoTime() < deadline) {
            if (condition()) {
                return
            }

            Thread.sleep(50)
        }

        throw AssertionError(
            "Condition was not satisfied within $timeout"
        )
    }
}
