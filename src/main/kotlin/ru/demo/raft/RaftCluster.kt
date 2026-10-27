package ru.demo.raft

class RaftCluster(val nodeCount: Int) {

    val nodes: List<RaftNode> = (1..nodeCount).map { RaftNode(it) }

    fun startElection(candidateId: Int) {

        val candidate = nodes.first { it.nodeID == candidateId }

        // Кандидат начинает выборы и голосует за себя
        candidate.startElection()

        // Отправляем запросы остальным узлам
        for (node in nodes) {
            if (node.nodeID == candidateId) {
                continue
            }

            val granted = node.handleRequestVote(
                candidateId = candidate.nodeID,
                candidateTerm = candidate.currentTerm
            )

            // Если узел отдал голос, учитываем его у кандидата
            if (granted) {
                candidate.votesReceived.add(node.nodeID)
            }
        }

        // Проверяем, набрал ли кандидат большинство
        val quorum = nodes.size / 2 + 1

        if (candidate.votesReceived.size >= quorum) {
            candidate.becomeLeader()
        }
    }
}