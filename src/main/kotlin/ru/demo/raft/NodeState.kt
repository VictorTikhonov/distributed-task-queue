package ru.demo.raft

enum class NodeState {
    FOLLOWER,
    CANDIDATE,
    LEADER
}