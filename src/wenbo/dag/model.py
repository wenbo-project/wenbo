"""DAG data model."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional
from uuid import uuid4


class NodeType(str, Enum):
    LLM_INFER = "llm_infer"
    TOOL_CALL = "tool_call"
    RETRIEVAL = "retrieval"
    AGGREGATE = "aggregate"
    CONDITION = "condition"
    BACKTRACK = "backtrack"
    CUSTOM = "custom"


class NodeState(str, Enum):
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    BACKTRACKED = "backtracked"
    BLOCKED = "blocked"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class DAGState(str, Enum):
    CREATED = "created"
    RUNNING = "running"
    EVOLVING = "evolving"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ModelConfig:
    model: str = "qwen2.5-7b"
    sampling_params: dict[str, Any] = field(default_factory=lambda: {
        "temperature": 0.7,
        "top_p": 0.9,
        "max_tokens": 512,
    })


@dataclass
class QoS:
    slo_deadline_ms: Optional[float] = None
    priority: int = 0


@dataclass
class ResourceReq:
    cpu_cores: float = 0.0
    gpu_count: int = 0
    memory_mb: float = 0.0
    gpu_memory_mb: float = 0.0
    estimated_gpu_memory_mb: float = 0.0
    estimated_compute_flops: float = 0.0
    estimated_time_ms: float = 0.0


@dataclass
class Node:
    node_id: str
    node_type: NodeType = NodeType.LLM_INFER
    prompt: str = ""
    hardware: str = "any"
    model_config: ModelConfig = field(default_factory=ModelConfig)
    state: NodeState = NodeState.PENDING
    dependencies: set[str] = field(default_factory=set)
    successors: set[str] = field(default_factory=set)
    priority: float = 0.0
    estimated_tokens: int = 0
    estimated_cost: dict[str, Any] = field(default_factory=dict)
    qos_requirement: Optional[QoS] = None
    resource_requirement: Optional[ResourceReq] = None
    metadata: dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    max_retries: int = 0
    retry_backoff_ms: float = 0.0
    retry_not_before: float = 0.0
    attempt: int = 0
    remaining_dependencies: int = 0
    lease_id: Optional[str] = None
    request_id: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    backtracked_from: Optional[str] = None
    activation_condition: Optional[Callable[[dict], bool]] = None
    parent_decision: Optional[str] = None
    result: Optional[dict] = None

    def is_ready(self) -> bool:
        return self.state == NodeState.READY


@dataclass
class Edge:
    edge_id: str
    source: str
    target: str
    dependency_type: str = "control"
    data_volume_bytes: int = 0
    data_mapping: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    condition: Optional[str] = None


@dataclass
class DynamicDAG:
    dag_id: str
    agent_id: str = ""
    nodes: dict[str, Node] = field(default_factory=dict)
    edges: dict[str, Edge] = field(default_factory=dict)
    state: DAGState = DAGState.CREATED
    priority: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)
    version: int = 1
    run_id: str = field(default_factory=lambda: uuid4().hex)

    def add_node(self, node: Node) -> None:
        if node.node_id in self.nodes:
            raise ValueError(f"Node '{node.node_id}' already exists in DAG '{self.dag_id}'")
        self.nodes[node.node_id] = node

    def remove_node(self, node_id: str) -> Optional[Node]:
        node = self.nodes.pop(node_id, None)
        if node is None:
            return None
        for dep in node.dependencies:
            parent = self.nodes.get(dep)
            if parent:
                parent.successors.discard(node_id)
        for succ in list(node.successors):
            child = self.nodes.get(succ)
            if child:
                child.dependencies.discard(node_id)
        edges_to_remove = [
            eid for eid, edge in self.edges.items()
            if edge.source == node_id or edge.target == node_id
        ]
        for eid in edges_to_remove:
            self.edges.pop(eid, None)
        return node

    def add_edge(self, edge: Edge) -> None:
        if edge.edge_id in self.edges:
            raise ValueError(f"Edge '{edge.edge_id}' already exists in DAG '{self.dag_id}'")
        if edge.source not in self.nodes:
            raise ValueError(f"Source node '{edge.source}' not found")
        if edge.target not in self.nodes:
            raise ValueError(f"Target node '{edge.target}' not found")
        if edge.source == edge.target:
            raise ValueError("A DAG edge cannot connect a node to itself")
        self.edges[edge.edge_id] = edge
        self.nodes[edge.source].successors.add(edge.target)
        self.nodes[edge.target].dependencies.add(edge.source)

    def get_root_nodes(self) -> list[Node]:
        return [node for node in self.nodes.values() if not node.dependencies]

    def get_ready_nodes(self) -> list[Node]:
        return [
            node for node in self.nodes.values()
            if node.state == NodeState.READY
        ]

    def get_pending_nodes(self) -> list[Node]:
        return [
            node for node in self.nodes.values()
            if node.state == NodeState.PENDING
        ]

    def get_active_nodes(self) -> list[Node]:
        return [
            node for node in self.nodes.values()
            if node.state in (
                NodeState.PENDING,
                NodeState.READY,
                NodeState.RUNNING,
                NodeState.BLOCKED,
            )
        ]

    def is_completed(self) -> bool:
        if not self.nodes:
            return False
        return all(
            node.state in (NodeState.COMPLETED, NodeState.SKIPPED)
            for node in self.nodes.values()
        )

    def has_failed(self) -> bool:
        return any(node.state == NodeState.FAILED for node in self.nodes.values())


__all__ = [
    "DAGState",
    "DynamicDAG",
    "Edge",
    "ModelConfig",
    "Node",
    "NodeState",
    "NodeType",
    "QoS",
    "ResourceReq",
]
