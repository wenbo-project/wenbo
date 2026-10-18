"""DAG data model."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from uuid import uuid4


class NodeType(str, Enum):
    """Semantic node categories; the backend determines how requests execute."""

    LLM_INFER = "llm_infer"
    TOOL_CALL = "tool_call"
    RETRIEVAL = "retrieval"
    AGGREGATE = "aggregate"
    CONDITION = "condition"
    BACKTRACK = "backtrack"
    CUSTOM = "custom"


class NodeState(str, Enum):
    """Node lifecycle states shared by the scheduler and state tracker."""

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
    """Workflow lifecycle states recorded in the registry."""

    CREATED = "created"
    RUNNING = "running"
    EVOLVING = "evolving"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ModelConfig:
    """Model identifier and sampling parameters forwarded to the backend."""

    model: str = "qwen2.5-7b"
    sampling_params: dict[str, Any] = field(
        default_factory=lambda: {
            "temperature": 0.7,
            "top_p": 0.9,
            "max_tokens": 512,
        }
    )


@dataclass
class QoS:
    """Optional deadline in milliseconds and priority metadata.

    These fields describe caller intent; they do not enforce a deadline guarantee.
    """

    slo_deadline_ms: float | None = None
    priority: int = 0


@dataclass
class ResourceReq:
    """Declared resource needs and execution-cost estimates.

    Memory fields use megabytes, time uses milliseconds, and compute uses
    floating-point operations. Zero denotes an unspecified estimate.
    Worker placement checks hardware, declared CPU cores, and GPU memory;
    the remaining fields are available to backends and experiments.
    """

    cpu_cores: float = 0.0
    gpu_count: int = 0
    memory_mb: float = 0.0
    gpu_memory_mb: float = 0.0
    estimated_gpu_memory_mb: float = 0.0
    estimated_compute_flops: float = 0.0
    estimated_time_ms: float = 0.0


@dataclass
class Node:
    """One execution step, including its mutable runtime state.

    Attributes:
        dependencies: IDs of direct predecessor nodes in the same DAG.
        successors: IDs of direct successor nodes in the same DAG.
        hardware: Placement label, normally ``any``, ``cpu``, or ``gpu``.
        estimated_tokens: Structured token estimate used by SJF policies.
        max_retries: Additional attempts allowed after the initial failure.
        retry_backoff_ms: Fixed delay before a retry becomes eligible.
        retry_not_before: Retry eligibility time from ``time.monotonic()``.
        attempt: Execution generation used with lease_id to reject stale results.
        result: Backend result mapping, including output when available.

    Build nodes and edges before submission. The runtime owns lifecycle
    fields after admission; direct edits bypass its events and counters.
    """

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
    qos_requirement: QoS | None = None
    resource_requirement: ResourceReq | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    max_retries: int = 0
    retry_backoff_ms: float = 0.0
    retry_not_before: float = 0.0
    attempt: int = 0
    remaining_dependencies: int = 0
    lease_id: str | None = None
    request_id: str | None = None
    created_at: float = field(default_factory=time.time)
    backtracked_from: str | None = None
    activation_condition: Callable[[dict], bool] | None = None
    parent_decision: str | None = None
    result: dict | None = None

    def is_ready(self) -> bool:
        return self.state == NodeState.READY


@dataclass
class Edge:
    """Directed dependency with optional transfer and application metadata.

    ``data_volume_bytes`` is a cost estimate in bytes. The prompt resolver
    uses predecessor-output placeholders; it does not interpret data_mapping.
    """

    edge_id: str
    source: str
    target: str
    dependency_type: str = "control"
    data_volume_bytes: int = 0
    data_mapping: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    condition: str | None = None


@dataclass
class DynamicDAG:
    """Mutable workflow graph and state for one execution run.

    Nodes and edges are indexed by ID. ``run_id`` distinguishes an execution
    and ``version`` identifies the graph revision. Mutation helpers maintain
    graph references; use DAGValidator or Director for whole-graph validation.
    """

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
        """Insert a node, raising ValueError if its ID already exists."""
        if node.node_id in self.nodes:
            raise ValueError(f"Node '{node.node_id}' already exists in DAG '{self.dag_id}'")
        self.nodes[node.node_id] = node

    def remove_node(self, node_id: str) -> Node | None:
        """Remove a node and its incident edges; return None if absent."""
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
            eid
            for eid, edge in self.edges.items()
            if edge.source == node_id or edge.target == node_id
        ]
        for eid in edges_to_remove:
            self.edges.pop(eid, None)
        return node

    def add_edge(self, edge: Edge) -> None:
        """Insert an edge and update both endpoint dependency sets.

        Raises:
            ValueError: The edge ID exists, an endpoint is missing, or it is a self-loop.

        This method does not check for cycles spanning multiple edges.
        """
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
        return [node for node in self.nodes.values() if node.state == NodeState.READY]

    def get_pending_nodes(self) -> list[Node]:
        return [node for node in self.nodes.values() if node.state == NodeState.PENDING]

    def get_active_nodes(self) -> list[Node]:
        """Return nodes in pending, ready, running, or blocked state."""
        return [
            node
            for node in self.nodes.values()
            if node.state
            in (
                NodeState.PENDING,
                NodeState.READY,
                NodeState.RUNNING,
                NodeState.BLOCKED,
            )
        ]

    def is_completed(self) -> bool:
        """Return whether a nonempty graph contains only completed or skipped nodes."""
        if not self.nodes:
            return False
        return all(
            node.state in (NodeState.COMPLETED, NodeState.SKIPPED) for node in self.nodes.values()
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
