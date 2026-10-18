"""DAG builder."""

from __future__ import annotations

import logging

from wenbo.dag.model import DynamicDAG, Edge, ModelConfig, Node, NodeState, NodeType, ResourceReq
from wenbo.dag.validator import DAGValidator, ValidationError

logger = logging.getLogger(__name__)


class DAGBuilder:
    """Construct graphs while maintaining edges and dependency references."""

    _edge_counter = 0

    def __init__(self, validator: DAGValidator | None = None) -> None:
        self._validator = validator or DAGValidator()

    def create_dag(
        self,
        dag_id: str,
        agent_id: str = "",
        priority: int = 0,
        metadata: dict | None = None,
        version: int = 1,
        run_id: str | None = None,
    ) -> DynamicDAG:
        """Create an empty graph in CREATED state.

        An omitted agent_id uses dag_id. An omitted run_id receives a UUID.
        Call validate after construction and submit through Scheduler for admission.
        """
        kwargs = {
            "dag_id": dag_id,
            "agent_id": agent_id or dag_id,
            "priority": priority,
            "metadata": metadata or {},
            "version": version,
        }
        if run_id is not None:
            kwargs["run_id"] = run_id
        return DynamicDAG(
            **kwargs,
        )

    def add_node(
        self,
        dag: DynamicDAG,
        node_id: str,
        node_type: NodeType = NodeType.LLM_INFER,
        prompt: str = "",
        hardware: str = "any",
        model_config: ModelConfig | None = None,
        dependencies: list[str] | None = None,
        priority: float = 0.0,
        estimated_tokens: int = 0,
        estimated_cost: dict | None = None,
        resource_requirement: ResourceReq | None = None,
        metadata: dict | None = None,
        activation_condition=None,
        max_retries: int = 0,
        retry_backoff_ms: float = 0.0,
    ) -> Node:
        """Add a node and control edges from its existing predecessors.

        Args:
            dag: Graph to mutate before submission.
            node_id: Unique node identifier within this graph.
            dependencies: Predecessor IDs; every predecessor must already exist.
            hardware: Placement label, independent of node_type.
            estimated_tokens: Token estimate used for scheduling, not prompt length.
            max_retries: Number of additional attempts after a retryable failure.
            retry_backoff_ms: Fixed retry delay in milliseconds.

        Returns:
            The inserted node, READY if it has no dependencies and PENDING otherwise.

        Raises:
            ValueError: A node ID or generated edge ID collides, or a dependency is missing.
                These checks run before the graph is mutated.
        """
        node = Node(
            node_id=node_id,
            node_type=node_type,
            prompt=prompt,
            hardware=hardware,
            model_config=model_config or ModelConfig(),
            dependencies=set(dependencies) if dependencies else set(),
            priority=priority,
            estimated_tokens=estimated_tokens,
            estimated_cost=estimated_cost or {},
            resource_requirement=resource_requirement,
            metadata=metadata or {},
            activation_condition=activation_condition,
            max_retries=max_retries,
            retry_backoff_ms=retry_backoff_ms,
        )
        # Nodes without predecessors are ready immediately; others await upstream completion.
        if not node.dependencies:
            node.state = NodeState.READY
        else:
            node.state = NodeState.PENDING

        # Validate every dependency and generated edge before mutating the DAG.
        # This keeps a failed builder call atomic: callers never get a partially
        # inserted node when an input dependency or edge id is invalid.
        if node_id in dag.nodes:
            raise ValueError(f"Node '{node_id}' already exists in DAG '{dag.dag_id}'")
        missing = [dep_id for dep_id in node.dependencies if dep_id not in dag.nodes]
        if missing:
            raise ValueError(f"Dependency node '{missing[0]}' not found while adding '{node_id}'")
        edge_ids = [f"e_{dep_id}_{node_id}" for dep_id in node.dependencies]
        collisions = [edge_id for edge_id in edge_ids if edge_id in dag.edges]
        if collisions:
            raise ValueError(f"Edge '{collisions[0]}' already exists in DAG '{dag.dag_id}'")

        dag.add_node(node)
        for dep_id in node.dependencies:
            edge_id = f"e_{dep_id}_{node_id}"
            dag.add_edge(
                Edge(
                    edge_id=edge_id,
                    source=dep_id,
                    target=node_id,
                )
            )
        return node

    def add_dependency(
        self,
        dag: DynamicDAG,
        source_id: str,
        target_id: str,
        edge_id: str | None = None,
        dependency_type: str = "control",
        data_volume_bytes: int = 0,
        data_mapping: dict | None = None,
        metadata: dict | None = None,
    ) -> None:
        """Add a directed dependency between existing nodes.

        The default edge ID is ``e_{source_id}_{target_id}``. Reusing an edge
        ID with the same endpoints is a no-op; conflicting endpoints raise
        ValueError. A READY target becomes PENDING. Transfer size is in bytes.
        Call validate to detect cycles after all edges have been added.
        """
        source = dag.nodes.get(source_id)
        target = dag.nodes.get(target_id)
        if source is None:
            raise ValueError(f"Source node '{source_id}' not found")
        if target is None:
            raise ValueError(f"Target node '{target_id}' not found")

        edge = Edge(
            edge_id=edge_id or f"e_{source_id}_{target_id}",
            source=source_id,
            target=target_id,
            dependency_type=dependency_type,
            data_volume_bytes=data_volume_bytes,
            data_mapping=data_mapping or {},
            metadata=metadata or {},
        )
        existing = dag.edges.get(edge.edge_id)
        if existing is not None:
            if existing.source != source_id or existing.target != target_id:
                raise ValueError(f"Edge '{edge.edge_id}' already exists with different endpoints")
            return
        dag.add_edge(edge)

        if target.state == NodeState.READY:
            target.state = NodeState.PENDING

    def validate(self, dag: DynamicDAG) -> None:
        """Raise ValidationError for a cycle or inconsistent dependency references."""
        error = self._validator.validate(dag)
        if error:
            raise ValidationError(error)


__all__ = ["DAGBuilder"]
