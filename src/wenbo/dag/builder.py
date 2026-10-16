"""DAG builder."""

from __future__ import annotations

import logging
from typing import Optional

from wenbo.dag.model import DynamicDAG, Edge, ModelConfig, Node, NodeState, NodeType, ResourceReq
from wenbo.dag.validator import DAGValidator, ValidationError

logger = logging.getLogger(__name__)


class DAGBuilder:
    _edge_counter = 0

    def __init__(self, validator: Optional[DAGValidator] = None) -> None:
        self._validator = validator or DAGValidator()

    def create_dag(
        self,
        dag_id: str,
        agent_id: str = "",
        priority: int = 0,
        metadata: Optional[dict] = None,
        version: int = 1,
        run_id: Optional[str] = None,
    ) -> DynamicDAG:
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
        model_config: Optional[ModelConfig] = None,
        dependencies: Optional[list[str]] = None,
        priority: float = 0.0,
        estimated_tokens: int = 0,
        estimated_cost: Optional[dict] = None,
        resource_requirement: Optional[ResourceReq] = None,
        metadata: Optional[dict] = None,
        activation_condition=None,
        max_retries: int = 0,
        retry_backoff_ms: float = 0.0,
    ) -> Node:
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
        # 没有前置依赖的节点可直接就绪，其余节点等待上游完成。
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
            raise ValueError(
                f"Dependency node '{missing[0]}' not found while adding '{node_id}'"
            )
        edge_ids = [f"e_{dep_id}_{node_id}" for dep_id in node.dependencies]
        collisions = [edge_id for edge_id in edge_ids if edge_id in dag.edges]
        if collisions:
            raise ValueError(
                f"Edge '{collisions[0]}' already exists in DAG '{dag.dag_id}'"
            )

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
        edge_id: Optional[str] = None,
        dependency_type: str = "control",
        data_volume_bytes: int = 0,
        data_mapping: Optional[dict] = None,
        metadata: Optional[dict] = None,
    ) -> None:
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
        error = self._validator.validate(dag)
        if error:
            raise ValidationError(error)


__all__ = ["DAGBuilder"]
