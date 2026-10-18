"""Admission boundary for already-generated Wenbo DAGs.

The current Director deliberately does not generate a DAG from natural language.
It validates and normalizes a DAG IR before the runtime owns its lifecycle.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from wenbo.dag.model import DynamicDAG
from wenbo.dag.validator import DAGValidator, ValidationError


@dataclass(frozen=True)
class AdmissionLimits:
    """Static limits protecting the in-process simulation from malformed input."""

    max_nodes: int | None = None
    max_edges: int | None = None
    max_priority: int | None = None


class Director:
    """Compile and admit a pre-built DAG without running it."""

    def __init__(
        self,
        validator: DAGValidator | None = None,
        limits: AdmissionLimits | None = None,
    ) -> None:
        self._validator = validator or DAGValidator()
        self._limits = limits or AdmissionLimits()

    def compile(self, dag: DynamicDAG) -> DynamicDAG:
        """Normalize and validate a graph in place, preserving object identity.

        Hardware labels are normalized and incoming transfer sizes are
        recomputed in node metadata. Static admission limits, nonnegative
        estimates, retry settings, and graph structure are validated.

        Args:
            dag: A pre-built DynamicDAG; compilation does not execute its nodes.

        Returns:
            The same graph instance after normalization.

        Raises:
            TypeError: The input is not a DynamicDAG.
            ValidationError: The graph violates an admission or structural constraint.
        """
        if not isinstance(dag, DynamicDAG):
            raise TypeError("Director expects an wenbo.dag.model.DynamicDAG")
        if not dag.dag_id or not str(dag.dag_id).strip():
            raise ValidationError("DAG id must not be empty")
        if dag.version < 1:
            raise ValidationError("DAG version must be at least 1")
        if not dag.run_id or not str(dag.run_id).strip():
            raise ValidationError("DAG run_id must not be empty")
        if not dag.nodes:
            raise ValidationError(f"DAG '{dag.dag_id}' must contain at least one node")

        limits = self._limits
        if limits.max_nodes is not None and len(dag.nodes) > limits.max_nodes:
            raise ValidationError(f"DAG '{dag.dag_id}' exceeds max_nodes={limits.max_nodes}")
        if limits.max_edges is not None and len(dag.edges) > limits.max_edges:
            raise ValidationError(f"DAG '{dag.dag_id}' exceeds max_edges={limits.max_edges}")
        if limits.max_priority is not None and dag.priority > limits.max_priority:
            raise ValidationError(f"DAG '{dag.dag_id}' exceeds max_priority={limits.max_priority}")

        incoming_bytes = {node_id: 0 for node_id in dag.nodes}
        for edge in dag.edges.values():
            incoming_bytes[edge.target] = incoming_bytes.get(edge.target, 0) + max(
                int(edge.data_volume_bytes or 0), 0
            )

        for node in dag.nodes.values():
            node.hardware = str(node.hardware or "any").strip().lower() or "any"
            if node.estimated_tokens < 0:
                raise ValidationError(f"Node '{node.node_id}' has negative estimated_tokens")
            if node.max_retries < 0:
                raise ValidationError(f"Node '{node.node_id}' has negative max_retries")
            if node.retry_backoff_ms < 0:
                raise ValidationError(f"Node '{node.node_id}' has negative retry_backoff_ms")
            resource = node.resource_requirement
            if resource is not None:
                for field_name in (
                    "cpu_cores",
                    "gpu_count",
                    "memory_mb",
                    "gpu_memory_mb",
                    "estimated_gpu_memory_mb",
                    "estimated_compute_flops",
                    "estimated_time_ms",
                ):
                    if getattr(resource, field_name, 0) < 0:
                        raise ValidationError(
                            f"Node '{node.node_id}' has negative resource field '{field_name}'"
                        )
            # This value is derived from the admitted graph.  Recompute it on
            # every compile so stale caller metadata cannot skew simulation
            # cost or placement decisions after the DAG was edited.
            node.metadata["incoming_data_volume_bytes"] = incoming_bytes.get(
                node.node_id,
                0,
            )

        error = self._validator.validate(dag)
        if error:
            raise ValidationError(error)
        return dag

    def admit(self, dag: DynamicDAG, registry: Any) -> DynamicDAG:
        """Compile and register a graph, rejecting an already registered DAG ID.

        Returns the same graph instance. ValidationError reports invalid
        inputs or a duplicate ID; compilation may already have normalized
        the caller's graph when an error is raised.
        """
        # Normalize and validate before registration to keep invalid tasks out of the scheduling queue.
        compiled = self.compile(dag)
        if registry.get(compiled.dag_id) is not None:
            raise ValidationError(f"DAG '{compiled.dag_id}' is already registered")
        registry.register(compiled)
        return compiled


__all__ = ["AdmissionLimits", "Director"]
