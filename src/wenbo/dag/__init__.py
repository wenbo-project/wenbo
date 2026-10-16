"""Flat DAG API."""

from wenbo.dag.builder import DAGBuilder
from wenbo.dag.model import (
    DAGState,
    DynamicDAG,
    Edge,
    ModelConfig,
    Node,
    NodeState,
    NodeType,
    QoS,
    ResourceReq,
)
from wenbo.dag.registry import DAGRegistry
from wenbo.dag.validator import DAGValidator, ValidationError


def __getattr__(name: str):
    if name == "DAGStateTracker":
        from wenbo.runtime.state import DAGStateTracker

        return DAGStateTracker
    raise AttributeError(name)


__all__ = [
    "DAGBuilder",
    "DAGRegistry",
    "DAGState",
    "DAGStateTracker",
    "DAGValidator",
    "DynamicDAG",
    "Edge",
    "ModelConfig",
    "Node",
    "NodeState",
    "NodeType",
    "QoS",
    "ResourceReq",
    "ValidationError",
]
