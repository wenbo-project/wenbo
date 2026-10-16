"""Flat DAG API."""

from agentx.dag.builder import DAGBuilder
from agentx.dag.model import (
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
from agentx.dag.registry import DAGRegistry
from agentx.dag.validator import DAGValidator, ValidationError


def __getattr__(name: str):
    if name == "DAGStateTracker":
        from agentx.runtime.state import DAGStateTracker

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
