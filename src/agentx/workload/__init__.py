"""Workload loaders."""

from agentx.workload.hotpotqa_loader import DAGLoader, DAGSpec
from agentx.workload.synthetic_loader import SyntheticDAGLoader, SyntheticDAGSpec

__all__ = [
    "DAGLoader",
    "DAGSpec",
    "SyntheticDAGLoader",
    "SyntheticDAGSpec",
]
