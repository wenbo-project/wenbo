"""Scheduler candidate and decision data types."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SchedulingCandidate:
    """Immutable selection record referencing a mutable READY node.

    ready_order is the current eligible queue order, not a timestamp.
    """

    dag_id: str
    agent_id: str
    dag_priority: int
    node_id: str
    node: Any
    node_priority: float
    estimated_tokens: int
    ready_order: int


@dataclass(frozen=True)
class SchedulingDecision:
    """Candidate selection plus policy identity, diagnostic score, and reason.

    List order determines dispatch preference; scores are policy-specific.
    """

    candidate: SchedulingCandidate
    score: float
    policy_name: str
    reason: str = ""
    policy_version: str = "1"

    @property
    def policy_id(self) -> str:
        return f"{self.policy_name}@{self.policy_version}"


__all__ = ["SchedulingCandidate", "SchedulingDecision"]
