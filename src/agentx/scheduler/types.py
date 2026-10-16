"""Scheduler candidate and decision data types."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SchedulingCandidate:
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
    candidate: SchedulingCandidate
    score: float
    policy_name: str
    reason: str = ""
    policy_version: str = "1"

    @property
    def policy_id(self) -> str:
        return f"{self.policy_name}@{self.policy_version}"


__all__ = ["SchedulingCandidate", "SchedulingDecision"]
