"""Candidate builder for scheduler-ready nodes."""

from __future__ import annotations

from typing import Any

from wenbo.scheduler.queue import ReadyQueue
from wenbo.scheduler.types import SchedulingCandidate


class CandidateBuilder:
    def __init__(self, registry: Any, ready_queue: ReadyQueue) -> None:
        self._registry = registry
        self._ready_queue = ready_queue

    def build(self) -> list[SchedulingCandidate]:
        return self._ready_queue.list_candidates(self._registry)


__all__ = ["CandidateBuilder"]
