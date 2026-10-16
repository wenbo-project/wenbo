"""Flat scheduler API compatibility layer."""

from wenbo.scheduler.policy import (
    FIFOPolicy,
    FairRoundRobinPolicy,
    FairShortestJobFirstPolicy,
    PriorityPolicy,
    SchedulingPolicy,
    ShortestJobFirstPolicy,
    build_policy,
)
from wenbo.scheduler.queue import ReadyQueue
from wenbo.scheduler.scheduler import Scheduler
from wenbo.scheduler.types import SchedulingCandidate, SchedulingDecision

__all__ = [
    "FIFOPolicy",
    "FairRoundRobinPolicy",
    "FairShortestJobFirstPolicy",
    "PriorityPolicy",
    "ReadyQueue",
    "Scheduler",
    "SchedulingCandidate",
    "SchedulingDecision",
    "SchedulingPolicy",
    "ShortestJobFirstPolicy",
    "build_policy",
]
