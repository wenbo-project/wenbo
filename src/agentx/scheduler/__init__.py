"""Flat scheduler API compatibility layer."""

from agentx.scheduler.policy import (
    FIFOPolicy,
    FairRoundRobinPolicy,
    FairShortestJobFirstPolicy,
    PriorityPolicy,
    SchedulingPolicy,
    ShortestJobFirstPolicy,
    build_policy,
)
from agentx.scheduler.queue import ReadyQueue
from agentx.scheduler.scheduler import Scheduler
from agentx.scheduler.types import SchedulingCandidate, SchedulingDecision

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
