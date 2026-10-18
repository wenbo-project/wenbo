"""Scheduling policies used by the runtime scheduler."""

from __future__ import annotations

from abc import ABC, abstractmethod

from wenbo.scheduler.types import SchedulingCandidate, SchedulingDecision


# Policies order candidate nodes; Scheduler handles resource allocation.
class SchedulingPolicy(ABC):
    """Order eligible nodes without reserving workers or mutating graph state.

    Custom policies implement select and provide a stable name/version
    so decisions can be identified in recorded traces.
    """

    name = "unknown"
    version = "1"

    @property
    def policy_id(self) -> str:
        return f"{self.name}@{self.version}"

    @abstractmethod
    def select(
        self,
        candidates: list[SchedulingCandidate],
        available_slots: int,
    ) -> list[SchedulingDecision]:
        """Return ordered decisions for at most available_slots candidates.

        Args:
            candidates: Eligible nodes in ready-queue order.
            available_slots: Nonnegative dispatch budget supplied by Scheduler.

        Returns:
            Decisions referring to input candidates; placement follows selection.
        """

    def select_nodes(
        self,
        candidates: list[SchedulingCandidate],
        available_slots: int,
    ) -> list[SchedulingCandidate]:
        """Select candidate nodes without scheduling decision metadata."""
        return [decision.candidate for decision in self.select(candidates, available_slots)]


class FIFOPolicy(SchedulingPolicy):
    """Select candidates in the ready-queue order supplied by the scheduler."""

    name = "fifo"

    def select(
        self,
        candidates: list[SchedulingCandidate],
        available_slots: int,
    ) -> list[SchedulingDecision]:
        selected = candidates[:available_slots]
        return [
            SchedulingDecision(
                candidate=candidate,
                score=float(-candidate.ready_order),
                policy_name=self.name,
                reason="earliest ready candidate",
                policy_version=self.version,
            )
            for candidate in selected
        ]


class ShortestJobFirstPolicy(SchedulingPolicy):
    """Prefer smaller positive token estimates; place missing or nonpositive estimates last."""

    name = "sjf"

    def select(
        self,
        candidates: list[SchedulingCandidate],
        available_slots: int,
    ) -> list[SchedulingDecision]:
        ranked = sorted(
            candidates,
            key=lambda c: (
                c.estimated_tokens if c.estimated_tokens > 0 else float("inf"),
                c.ready_order,
            ),
        )
        selected = ranked[:available_slots]
        return [
            SchedulingDecision(
                candidate=candidate,
                score=float(
                    -(candidate.estimated_tokens if candidate.estimated_tokens > 0 else 10**9)
                ),
                policy_name=self.name,
                reason="smaller estimated token cost",
                policy_version=self.version,
            )
            for candidate in selected
        ]


class PriorityPolicy(SchedulingPolicy):
    """Prefer higher DAG priority, then node priority, then earlier ready order."""

    name = "priority"

    def select(
        self,
        candidates: list[SchedulingCandidate],
        available_slots: int,
    ) -> list[SchedulingDecision]:
        ranked = sorted(
            candidates,
            key=lambda c: (-c.dag_priority, -c.node_priority, c.ready_order),
        )
        selected = ranked[:available_slots]
        return [
            SchedulingDecision(
                candidate=candidate,
                score=float(candidate.dag_priority * 1000 + candidate.node_priority),
                policy_name=self.name,
                reason="higher dag/node priority",
                policy_version=self.version,
            )
            for candidate in selected
        ]


class FairRoundRobinPolicy(SchedulingPolicy):
    """Interleave DAGs in their first-appearance order within each select call.

    No round-robin cursor is retained between calls; this policy does
    not guarantee fairness across scheduling rounds.
    """

    name = "fair_rr"

    def select(
        self,
        candidates: list[SchedulingCandidate],
        available_slots: int,
    ) -> list[SchedulingDecision]:
        if available_slots <= 0 or not candidates:
            return []

        candidates_by_dag: dict[str, list[SchedulingCandidate]] = {}
        dag_order: list[str] = []
        for candidate in candidates:
            if candidate.dag_id not in candidates_by_dag:
                candidates_by_dag[candidate.dag_id] = []
                dag_order.append(candidate.dag_id)
            candidates_by_dag[candidate.dag_id].append(candidate)

        selected: list[SchedulingDecision] = []
        while len(selected) < available_slots:
            made_progress = False
            for dag_id in dag_order:
                dag_candidates = candidates_by_dag[dag_id]
                if not dag_candidates:
                    continue
                candidate = dag_candidates.pop(0)
                selected.append(
                    SchedulingDecision(
                        candidate=candidate,
                        score=float(-candidate.ready_order),
                        policy_name=self.name,
                        reason="round-robin across dags",
                        policy_version=self.version,
                    )
                )
                made_progress = True
                if len(selected) >= available_slots:
                    break
            if not made_progress:
                break

        return selected


class FairShortestJobFirstPolicy(SchedulingPolicy):
    """Interleave DAGs per select call, sorting each DAG by positive token estimate."""

    name = "fair_sjf"

    def select(
        self,
        candidates: list[SchedulingCandidate],
        available_slots: int,
    ) -> list[SchedulingDecision]:
        if available_slots <= 0 or not candidates:
            return []

        candidates_by_dag: dict[str, list[SchedulingCandidate]] = {}
        dag_order: list[str] = []
        for candidate in candidates:
            if candidate.dag_id not in candidates_by_dag:
                candidates_by_dag[candidate.dag_id] = []
                dag_order.append(candidate.dag_id)
            candidates_by_dag[candidate.dag_id].append(candidate)

        for dag_candidates in candidates_by_dag.values():
            dag_candidates.sort(
                key=lambda c: (
                    c.estimated_tokens if c.estimated_tokens > 0 else float("inf"),
                    c.ready_order,
                )
            )

        selected: list[SchedulingDecision] = []
        while len(selected) < available_slots:
            made_progress = False
            for dag_id in dag_order:
                dag_candidates = candidates_by_dag[dag_id]
                if not dag_candidates:
                    continue
                candidate = dag_candidates.pop(0)
                estimated = candidate.estimated_tokens if candidate.estimated_tokens > 0 else 10**9
                selected.append(
                    SchedulingDecision(
                        candidate=candidate,
                        score=float(-estimated),
                        policy_name=self.name,
                        reason="round-robin across dags, shortest job within dag",
                        policy_version=self.version,
                    )
                )
                made_progress = True
                if len(selected) >= available_slots:
                    break
            if not made_progress:
                break

        return selected


POLICY_TYPES: dict[str, type[SchedulingPolicy]] = {
    policy_type.name: policy_type
    for policy_type in (
        FIFOPolicy,
        FairRoundRobinPolicy,
        FairShortestJobFirstPolicy,
        PriorityPolicy,
        ShortestJobFirstPolicy,
    )
}


def build_policy(name: str) -> SchedulingPolicy:
    """Construct a built-in policy from a case-insensitive name.

    Supported names are fifo, sjf, priority, fair_rr, and fair_sjf.
    Whitespace is stripped. Unknown names raise ValueError.
    """
    normalized = str(name).strip().lower()
    try:
        return POLICY_TYPES[normalized]()
    except KeyError as exc:
        supported = ", ".join(sorted(POLICY_TYPES))
        raise ValueError(
            f"unsupported scheduling policy {name!r}; expected one of: {supported}"
        ) from exc


__all__ = [
    "FIFOPolicy",
    "FairRoundRobinPolicy",
    "FairShortestJobFirstPolicy",
    "POLICY_TYPES",
    "PriorityPolicy",
    "SchedulingPolicy",
    "ShortestJobFirstPolicy",
    "build_policy",
]
