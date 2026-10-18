"""DAG registry."""

from __future__ import annotations

from wenbo.dag.model import DAGState, DynamicDAG


class DAGRegistry:
    """In-memory DAG lookup with secondary indexes by agent and lifecycle state."""

    def __init__(self) -> None:
        self._dags: dict[str, DynamicDAG] = {}
        self._by_agent: dict[str, set[str]] = {}
        self._by_state: dict[DAGState, set[str]] = {state: set() for state in DAGState}

    def register(self, dag: DynamicDAG) -> None:
        """Register a graph, replacing and reindexing an existing graph with the same ID."""
        existing = self._dags.get(dag.dag_id)
        if existing is not None:
            self._by_agent.get(existing.agent_id, set()).discard(dag.dag_id)
            for state_set in self._by_state.values():
                state_set.discard(dag.dag_id)
        self._dags[dag.dag_id] = dag
        self._by_agent.setdefault(dag.agent_id, set()).add(dag.dag_id)
        self._by_state[dag.state].add(dag.dag_id)

    def unregister(self, dag_id: str) -> DynamicDAG | None:
        """Remove the graph and its index entries; return None if absent."""
        dag = self._dags.pop(dag_id, None)
        if dag is None:
            return None
        self._by_agent.get(dag.agent_id, set()).discard(dag_id)
        for state_set in self._by_state.values():
            state_set.discard(dag_id)
        return dag

    def update_state(self, dag_id: str, old_state: DAGState, new_state: DAGState) -> None:
        """Move a DAG ID between state indexes without mutating the graph.

        The state tracker must also update dag.state. Callers should only
        use this operation for graphs already registered in this registry.
        """
        if dag_id in self._by_state.get(old_state, set()):
            self._by_state[old_state].discard(dag_id)
        self._by_state[new_state].add(dag_id)

    def get(self, dag_id: str) -> DynamicDAG | None:
        return self._dags.get(dag_id)

    def get_by_agent(self, agent_id: str) -> list[DynamicDAG]:
        ids = self._by_agent.get(agent_id, set())
        return [self._dags[dag_id] for dag_id in ids if dag_id in self._dags]

    def get_by_state(self, state: DAGState) -> list[DynamicDAG]:
        ids = self._by_state.get(state, set())
        return [self._dags[dag_id] for dag_id in ids if dag_id in self._dags]

    def get_active_dags(self) -> list[DynamicDAG]:
        """Return graphs indexed as CREATED, RUNNING, or EVOLVING."""
        active_states = {DAGState.CREATED, DAGState.RUNNING, DAGState.EVOLVING}
        result = []
        for state in active_states:
            result.extend(self.get_by_state(state))
        return result

    def get_all(self) -> list[DynamicDAG]:
        return list(self._dags.values())

    @property
    def size(self) -> int:
        return len(self._dags)


__all__ = ["DAGRegistry"]
