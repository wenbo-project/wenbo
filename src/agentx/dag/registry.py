"""DAG registry."""

from __future__ import annotations

from typing import Optional

from agentx.dag.model import DAGState, DynamicDAG


class DAGRegistry:
    def __init__(self) -> None:
        self._dags: dict[str, DynamicDAG] = {}
        self._by_agent: dict[str, set[str]] = {}
        self._by_state: dict[DAGState, set[str]] = {
            state: set() for state in DAGState
        }

    def register(self, dag: DynamicDAG) -> None:
        existing = self._dags.get(dag.dag_id)
        if existing is not None:
            self._by_agent.get(existing.agent_id, set()).discard(dag.dag_id)
            for state_set in self._by_state.values():
                state_set.discard(dag.dag_id)
        self._dags[dag.dag_id] = dag
        self._by_agent.setdefault(dag.agent_id, set()).add(dag.dag_id)
        self._by_state[dag.state].add(dag.dag_id)

    def unregister(self, dag_id: str) -> Optional[DynamicDAG]:
        dag = self._dags.pop(dag_id, None)
        if dag is None:
            return None
        self._by_agent.get(dag.agent_id, set()).discard(dag_id)
        for state_set in self._by_state.values():
            state_set.discard(dag_id)
        return dag

    def update_state(self, dag_id: str, old_state: DAGState, new_state: DAGState) -> None:
        if dag_id in self._by_state.get(old_state, set()):
            self._by_state[old_state].discard(dag_id)
        self._by_state[new_state].add(dag_id)

    def get(self, dag_id: str) -> Optional[DynamicDAG]:
        return self._dags.get(dag_id)

    def get_by_agent(self, agent_id: str) -> list[DynamicDAG]:
        ids = self._by_agent.get(agent_id, set())
        return [self._dags[dag_id] for dag_id in ids if dag_id in self._dags]

    def get_by_state(self, state: DAGState) -> list[DynamicDAG]:
        ids = self._by_state.get(state, set())
        return [self._dags[dag_id] for dag_id in ids if dag_id in self._dags]

    def get_active_dags(self) -> list[DynamicDAG]:
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
