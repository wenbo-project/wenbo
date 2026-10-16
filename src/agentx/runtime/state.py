"""Runtime state machine and DAG state tracker."""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

from agentx.dag.model import DAGState, NodeState
from agentx.runtime.event import Event, EventBus, EventType

logger = logging.getLogger(__name__)


class RuntimeStateMachine:
    def __init__(self, event_bus: EventBus, registry: Any) -> None:
        self._event_bus = event_bus
        self._registry = registry

    async def on_dag_submitted(self, dag: Any) -> None:
        if dag.state != DAGState.CREATED:
            return
        dag.state = DAGState.RUNNING
        ready_count = 0
        for node in dag.nodes.values():
            node.remaining_dependencies = sum(
                1
                for dep_id in node.dependencies
                if dep_id in dag.nodes
                and dag.nodes[dep_id].state != NodeState.COMPLETED
            )
        for node in dag.get_root_nodes():
            if node.state in (NodeState.PENDING, NodeState.READY):
                node.state = NodeState.READY
                ready_count += 1
                await self._event_bus.publish(Event(
                    event_type=EventType.NODE_READY,
                    dag_id=dag.dag_id,
                    run_id=dag.run_id,
                    node_id=node.node_id,
                ))
        self._registry.update_state(dag.dag_id, DAGState.CREATED, DAGState.RUNNING)
        logger.info("DAG %s submitted, %d root nodes ready", dag.dag_id, ready_count)

    async def on_node_started(
        self,
        dag: Any,
        node_id: str,
        *,
        attempt: int = 0,
        lease_id: Optional[str] = None,
        worker_id: Optional[str] = None,
    ) -> bool:
        node = dag.nodes.get(node_id)
        if node is None or node.state != NodeState.READY:
            return False
        node.state = NodeState.RUNNING
        if attempt > 0:
            node.attempt = attempt
        node.lease_id = lease_id
        node.retry_not_before = 0.0
        await self._event_bus.publish(Event(
            event_type=EventType.NODE_STARTED,
            dag_id=dag.dag_id,
            run_id=dag.run_id,
            node_id=node_id,
            attempt=node.attempt,
            lease_id=lease_id,
            worker_id=worker_id,
        ))
        return True

    async def on_node_completed(
        self,
        dag: Any,
        node_id: str,
        result: Optional[dict] = None,
        *,
        attempt: int = 0,
        lease_id: Optional[str] = None,
        worker_id: Optional[str] = None,
    ) -> bool:
        node = dag.nodes.get(node_id)
        if node is None or node.state != NodeState.RUNNING:
            return False
        if attempt > 0 and node.attempt != attempt:
            return False
        if lease_id is not None and node.lease_id != lease_id:
            return False
        event_lease_id = node.lease_id
        node.state = NodeState.COMPLETED
        node.request_id = None
        node.lease_id = None
        if result is not None:
            node.result = result
        logger.debug("Node %s completed in DAG %s", node_id, dag.dag_id)

        await self._event_bus.publish(Event(
            event_type=EventType.NODE_COMPLETED,
            dag_id=dag.dag_id,
            run_id=dag.run_id,
            node_id=node_id,
            attempt=node.attempt,
            lease_id=event_lease_id,
            worker_id=worker_id,
            data=result or {},
        ))

        await self._check_successors(dag, node_id, result)

        if dag.is_completed():
            dag.state = DAGState.COMPLETED
            self._registry.update_state(dag.dag_id, DAGState.RUNNING, DAGState.COMPLETED)
            await self._event_bus.publish(Event(
                event_type=EventType.DAG_COMPLETED,
                dag_id=dag.dag_id,
                run_id=dag.run_id,
            ))
            logger.info("DAG %s completed", dag.dag_id)
        elif dag.has_failed():
            dag.state = DAGState.FAILED
            self._registry.update_state(dag.dag_id, DAGState.RUNNING, DAGState.FAILED)
            await self._event_bus.publish(Event(
                event_type=EventType.DAG_FAILED,
                dag_id=dag.dag_id,
                run_id=dag.run_id,
            ))
        return True

    async def on_node_failed(
        self,
        dag: Any,
        node_id: str,
        error: str = "",
        *,
        attempt: int = 0,
        lease_id: Optional[str] = None,
        worker_id: Optional[str] = None,
        retryable: bool = True,
    ) -> bool:
        node = dag.nodes.get(node_id)
        if node is None or node.state != NodeState.RUNNING:
            return False
        if attempt > 0 and node.attempt != attempt:
            return False
        if lease_id is not None and node.lease_id != lease_id:
            return False
        event_lease_id = node.lease_id
        node.state = NodeState.FAILED
        node.retry_count += 1
        node.request_id = None
        logger.warning("Node %s failed in DAG %s: %s", node_id, dag.dag_id, error)
        await self._event_bus.publish(Event(
            event_type=EventType.NODE_FAILED,
            dag_id=dag.dag_id,
            run_id=dag.run_id,
            node_id=node_id,
            attempt=node.attempt,
            lease_id=event_lease_id,
            worker_id=worker_id,
            data={"error": error, "retryable": retryable},
        ))

        if retryable and node.retry_count <= max(int(node.max_retries), 0):
            node.state = NodeState.READY
            node.lease_id = None
            backoff_ms = max(float(node.retry_backoff_ms), 0.0)
            node.retry_not_before = time.monotonic() + backoff_ms / 1000.0
            await self._event_bus.publish(Event(
                event_type=EventType.NODE_READY,
                dag_id=dag.dag_id,
                run_id=dag.run_id,
                node_id=node_id,
                attempt=node.attempt + 1,
                data={
                    "retry": node.retry_count,
                    "backoff_ms": backoff_ms,
                },
            ))
            return True

        dag.state = DAGState.FAILED
        self._registry.update_state(dag.dag_id, DAGState.RUNNING, DAGState.FAILED)
        node.lease_id = None
        await self._cancel_unstarted_nodes(dag, "DAG failed")
        await self._event_bus.publish(Event(
            event_type=EventType.DAG_FAILED,
            dag_id=dag.dag_id,
            run_id=dag.run_id,
            data={"error": error},
        ))
        return True

    async def on_node_cancelled(
        self,
        dag: Any,
        node_id: str,
        reason: str = "",
        *,
        attempt: int = 0,
        lease_id: Optional[str] = None,
        worker_id: Optional[str] = None,
    ) -> bool:
        node = dag.nodes.get(node_id)
        if node is None or node.state not in (
            NodeState.PENDING,
            NodeState.READY,
            NodeState.RUNNING,
            NodeState.BLOCKED,
        ):
            return False
        if attempt > 0 and node.attempt != attempt:
            return False
        if lease_id is not None and node.lease_id != lease_id:
            return False
        event_lease_id = node.lease_id
        node.state = NodeState.CANCELLED
        node.request_id = None
        node.lease_id = None
        await self._event_bus.publish(Event(
            event_type=EventType.NODE_CANCELLED,
            dag_id=dag.dag_id,
            run_id=dag.run_id,
            node_id=node_id,
            attempt=node.attempt,
            lease_id=event_lease_id,
            worker_id=worker_id,
            data={"reason": reason},
        ))
        return True

    async def _cancel_unstarted_nodes(self, dag: Any, reason: str) -> None:
        for node in dag.nodes.values():
            if node.state in (NodeState.PENDING, NodeState.READY, NodeState.BLOCKED):
                await self.on_node_cancelled(dag, node.node_id, reason)

    async def on_dag_cancelled(self, dag: Any, reason: str = "") -> bool:
        """Cancel all unfinished nodes and publish one terminal DAG event."""
        if dag.state in (DAGState.COMPLETED, DAGState.FAILED, DAGState.CANCELLED):
            return False
        old_state = dag.state
        for node in dag.nodes.values():
            if node.state not in (
                NodeState.PENDING,
                NodeState.READY,
                NodeState.RUNNING,
                NodeState.BLOCKED,
            ):
                continue
            await self.on_node_cancelled(dag, node.node_id, reason)
        dag.state = DAGState.CANCELLED
        self._registry.update_state(dag.dag_id, old_state, DAGState.CANCELLED)
        await self._event_bus.publish(Event(
            event_type=EventType.DAG_CANCELLED,
            dag_id=dag.dag_id,
            run_id=dag.run_id,
            data={"reason": reason},
        ))
        return True

    async def on_node_unschedulable(
        self,
        dag: Any,
        node_id: str,
        reason: str,
    ) -> bool:
        """Fail a READY/PENDING node when no registered capability supports it."""
        node = dag.nodes.get(node_id)
        if node is None or node.state not in (
            NodeState.PENDING,
            NodeState.READY,
            NodeState.BLOCKED,
        ):
            return False
        if dag.state != DAGState.RUNNING:
            return False
        node.state = NodeState.FAILED
        node.request_id = None
        node.lease_id = None
        node.result = {"error": reason, "unschedulable": True}
        await self._event_bus.publish(Event(
            event_type=EventType.NODE_FAILED,
            dag_id=dag.dag_id,
            run_id=dag.run_id,
            node_id=node_id,
            attempt=node.attempt,
            data={"error": reason, "unschedulable": True},
        ))
        dag.state = DAGState.FAILED
        self._registry.update_state(dag.dag_id, DAGState.RUNNING, DAGState.FAILED)
        await self._cancel_unstarted_nodes(dag, "DAG contains an unschedulable node")
        await self._event_bus.publish(Event(
            event_type=EventType.DAG_FAILED,
            dag_id=dag.dag_id,
            run_id=dag.run_id,
            data={"error": reason, "unschedulable": True},
        ))
        return True

    async def _check_successors(
        self,
        dag: Any,
        completed_node_id: str,
        result: Optional[dict] = None,
    ) -> None:
        completed_node = dag.nodes[completed_node_id]
        for succ_id in list(completed_node.successors):
            succ = dag.nodes.get(succ_id)
            if succ is None or succ.state != NodeState.PENDING:
                continue

            if succ.remaining_dependencies > 0:
                succ.remaining_dependencies -= 1

            if succ.remaining_dependencies != 0:
                continue

            if succ.activation_condition is not None:
                decision = "activate" if succ.activation_condition(result or {}) else "skip"
                succ.parent_decision = decision
                if decision == "skip":
                    succ.state = NodeState.SKIPPED
                    await self._event_bus.publish(Event(
                        event_type=EventType.NODE_SKIPPED,
                        dag_id=dag.dag_id,
                        run_id=dag.run_id,
                        node_id=succ_id,
                    ))
                    await self._check_successors(dag, succ_id)
                    continue

            succ.state = NodeState.READY
            await self._event_bus.publish(Event(
                event_type=EventType.NODE_READY,
                dag_id=dag.dag_id,
                run_id=dag.run_id,
                node_id=succ_id,
            ))


class DAGStateTracker:
    def __init__(self, event_bus: EventBus, registry: Any) -> None:
        self._state_machine = RuntimeStateMachine(event_bus, registry)

    async def on_dag_submitted(self, dag: Any) -> None:
        await self._state_machine.on_dag_submitted(dag)

    async def on_node_started(
        self,
        dag: Any,
        node_id: str,
        *,
        attempt: int = 0,
        lease_id: Optional[str] = None,
        worker_id: Optional[str] = None,
    ) -> bool:
        return await self._state_machine.on_node_started(
            dag,
            node_id,
            attempt=attempt,
            lease_id=lease_id,
            worker_id=worker_id,
        )

    async def on_node_completed(
        self,
        dag: Any,
        node_id: str,
        result: Optional[dict] = None,
        *,
        attempt: int = 0,
        lease_id: Optional[str] = None,
        worker_id: Optional[str] = None,
    ) -> bool:
        return await self._state_machine.on_node_completed(
            dag,
            node_id,
            result,
            attempt=attempt,
            lease_id=lease_id,
            worker_id=worker_id,
        )

    async def on_node_failed(
        self,
        dag: Any,
        node_id: str,
        error: str = "",
        *,
        attempt: int = 0,
        lease_id: Optional[str] = None,
        worker_id: Optional[str] = None,
        retryable: bool = True,
    ) -> bool:
        return await self._state_machine.on_node_failed(
            dag,
            node_id,
            error,
            attempt=attempt,
            lease_id=lease_id,
            worker_id=worker_id,
            retryable=retryable,
        )

    async def on_dag_cancelled(self, dag: Any, reason: str = "") -> bool:
        return await self._state_machine.on_dag_cancelled(dag, reason)

    async def on_node_cancelled(
        self,
        dag: Any,
        node_id: str,
        reason: str = "",
        *,
        attempt: int = 0,
        lease_id: Optional[str] = None,
        worker_id: Optional[str] = None,
    ) -> bool:
        return await self._state_machine.on_node_cancelled(
            dag,
            node_id,
            reason,
            attempt=attempt,
            lease_id=lease_id,
            worker_id=worker_id,
        )

    async def on_node_unschedulable(
        self,
        dag: Any,
        node_id: str,
        reason: str,
    ) -> bool:
        return await self._state_machine.on_node_unschedulable(dag, node_id, reason)


__all__ = ["DAGStateTracker", "RuntimeStateMachine"]
