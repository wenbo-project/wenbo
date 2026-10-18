from __future__ import annotations

import asyncio

import pytest

from wenbo.dag.builder import DAGBuilder
from wenbo.dag.model import DAGState, NodeState
from wenbo.dag.registry import DAGRegistry
from wenbo.dag.validator import DAGValidator
from wenbo.runtime.event import EventBus, EventType
from wenbo.runtime.state import DAGStateTracker


def setup_graph(count=1, *, chain=False, skip=False, retries=0):
    builder = DAGBuilder()
    dag = builder.create_dag("regression")
    for i in range(count):
        builder.add_node(
            dag,
            str(i),
            dependencies=[str(i - 1)] if chain and i else None,
            activation_condition=(lambda result: False) if skip and i else None,
            max_retries=retries,
        )
    registry = DAGRegistry()
    registry.register(dag)
    bus = EventBus()
    tracker = DAGStateTracker(bus, registry)
    return dag, registry, bus, tracker


def test_deep_graph_validation_and_cycle_detection():
    dag, _, _, _ = setup_graph(1500, chain=True)
    assert DAGValidator.validate(dag) is None
    dag.nodes["1499"].successors.add("0")
    dag.nodes["0"].dependencies.add("1499")
    assert "Cycle detected" in DAGValidator.validate(dag)


def test_deep_skipped_chain():
    async def run():
        dag, registry, _, tracker = setup_graph(1500, chain=True, skip=True)
        await tracker.on_dag_submitted(dag)
        assert await tracker.on_node_started(dag, "0")
        assert await tracker.on_node_completed(dag, "0", {"output": "ok"})
        assert dag.state == DAGState.COMPLETED
        assert len(registry.get_by_state(DAGState.COMPLETED)) == 1
        assert all(n.state == NodeState.SKIPPED for k, n in dag.nodes.items() if k != "0")

    asyncio.run(run())


@pytest.mark.parametrize(
    "event_type",
    [EventType.NODE_READY, EventType.NODE_STARTED, EventType.NODE_FAILED, EventType.NODE_COMPLETED],
)
def test_reentrant_cancellation_stays_terminal(event_type):
    async def run():
        dag, registry, bus, tracker = setup_graph(2, chain=True, retries=1)

        async def cancel(event):
            await tracker.on_dag_cancelled(dag)

        bus.subscribe(event_type, cancel)
        await tracker.on_dag_submitted(dag)
        if dag.state == DAGState.RUNNING:
            await tracker.on_node_started(dag, "0")
        if dag.state == DAGState.RUNNING:
            if event_type == EventType.NODE_FAILED:
                await tracker.on_node_failed(dag, "0")
            else:
                await tracker.on_node_completed(dag, "0")
        assert dag.state == DAGState.CANCELLED
        assert not registry.get_by_state(DAGState.RUNNING)
        assert not registry.get_by_state(DAGState.CREATED)
        assert not any(n.state == NodeState.READY for n in dag.nodes.values())

    asyncio.run(run())


def test_late_completion_does_not_publish_duplicate_failure():
    async def run():
        dag, _, bus, tracker = setup_graph(2)
        failures = []

        async def record(event):
            failures.append(event)

        bus.subscribe(EventType.DAG_FAILED, record)
        await tracker.on_dag_submitted(dag)
        await tracker.on_node_started(dag, "0")
        await tracker.on_node_started(dag, "1")
        await tracker.on_node_failed(dag, "0")
        assert not await tracker.on_node_completed(dag, "1")
        assert len(failures) == 1
        assert dag.state == DAGState.FAILED

    asyncio.run(run())


def test_retry_and_stale_completion():
    async def run():
        dag, _, _, tracker = setup_graph(retries=1)
        await tracker.on_dag_submitted(dag)
        await tracker.on_node_started(dag, "0", attempt=1, lease_id="old")
        await tracker.on_node_failed(dag, "0", attempt=1, lease_id="old")
        assert dag.nodes["0"].state == NodeState.READY
        await tracker.on_node_started(dag, "0", attempt=2, lease_id="new")
        assert not await tracker.on_node_completed(dag, "0", attempt=1, lease_id="old")
        assert await tracker.on_node_completed(dag, "0", attempt=2, lease_id="new")
        assert dag.state == DAGState.COMPLETED

    asyncio.run(run())


def test_peer_completion_during_retry_does_not_fail_graph():
    async def run():
        dag, _, bus, tracker = setup_graph(2, retries=1)

        async def finish_peer(event):
            await tracker.on_node_completed(dag, "1")

        bus.subscribe(EventType.NODE_FAILED, finish_peer)
        await tracker.on_dag_submitted(dag)
        await tracker.on_node_started(dag, "0")
        await tracker.on_node_started(dag, "1")
        await tracker.on_node_failed(dag, "0")
        assert dag.state == DAGState.RUNNING
        assert dag.nodes["0"].state == NodeState.READY
        await tracker.on_node_started(dag, "0")
        await tracker.on_node_completed(dag, "0")
        assert dag.state == DAGState.COMPLETED

    asyncio.run(run())
