"""Async event bus and event types."""

from __future__ import annotations

import asyncio
import logging
from uuid import uuid4
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum
from time import time
from typing import Any, Optional

logger = logging.getLogger(__name__)


class EventType(str, Enum):
    DAG_SUBMITTED = "dag.submitted"
    DAG_COMPLETED = "dag.completed"
    DAG_FAILED = "dag.failed"
    DAG_CANCELLED = "dag.cancelled"
    DAG_STRUCTURE_CHANGED = "dag.structure_changed"

    NODE_READY = "node.ready"
    NODE_STARTED = "node.started"
    NODE_COMPLETED = "node.completed"
    NODE_FAILED = "node.failed"
    NODE_PREEMPTED = "node.preempted"
    NODE_CANCELLED = "node.cancelled"
    NODE_SKIPPED = "node.skipped"

    RESOURCE_ALLOCATED = "resource.allocated"
    RESOURCE_RELEASED = "resource.released"
    RESOURCE_OOM_WARNING = "resource.oom_warning"


@dataclass
class Event:
    event_type: EventType
    event_id: str = field(default_factory=lambda: uuid4().hex)
    timestamp: float = field(default_factory=time)
    dag_id: Optional[str] = None
    run_id: Optional[str] = None
    node_id: Optional[str] = None
    attempt: int = 0
    lease_id: Optional[str] = None
    worker_id: Optional[str] = None
    trace_id: Optional[str] = None
    data: dict[str, Any] = field(default_factory=dict)


EventHandler = Callable[[Event], Awaitable[None]]


class EventBus:
    def __init__(self) -> None:
        self._subscribers: dict[EventType, list[EventHandler]] = defaultdict(list)

    def subscribe(self, event_type: EventType, handler: EventHandler) -> None:
        self._subscribers[event_type].append(handler)
        logger.debug("Subscribed %s to %s", handler.__qualname__, event_type.value)

    def unsubscribe(self, event_type: EventType, handler: EventHandler) -> None:
        handlers = self._subscribers.get(event_type, [])
        if handler in handlers:
            handlers.remove(handler)

    async def publish(self, event: Event) -> None:
        handlers = self._subscribers.get(event.event_type, [])
        if handlers:
            logger.debug(
                "Publishing %s (dag=%s, node=%s) to %d handlers",
                event.event_type.value,
                event.dag_id,
                event.node_id,
                len(handlers),
            )
            results = await asyncio.gather(
                *(handler(event) for handler in handlers),
                return_exceptions=True,
            )
            for handler, result in zip(handlers, results):
                if isinstance(result, Exception):
                    logger.error(
                        "Event handler %s failed while handling %s",
                        handler.__qualname__,
                        event.event_type.value,
                        exc_info=(type(result), result, result.__traceback__),
                    )
        else:
            logger.debug(
                "Publishing %s (dag=%s, node=%s) - no subscribers",
                event.event_type.value,
                event.dag_id,
                event.node_id,
            )


__all__ = ["Event", "EventBus", "EventHandler", "EventType"]
