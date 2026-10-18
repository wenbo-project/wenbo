"""Executor interfaces and data types."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class RequestStatus(str, Enum):
    """Backend request lifecycle status, distinct from a node's runtime state."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class LLMRequest:
    """Model request with prompt, sampling options, and structured scheduling metadata."""

    model: str = "qwen2.5-7b"
    prompt: str = ""
    sampling_params: dict[str, Any] = field(default_factory=dict)
    request_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class LLMResponse:
    """Backend result including success/error, token counts, and latency in milliseconds."""

    request_id: str = ""
    output: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    latency_ms: float = 0.0
    success: bool = True
    error: str | None = None


class ExecutorBackend(ABC):
    """Async boundary between workflow execution and a request implementation.

    Implementations own request IDs and status storage. Methods should
    yield control during I/O or simulated execution. The runtime calls
    submit followed by wait and may call cancel on timeout or shutdown.
    """

    @abstractmethod
    async def submit(self, request: LLMRequest) -> str:
        """Submit a request and return the backend's request identifier."""

    @abstractmethod
    async def cancel(self, request_id: str) -> bool:
        """Request cancellation and report whether it was accepted by the backend."""

    @abstractmethod
    async def query(self, request_id: str) -> RequestStatus:
        """Return the backend status associated with a request identifier."""

    @abstractmethod
    async def wait(self, request_id: str, timeout: float = 30.0) -> LLMResponse:
        """Await a response with a timeout expressed in seconds.

        Concrete backends define how unknown IDs, cancellation, and timeout
        are reported. Callers must inspect LLMResponse.success and error.
        """
