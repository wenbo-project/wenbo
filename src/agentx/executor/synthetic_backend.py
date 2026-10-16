"""Resource-aware synthetic executor for throughput experiments."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field

from agentx.executor.base import ExecutorBackend, LLMRequest, LLMResponse, RequestStatus

logger = logging.getLogger(__name__)


@dataclass
class SyntheticBackendConfig:
    gpu_base_latency_ms: float = 2.0
    cpu_base_latency_ms: float = 1.0
    retrieval_base_latency_ms: float = 1.5
    tool_base_latency_ms: float = 1.0
    gpu_tokens_per_second: float = 20_000.0
    cpu_tokens_per_second: float = 8_000.0
    bandwidth_bytes_per_second: float = 100_000_000.0
    time_scale: float = 1.0
    min_latency_ms: float = 0.1


@dataclass
class _SyntheticRequest:
    request: LLMRequest
    status: RequestStatus = RequestStatus.RUNNING
    submitted_at: float = field(default_factory=time.monotonic)


class SyntheticBackend(ExecutorBackend):
    """Simulate node execution time from structured DAG cost fields."""

    def __init__(self, config: SyntheticBackendConfig | None = None) -> None:
        self._config = config or SyntheticBackendConfig()
        self._requests: dict[str, _SyntheticRequest] = {}

    async def submit(self, request: LLMRequest) -> str:
        request_id = f"synthetic_{request.request_id or id(request)}"
        self._requests[request_id] = _SyntheticRequest(request=request)
        logger.debug("Synthetic submit: %s", request_id)
        return request_id

    async def cancel(self, request_id: str) -> bool:
        item = self._requests.get(request_id)
        if item is None:
            return False
        item.status = RequestStatus.CANCELLED
        return True

    async def query(self, request_id: str) -> RequestStatus:
        item = self._requests.get(request_id)
        return item.status if item is not None else RequestStatus.PENDING

    async def wait(self, request_id: str, timeout: float = 30.0) -> LLMResponse:
        item = self._requests.get(request_id)
        if item is None:
            return LLMResponse(request_id=request_id, success=False, error="request not found")

        latency_ms = self._compute_latency_ms(item.request)
        actual_delay_s = min(latency_ms / 1000.0, timeout)
        await asyncio.sleep(actual_delay_s)

        success = actual_delay_s < timeout and item.status != RequestStatus.CANCELLED
        item.status = RequestStatus.COMPLETED if success else RequestStatus.FAILED
        metadata = item.request.metadata
        estimated_tokens = int(metadata.get("estimated_tokens", 0) or 0)

        return LLMResponse(
            request_id=request_id,
            output="[SYNTHETIC] ok",
            input_tokens=estimated_tokens,
            output_tokens=0,
            latency_ms=latency_ms,
            success=success,
            error=None if success else "timeout or cancelled",
        )

    def _compute_latency_ms(self, request: LLMRequest) -> float:
        metadata = request.metadata
        resource = metadata.get("resource_requirement") or {}
        explicit_time = float(resource.get("estimated_time_ms", 0.0) or 0.0)
        if explicit_time > 0:
            return max(explicit_time * self._config.time_scale, self._config.min_latency_ms)

        node_type = str(metadata.get("node_type", "custom"))
        hardware = str(metadata.get("hardware", "any"))
        tokens = int(metadata.get("estimated_tokens", 0) or 0)
        data_bytes = int(metadata.get("incoming_data_volume_bytes", 0) or 0)

        if node_type == "llm_infer" or hardware == "gpu":
            token_ms = tokens / max(self._config.gpu_tokens_per_second, 1.0) * 1000.0
            latency = self._config.gpu_base_latency_ms + token_ms
        elif node_type == "retrieval":
            transfer_ms = data_bytes / max(self._config.bandwidth_bytes_per_second, 1.0) * 1000.0
            latency = self._config.retrieval_base_latency_ms + transfer_ms
        elif node_type == "tool_call":
            token_ms = tokens / max(self._config.cpu_tokens_per_second, 1.0) * 1000.0
            latency = self._config.tool_base_latency_ms + token_ms
        else:
            token_ms = tokens / max(self._config.cpu_tokens_per_second, 1.0) * 1000.0
            latency = self._config.cpu_base_latency_ms + token_ms

        return max(latency * self._config.time_scale, self._config.min_latency_ms)
