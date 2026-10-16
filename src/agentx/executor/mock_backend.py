"""模拟执行器：不执行真实推理，模拟延迟后返回结果"""

from __future__ import annotations

import asyncio
import logging
import time
from enum import Enum
from typing import Optional

from agentx.executor.base import (
    ExecutorBackend,
    LLMRequest,
    LLMResponse,
    RequestStatus,
)

logger = logging.getLogger(__name__)


class MockMode(str, Enum):
    INSTANT = "instant"
    ESTIMATED = "estimated"
    RANDOM = "random"
    STRESS = "stress"


class MockBackend(ExecutorBackend):
    def __init__(
        self,
        mode: MockMode = MockMode.INSTANT,
        base_latency_ms: float = 100.0,
        random_range_ms: float = 50.0,
    ) -> None:
        self._mode = mode
        self._base_latency_ms = base_latency_ms
        self._random_range_ms = random_range_ms
        self._requests: dict[str, RequestStatus] = {}

    async def submit(self, request: LLMRequest) -> str:
        request_id = f"mock_{request.request_id or id(request)}"
        self._requests[request_id] = RequestStatus.RUNNING
        logger.debug("Mock submit: %s", request_id)
        return request_id

    async def cancel(self, request_id: str) -> bool:
        if request_id in self._requests:
            self._requests[request_id] = RequestStatus.CANCELLED
            logger.debug("Mock cancel: %s", request_id)
            return True
        return False

    async def query(self, request_id: str) -> RequestStatus:
        return self._requests.get(request_id, RequestStatus.PENDING)

    async def wait(self, request_id: str, timeout: float = 30.0) -> LLMResponse:
        latency = self._compute_latency()
        actual_delay_s = min(latency / 1000.0, timeout)

        logger.debug("Mock wait: %s (simulated %.1fms)", request_id, latency)
        await asyncio.sleep(actual_delay_s)

        success = actual_delay_s < timeout
        self._requests[request_id] = RequestStatus.COMPLETED if success else RequestStatus.FAILED

        estimated_input_tokens = 50
        estimated_output_tokens = 100

        return LLMResponse(
            request_id=request_id,
            output="[MOCK] simulated response",
            input_tokens=estimated_input_tokens,
            output_tokens=estimated_output_tokens,
            latency_ms=latency,
            success=success,
            error=None if success else "timeout",
        )

    def _compute_latency(self) -> float:
        if self._mode == MockMode.INSTANT:
            return 0.0
        elif self._mode == MockMode.ESTIMATED:
            return self._base_latency_ms
        elif self._mode == MockMode.RANDOM:
            import random
            return self._base_latency_ms + random.uniform(
                -self._random_range_ms, self._random_range_ms
            )
        elif self._mode == MockMode.STRESS:
            return self._base_latency_ms * 3.0
        return 0.0
