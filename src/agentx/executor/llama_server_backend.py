"""llama-server HTTP 执行器：调用 llama-server 的 OpenAI 兼容 API"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import aiohttp

from agentx.executor.base import (
    ExecutorBackend,
    LLMRequest,
    LLMResponse,
    RequestStatus,
)

logger = logging.getLogger(__name__)


@dataclass
class LlamaServerConfig:
    """llama-server 后端配置"""

    # llama-server 地址
    base_url: str = "http://localhost:8080"
    # 最大生成 token 数
    n_predict: int = 256
    # 超时秒数
    timeout: float = 120.0


@dataclass
class _RunningRequest:
    request_id: str
    status: RequestStatus = RequestStatus.RUNNING
    start_time: float = field(default_factory=time.monotonic)
    response: Optional[LLMResponse] = None
    task: Optional[asyncio.Task] = None


class LlamaServerBackend(ExecutorBackend):
    """通过 llama-server 的 OpenAI 兼容 HTTP API 执行推理。

    使用前需先启动 llama-server:
        llama-server -m model.gguf --gpu-layers -1 -c 2048 --port 8080

    用法:
        config = LlamaServerConfig(base_url="http://localhost:8080")
        executor = LlamaServerBackend(config)
        response = await executor.wait(await executor.submit(request))
    """

    def __init__(self, config: Optional[LlamaServerConfig] = None) -> None:
        self._config = config or LlamaServerConfig()
        self._requests: dict[str, _RunningRequest] = {}
        self._session: Optional[aiohttp.ClientSession] = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                base_url=self._config.base_url,
                timeout=aiohttp.ClientTimeout(total=self._config.timeout),
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    # ── ExecutorBackend 接口 ──────────────────────────────────

    async def submit(self, request: LLMRequest) -> str:
        request_id = f"http_{request.request_id or id(request)}"

        rr = _RunningRequest(request_id=request_id)
        self._requests[request_id] = rr
        rr.task = asyncio.create_task(self._run_inference(request_id, request))

        logger.info("Submitted request %s (prompt: %.80s...)", request_id, request.prompt[:80])
        return request_id

    async def cancel(self, request_id: str) -> bool:
        rr = self._requests.get(request_id)
        if rr is None or rr.status != RequestStatus.RUNNING:
            return False
        if rr.task:
            rr.task.cancel()
        rr.status = RequestStatus.CANCELLED
        logger.info("Cancelled request %s", request_id)
        return True

    async def query(self, request_id: str) -> RequestStatus:
        rr = self._requests.get(request_id)
        return rr.status if rr else RequestStatus.PENDING

    async def wait(self, request_id: str, timeout: float = 30.0) -> LLMResponse:
        rr = self._requests.get(request_id)
        if rr is None:
            return LLMResponse(request_id=request_id, success=False, error="request not found")

        elapsed = time.monotonic() - rr.start_time
        deadline = time.monotonic() + max(timeout - elapsed, 0.1)

        while rr.status == RequestStatus.RUNNING:
            if time.monotonic() >= deadline:
                await self.cancel(request_id)
                return LLMResponse(
                    request_id=request_id,
                    latency_ms=(time.monotonic() - rr.start_time) * 1000,
                    success=False,
                    error="wait timeout",
                )
            await asyncio.sleep(0.1)

        return rr.response or LLMResponse(
            request_id=request_id,
            latency_ms=(time.monotonic() - rr.start_time) * 1000,
            success=False,
            error="no response produced",
        )

    # ── 内部方法 ──────────────────────────────────────────────

    async def _run_inference(self, request_id: str, request: LLMRequest) -> None:
        rr = self._requests[request_id]
        start = time.monotonic()

        try:
            session = await self._ensure_session()

            payload: dict[str, Any] = {
                "prompt": request.prompt,
                "n_predict": request.sampling_params.get("max_tokens", self._config.n_predict),
                "temperature": request.sampling_params.get("temperature", 0.7),
                "top_p": request.sampling_params.get("top_p", 0.9),
            }

            async with session.post("/v1/completions", json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    raise RuntimeError(f"llama-server error {resp.status}: {body[:500]}")

                data = await resp.json()

            latency_ms = (time.monotonic() - start) * 1000

            # 解析响应
            output = ""
            input_tokens = 0
            output_tokens = 0

            if "choices" in data and data["choices"]:
                output = data["choices"][0].get("text", "").strip()
            if "usage" in data:
                input_tokens = data["usage"].get("prompt_tokens", 0)
                output_tokens = data["usage"].get("completion_tokens", 0)

            rr.status = RequestStatus.COMPLETED
            rr.response = LLMResponse(
                request_id=request_id,
                output=output,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                latency_ms=latency_ms,
                success=True,
            )
            logger.info(
                "Request %s completed (%.0fms, %d in / %d out tokens)",
                request_id, latency_ms, input_tokens, output_tokens,
            )

        except asyncio.CancelledError:
            rr.status = RequestStatus.CANCELLED
        except Exception as e:
            rr.status = RequestStatus.FAILED
            rr.response = LLMResponse(
                request_id=request_id,
                latency_ms=(time.monotonic() - start) * 1000,
                success=False,
                error=str(e),
            )
            logger.error("Inference error for %s: %s", request_id, e)
