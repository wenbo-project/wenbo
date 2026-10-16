"""本地 llama.cpp 执行器：通过 llama-cpp-python 在进程内加载 GGUF 模型推理"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from agentx.executor.base import (
    ExecutorBackend,
    LLMRequest,
    LLMResponse,
    RequestStatus,
)

logger = logging.getLogger(__name__)


@dataclass
class LlamaCppConfig:
    """llama-cpp-python 后端配置"""

    # 模型文件路径 (GGUF)
    model_path: str = ""
    # GPU 层数 (-1 = 全部加载到 GPU)
    n_gpu_layers: int = -1
    # 最大生成 token 数
    n_predict: int = 256
    # 上下文窗口大小 (0 = 模型默认)
    n_ctx: int = 2048
    # 并发推理线程数
    max_concurrent: int = 1


@dataclass
class _RunningRequest:
    request_id: str
    request: LLMRequest
    status: RequestStatus = RequestStatus.RUNNING
    start_time: float = field(default_factory=time.monotonic)
    response: Optional[LLMResponse] = None
    cancelled: bool = False


class LlamaCppBackend(ExecutorBackend):
    """通过 llama-cpp-python 在进程内加载 GGUF 模型执行推理。

    模型只加载一次，后续请求复用同一实例，避免重复加载开销。
    llama-cpp-python 的 generate 是同步阻塞的，用线程池转为异步。

    安装:
        set CMAKE_ARGS="-DGGML_CUDA=on"
        pip install llama-cpp-python

    用法:
        config = LlamaCppConfig(model_path="path/to/model.gguf")
        executor = LlamaCppBackend(config)
        response = await executor.wait(await executor.submit(request))
    """

    def __init__(self, config: Optional[LlamaCppConfig] = None) -> None:
        self._config = config or LlamaCppConfig()
        self._requests: dict[str, _RunningRequest] = {}
        self._semaphore = asyncio.Semaphore(self._config.max_concurrent)
        self._llm = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    async def _ensure_model_loaded(self) -> None:
        """延迟加载模型（首次 submit 时调用）"""
        if self._llm is not None:
            return
        try:
            from llama_cpp import Llama
        except ImportError:
            raise ImportError(
                "llama-cpp-python 未安装。请执行:\n"
                "  set CMAKE_ARGS=\"-DGGML_CUDA=on\"\n"
                "  pip install llama-cpp-python"
            )

        logger.info("Loading model: %s (n_gpu_layers=%d, n_ctx=%d)",
                     self._config.model_path, self._config.n_gpu_layers, self._config.n_ctx)

        # 在线程中加载模型（避免阻塞事件循环）
        self._llm = await asyncio.to_thread(
            Llama,
            model_path=self._config.model_path,
            n_gpu_layers=self._config.n_gpu_layers,
            n_ctx=self._config.n_ctx,
            verbose=False,
        )
        logger.info("Model loaded successfully")

    # ── ExecutorBackend 接口 ──────────────────────────────────

    async def submit(self, request: LLMRequest) -> str:
        request_id = f"llama_{request.request_id or id(request)}"

        rr = _RunningRequest(request_id=request_id, request=request)
        self._requests[request_id] = rr

        asyncio.create_task(self._run_inference(request_id, request))
        logger.info("Submitted request %s", request_id)
        return request_id

    async def cancel(self, request_id: str) -> bool:
        rr = self._requests.get(request_id)
        if rr is None or rr.status != RequestStatus.RUNNING:
            return False
        rr.cancelled = True
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
        """在线程池中执行同步推理，避免阻塞事件循环"""
        rr = self._requests[request_id]

        try:
            await self._ensure_model_loaded()

            async with self._semaphore:
                if rr.cancelled:
                    return

                start = time.monotonic()

                # 在线程中运行同步的 generate
                result = await asyncio.to_thread(
                    self._generate_sync,
                    request.prompt,
                    request.sampling_params,
                )

                latency_ms = (time.monotonic() - start) * 1000

                if rr.cancelled:
                    return

                output = result["output"]
                input_tokens = result.get("input_tokens", 0)
                output_tokens = result.get("output_tokens", 0)

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

        except Exception as e:
            if rr.cancelled:
                return
            rr.status = RequestStatus.FAILED
            rr.response = LLMResponse(
                request_id=request_id,
                latency_ms=(time.monotonic() - rr.start_time) * 1000,
                success=False,
                error=str(e),
            )
            logger.error("Inference error for %s: %s", request_id, e)

    def _generate_sync(self, prompt: str, sampling_params: dict[str, Any]) -> dict[str, Any]:
        """同步调用 llama-cpp-python 生成（在线程池中执行）"""
        result = self._llm(
            prompt,
            max_tokens=sampling_params.get("max_tokens", self._config.n_predict),
            temperature=sampling_params.get("temperature", 0.7),
            top_p=sampling_params.get("top_p", 0.9),
            stop=["<|im_end|>", "<|endoftext|>"],
        )

        # result 是 Llama._CompletionOutput 或纯字符串
        if hasattr(result, "choices"):
            output = result.choices[0].text.strip()
        elif isinstance(result, dict) and "choices" in result:
            output = result["choices"][0]["text"].strip()
        elif isinstance(result, str):
            output = result.strip()
        else:
            output = str(result).strip()

        # 从 usage 获取真实 token 数
        input_tokens = 0
        output_tokens = 0
        if hasattr(result, "usage"):
            input_tokens = result.usage.prompt_tokens
            output_tokens = result.usage.completion_tokens
        elif isinstance(result, dict) and "usage" in result:
            input_tokens = result["usage"].get("prompt_tokens", 0)
            output_tokens = result["usage"].get("completion_tokens", 0)

        return {
            "output": output,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
        }
