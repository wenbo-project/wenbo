"""Llama.cpp CLI 执行器：通过 llama-cli 子进程执行本地推理"""

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
class LlamaCLIConfig:
    """llama-cli 后端配置"""

    # llama-cli 可执行文件路径
    cli_path: str = "llama-cli"
    # 模型文件路径 (GGUF)
    model_path: str = ""
    # GPU 层数 (-1 = 全部加载到 GPU)
    gpu_layers: int = -1
    # 最大生成 token 数
    n_predict: int = 256
    # 温度
    temperature: float = 0.7
    # top_p
    top_p: float = 0.9
    # 上下文窗口大小
    ctx_size: int = 2048
    # 并发数 (llama-cli 不支持真并发，这里限制为 1)
    max_concurrent: int = 1
    # 超时秒数
    timeout: float = 120.0


@dataclass
class _RunningRequest:
    request_id: str
    request: LLMRequest
    status: RequestStatus = RequestStatus.RUNNING
    start_time: float = field(default_factory=time.monotonic)
    response: Optional[LLMResponse] = None
    process: Optional[asyncio.subprocess.Process] = None


class LlamaCLIBackend(ExecutorBackend):
    """通过 llama-cli 命令行执行本地推理。

    由于 llama-cli 是单进程串行执行的，此类内部维护一个执行信号量，
    保证同时只有一个推理请求在运行。多请求时自动排队。

    用法:
        config = LlamaCLIConfig(model_path="path/to/model.gguf")
        executor = LlamaCLIBackend(config)
        response = await executor.wait(await executor.submit(request))
    """

    def __init__(self, config: Optional[LlamaCLIConfig] = None) -> None:
        self._config = config or LlamaCLIConfig()
        self._requests: dict[str, _RunningRequest] = {}
        # 信号量：限制 llama-cli 并发为 1
        self._semaphore = asyncio.Semaphore(self._config.max_concurrent)

    # ── ExecutorBackend 接口 ──────────────────────────────────

    async def submit(self, request: LLMRequest) -> str:
        request_id = f"llama_{request.request_id or id(request)}"

        # 优先使用请求中的模型路径，否则回退到配置中的默认路径
        model_path = self._resolve_model_path(request.model)

        rr = _RunningRequest(request_id=request_id, request=request)
        self._requests[request_id] = rr

        # 在后台启动执行
        asyncio.create_task(self._run_inference(request_id, model_path, request))

        logger.info("Submitted request %s (prompt: %.80s...)", request_id, request.prompt[:80])
        return request_id

    async def cancel(self, request_id: str) -> bool:
        rr = self._requests.get(request_id)
        if rr is None:
            return False
        if rr.status in (RequestStatus.COMPLETED, RequestStatus.FAILED, RequestStatus.CANCELLED):
            return False

        if rr.process and rr.process.returncode is None:
            rr.process.terminate()
            try:
                await asyncio.wait_for(rr.process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                rr.process.kill()

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
        remaining = max(timeout - elapsed, 0.1)

        # 轮询等待完成
        deadline = time.monotonic() + remaining
        while rr.status == RequestStatus.RUNNING:
            if time.monotonic() >= deadline:
                logger.warning("Wait timeout for %s", request_id)
                await self.cancel(request_id)
                return LLMResponse(
                    request_id=request_id,
                    latency_ms=elapsed * 1000,
                    success=False,
                    error="wait timeout",
                )
            await asyncio.sleep(0.1)

        response = rr.response
        if response is None:
            return LLMResponse(
                request_id=request_id,
                latency_ms=elapsed * 1000,
                success=False,
                error="no response produced",
            )
        return response

    # ── 内部方法 ──────────────────────────────────────────────

    def _resolve_model_path(self, model: str) -> str:
        """解析模型路径：如果是有效路径直接使用，否则回退到默认配置"""
        if not model or model == "qwen2.5-7b":
            return self._config.model_path
        # 如果 model 看起来像文件路径（含扩展名或路径分隔符），直接使用
        if Path(model).suffix or "\\" in model or "/" in model:
            return model
        return self._config.model_path

    async def _run_inference(self, request_id: str, model_path: str, request: LLMRequest) -> None:
        """调用 llama-cli 执行推理"""
        rr = self._requests[request_id]

        async with self._semaphore:
            try:
                cmd = self._build_cmd(model_path, request.prompt, request.sampling_params)
                # 用 shell 模式避免 Windows 下中文参数编码问题
                cmd_str = " ".join(f'"{c}"' if (" " in c or "\"" in c) else c for c in cmd)
                logger.info("Running: %s", cmd_str)

                rr.process = await asyncio.create_subprocess_shell(
                    cmd_str,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )

                try:
                    stdout, stderr = await asyncio.wait_for(
                        rr.process.communicate(),
                        timeout=self._config.timeout,
                    )
                except asyncio.TimeoutError:
                    rr.process.kill()
                    await rr.process.wait()
                    rr.status = RequestStatus.FAILED
                    rr.response = LLMResponse(
                        request_id=request_id,
                        latency_ms=(time.monotonic() - rr.start_time) * 1000,
                        success=False,
                        error="process timeout",
                    )
                    return

                latency_ms = (time.monotonic() - rr.start_time) * 1000

                if rr.process.returncode != 0:
                    err_msg = stderr.decode("utf-8", errors="replace").strip()
                    logger.error("llama-cli failed (rc=%d): %s", rr.process.returncode, err_msg[:200])
                    rr.status = RequestStatus.FAILED
                    rr.response = LLMResponse(
                        request_id=request_id,
                        latency_ms=latency_ms,
                        success=False,
                        error=err_msg[:500],
                    )
                    return

                output = stdout.decode("utf-8", errors="replace").strip()

                # 粗略估算 token 数 (中文约 1.5 字/token, 英文约 4 字符/token)
                input_tokens = self._estimate_tokens(request.prompt)
                output_tokens = self._estimate_tokens(output)

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
                rr.status = RequestStatus.FAILED
                rr.response = LLMResponse(
                    request_id=request_id,
                    latency_ms=(time.monotonic() - rr.start_time) * 1000,
                    success=False,
                    error=str(e),
                )
                logger.error("Inference error for %s: %s", request_id, e)

    def _build_cmd(self, model_path: str, prompt: str, sampling_params: dict[str, Any]) -> list[str]:
        """构建 llama-cli 命令行参数"""
        cfg = self._config
        cmd = [
            cfg.cli_path,
            "-m", model_path,
            "-ngl", str(cfg.gpu_layers),
            "-p", prompt,
            "-n", str(cfg.n_predict),
        ]

        # 仅在非默认值时添加可选参数
        temp = sampling_params.get("temperature", cfg.temperature)
        if temp != 0.0:
            cmd += ["--temp", str(temp)]
        top_p = sampling_params.get("top_p", cfg.top_p)
        if top_p < 1.0:
            cmd += ["--top-p", str(top_p)]
        if cfg.ctx_size > 0:
            cmd += ["-c", str(cfg.ctx_size)]

        return cmd

    @staticmethod
    def _estimate_tokens(text: str) -> int:
        """粗略估算 token 数"""
        if not text:
            return 0
        # 混合估算：假设平均每个 token 约 2 个字符
        return max(1, len(text) // 2)
