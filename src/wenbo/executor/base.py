"""执行器抽象接口与数据类型"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class RequestStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class LLMRequest:
    model: str = "qwen2.5-7b"
    prompt: str = ""
    sampling_params: dict[str, Any] = field(default_factory=dict)
    request_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class LLMResponse:
    request_id: str = ""
    output: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    latency_ms: float = 0.0
    success: bool = True
    error: Optional[str] = None


class ExecutorBackend(ABC):

    @abstractmethod
    async def submit(self, request: LLMRequest) -> str:
        """提交推理请求，返回 request_id"""

    @abstractmethod
    async def cancel(self, request_id: str) -> bool:
        """取消请求"""

    @abstractmethod
    async def query(self, request_id: str) -> RequestStatus:
        """查询请求状态"""

    @abstractmethod
    async def wait(self, request_id: str, timeout: float = 30.0) -> LLMResponse:
        """等待请求完成并返回结果"""
