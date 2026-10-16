"""DAG 加载器：从 JSON 文件恢复为 DynamicDAG 对象

JSON 格式：[{dag_def}, ...]（预分解 DAG，带 {Nx_output} 占位符）
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from agentx.dag.builder import DAGBuilder
from agentx.dag.model import DynamicDAG, ModelConfig, NodeType

logger = logging.getLogger(__name__)


@dataclass
class DAGSpec:
    """单个 DAG 的序列化表示"""

    dag_id: str
    topology: str
    question_type: str
    original_question: str
    gold_answer: str
    nodes: list[dict[str, Any]] = field(default_factory=list)
    edges: list[dict[str, str]] = field(default_factory=list)
    hop_count: int = 2


def _load_json(file_path: str) -> list[dict]:
    """加载 JSON DAG 文件"""
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "dags" in data:
        return data["dags"]
    raise ValueError(f"Unexpected JSON structure in {file_path}")


def parse_specs(raw_list: list[dict]) -> list[DAGSpec]:
    """将原始 dict 列表转为 DAGSpec 列表"""
    specs = []
    for item in raw_list:
        spec = DAGSpec(
            dag_id=item["dag_id"],
            topology=item.get("topology", "linear"),
            question_type=item.get("question_type", "bridge"),
            original_question=item["original_question"],
            gold_answer=item.get("gold_answer", ""),
            nodes=item.get("nodes", []),
            edges=item.get("edges", []),
            hop_count=item.get("hop_count", len(item.get("nodes", [])) - 1),
        )
        specs.append(spec)
    return specs


def _infer_output_constraint(prompt: str) -> str:
    lowered = prompt.lower()
    if re.search(r"\byes or no\b", lowered):
        return "Output exactly one token: yes or no. No explanation."
    if re.search(r"\bnumber only\b|\byear only\b|\binteger\b", lowered):
        return "Output exactly one number. No words, no explanation."
    if re.search(r"\bcountry name only\b", lowered):
        return "Output exactly one country name. No explanation."
    if re.search(r"\bband name only\b|\bfull name only\b|\bcity name only\b|\bprefecture name only\b|\bisland group name\b", lowered):
        return "Output only the final short answer. No explanation."
    return "Output only the final short answer. No reasoning, no extra text."


def _harden_prompt(prompt: str) -> str:
    instruction = _infer_output_constraint(prompt)
    return (
        "You are answering a DAG node. Think silently and do not reveal reasoning.\n"
        f"{instruction}\n"
        "If unsure, still return only the best short answer.\n\n"
        f"Question: {prompt}"
    )


class DAGLoader:
    """从 JSON 文件恢复为可执行的 DynamicDAG。

    用法:
        loader = DAGLoader()
        dags = loader.load_from_file("data/hotpotqa_dags.json", limit=10)
        for dag, spec in dags:
            await scheduler.submit_dag(dag)
    """

    def __init__(
        self,
        default_model: str = "",
        default_sampling_params: Optional[dict] = None,
    ) -> None:
        self._default_model = default_model
        self._default_sampling_params = default_sampling_params or {
            "temperature": 0.7,
            "top_p": 0.9,
            "max_tokens": 256,
        }

    def load_from_file(
        self,
        file_path: str,
        limit: Optional[int] = None,
        topology_filter: Optional[str] = None,
    ) -> list[tuple[DynamicDAG, DAGSpec]]:
        """从 JSON 文件加载 DAG。

        Args:
            file_path: JSON 文件路径
            limit: 最多加载多少个 DAG
            topology_filter: 仅加载指定拓扑 ("linear" | "diamond")

        Returns:
            [(DynamicDAG, DAGSpec), ...] 元组列表
        """
        raw_list = _load_json(file_path)
        specs = parse_specs(raw_list)

        if topology_filter:
            specs = [s for s in specs if s.topology == topology_filter]
            logger.info("Filtered to %d %s DAGs", len(specs), topology_filter)

        if limit:
            specs = specs[:limit]

        builder = DAGBuilder()
        results = []

        for spec in specs:
            dag = self._build_dag(builder, spec)
            results.append((dag, spec))

        logger.info("Built %d DynamicDAGs", len(results))
        return results

    def _build_dag(self, builder: DAGBuilder, spec: DAGSpec) -> DynamicDAG:
        """将 DAGSpec 转换为 DynamicDAG"""
        model_config = ModelConfig(
            model=self._default_model,
            sampling_params=self._default_sampling_params.copy(),
        )

        dag = builder.create_dag(
            dag_id=spec.dag_id,
            agent_id="hotpotqa_agent",
            priority=1,
            metadata={
                "question_type": spec.question_type,
                "original_question": spec.original_question,
                "gold_answer": spec.gold_answer,
                "topology": spec.topology,
                "hop_count": spec.hop_count,
            },
        )

        for node_def in spec.nodes:
            builder.add_node(
                dag,
                node_id=node_def["node_id"],
                node_type=NodeType(node_def.get("node_type", "llm_infer")),
                prompt=_harden_prompt(node_def["prompt"]),
                hardware=node_def.get("hardware", "any"),
                model_config=model_config,
            )

        for edge_def in spec.edges:
            builder.add_dependency(dag, edge_def["source"], edge_def["target"])

        builder.validate(dag)
        return dag
