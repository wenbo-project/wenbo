"""HotpotQA 数据集转换为 DAG 图集合

将 HotpotQA 多跳问答问题转换为 DAG 结构：
- bridge 类型 → 线性链（分步推理）
- comparison 类型 → 菱形拓扑（并行对比后聚合）

每个 DAG 节点仅包含问题引导 prompt，不提供 supporting facts（纯生成式）。
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class DAGSpec:
    """单个 DAG 的序列化表示"""

    dag_id: str
    topology: str  # "linear" | "diamond"
    question_type: str  # "bridge" | "comparison"
    original_question: str
    gold_answer: str
    hop_count: int
    nodes: list[dict[str, Any]] = field(default_factory=list)
    edges: list[dict[str, str]] = field(default_factory=list)


def _build_bridge_dag(idx: int, question: str, answer: str, hop_count: int) -> DAGSpec:
    """bridge 类型 → 线性链

    结构: N0(分解) → N1(跳1) → N2(跳2) → ... → Nn(综合)
    """
    dag_id = f"hotpot_{idx:04d}"
    total_nodes = hop_count + 2  # 分解 + 跳数 + 综合
    nodes = []
    edges = []

    # N0: 问题分解
    nodes.append({
        "node_id": "N0",
        "prompt": (
            f"请分析以下多跳问题，将其分解为 {hop_count} 个需要依次回答的子问题。\n"
            f"只需列出子问题，不需要回答。\n\n"
            f"问题：{question}"
        ),
        "node_type": "llm_infer",
    })

    # N1..N_{hop}: 逐步推理
    for i in range(hop_count):
        if i == 0:
            prompt = (
                f"原始问题：{question}\n\n"
                f"这是多跳推理的第 1 步。请直接回答你能确定的第一个子问题。"
            )
        elif i == hop_count - 1:
            prompt = (
                f"原始问题：{question}\n\n"
                f"这是多跳推理的最后一步。请基于前面所有步骤的推理，完成最终推理。"
            )
        else:
            prompt = (
                f"原始问题：{question}\n\n"
                f"这是多跳推理的第 {i + 1} 步。请继续推理。"
            )

        nodes.append({
            "node_id": f"N{i + 1}",
            "prompt": prompt,
            "node_type": "llm_infer",
        })

    # N_{hop+1}: 综合答案
    nodes.append({
        "node_id": f"N{hop_count + 1}",
        "prompt": (
            f"原始问题：{question}\n\n"
            f"请基于前面的推理链，给出最终答案。只需给出简洁的答案，不需要解释过程。"
        ),
        "node_type": "llm_infer",
    })

    # 边: 线性链
    for i in range(total_nodes - 1):
        edges.append({"source": f"N{i}", "target": f"N{i + 1}"})

    return DAGSpec(
        dag_id=dag_id,
        topology="linear",
        question_type="bridge",
        original_question=question,
        gold_answer=answer,
        hop_count=hop_count,
        nodes=nodes,
        edges=edges,
    )


def _build_comparison_dag(idx: int, question: str, answer: str) -> DAGSpec:
    """comparison 类型 → 菱形拓扑

    结构:
        N0(分解)
       /        \\
    N1(方面A)   N2(方面B)
       \\        /
        N3(对比综合)
    """
    dag_id = f"hotpot_{idx:04d}"
    nodes = [
        {
            "node_id": "N0",
            "prompt": (
                f"请分析以下对比问题，确定需要比较的两个方面或实体。\n"
                f"只需指出需要比较什么，不需要给出比较结果。\n\n"
                f"问题：{question}"
            ),
            "node_type": "llm_infer",
        },
        {
            "node_id": "N1",
            "prompt": (
                f"原始问题：{question}\n\n"
                f"请推理该对比问题的第一个方面。"
            ),
            "node_type": "llm_infer",
        },
        {
            "node_id": "N2",
            "prompt": (
                f"原始问题：{question}\n\n"
                f"请推理该对比问题的第二个方面。"
            ),
            "node_type": "llm_infer",
        },
        {
            "node_id": "N3",
            "prompt": (
                f"原始问题：{question}\n\n"
                f"请基于前面两方面的推理结果进行对比，给出最终答案。"
                f"只需给出简洁的答案，不需要解释过程。"
            ),
            "node_type": "llm_infer",
        },
    ]
    edges = [
        {"source": "N0", "target": "N1"},
        {"source": "N0", "target": "N2"},
        {"source": "N1", "target": "N3"},
        {"source": "N2", "target": "N3"},
    ]

    return DAGSpec(
        dag_id=dag_id,
        topology="diamond",
        question_type="comparison",
        original_question=question,
        gold_answer=answer,
        hop_count=2,
        nodes=nodes,
        edges=edges,
    )


def convert_hotpotqa(
    file_path: str,
    max_samples: Optional[int] = None,
    skip_no_answer: bool = True,
) -> list[DAGSpec]:
    """从本地 JSON 文件加载 HotpotQA 数据并转换为 DAG 列表。

    支持的数据格式:
        [{"id": "...", "level": "hard", "question": "...", "answer": "...", ...}, ...]
        或包含 "type" 字段的数据（bridge/comparison）。

    Args:
        file_path: 本地 JSON 文件路径
        max_samples: 最大样本数，None 表示全部
        skip_no_answer: 跳过无答案的样本
    """
    logger.info("Loading HotpotQA from %s ...", file_path)

    with open(file_path, "r", encoding="utf-8") as f:
        dataset = json.load(f)

    if max_samples:
        dataset = dataset[:max_samples]

    dags: list[DAGSpec] = []
    idx = 0

    for example in dataset:
        question = example.get("question", "")
        answer = example.get("answer", "")

        if skip_no_answer and not answer:
            continue

        # 获取问题类型：优先用数据中的 type 字段，否则从 question 内容推断
        question_type = example.get("type", "")
        if not question_type:
            q_lower = question.lower()
            if any(kw in q_lower for kw in ("both", "same", "either", "which")):
                question_type = "comparison"
            else:
                question_type = "bridge"

        # 从 supporting_facts 推断跳数（如果有的话）
        supporting_facts = example.get("supporting_facts", {})
        if isinstance(supporting_facts, dict):
            titles = supporting_facts.get("title", [])
            unique_titles = len(set(titles)) if titles else 2
            hop_count = max(unique_titles - 1, 1)
        else:
            hop_count = 2  # 默认 2 跳

        hop_count = min(hop_count, 5)

        # 根据 level 调整难度标记
        level = example.get("level", "medium")

        if question_type == "comparison":
            dag = _build_comparison_dag(idx, question, answer)
        else:
            dag = _build_bridge_dag(idx, question, answer, hop_count)

        dag.nodes[0]["prompt"] = (
            f"请分析以下{level}难度的多跳问题。{dag.nodes[0]['prompt']}"
        )

        dags.append(dag)
        idx += 1

    logger.info("Converted %d DAGs (bridge: %d, comparison: %d)",
                len(dags),
                sum(1 for d in dags if d.question_type == "bridge"),
                sum(1 for d in dags if d.question_type == "comparison"))
    return dags


def save_dags(dags: list[DAGSpec], output_path: str) -> None:
    """将 DAG 列表保存为单个 JSON 文件"""
    payload = {
        "metadata": {
            "source": "hotpotqa",
            "version": "v1.0",
            "total_dags": len(dags),
            "bridge_count": sum(1 for d in dags if d.question_type == "bridge"),
            "comparison_count": sum(1 for d in dags if d.question_type == "comparison"),
            "topology_distribution": _count_topologies(dags),
            "hop_distribution": _count_hops(dags),
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        },
        "dags": [
            {
                "dag_id": d.dag_id,
                "topology": d.topology,
                "question_type": d.question_type,
                "original_question": d.original_question,
                "gold_answer": d.gold_answer,
                "hop_count": d.hop_count,
                "nodes": d.nodes,
                "edges": d.edges,
            }
            for d in dags
        ],
    }

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    logger.info("Saved %d DAGs to %s", len(dags), output_path)


def load_dags(input_path: str) -> list[DAGSpec]:
    """从 JSON 文件加载 DAG 列表"""
    with open(input_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    dags = []
    for d in payload["dags"]:
        dag = DAGSpec(
            dag_id=d["dag_id"],
            topology=d["topology"],
            question_type=d["question_type"],
            original_question=d["original_question"],
            gold_answer=d["gold_answer"],
            hop_count=d["hop_count"],
            nodes=d["nodes"],
            edges=d["edges"],
        )
        dags.append(dag)

    logger.info("Loaded %d DAGs from %s (metadata: %s)",
                len(dags), input_path, json.dumps(payload["metadata"], ensure_ascii=False))
    return dags


def _count_topologies(dags: list[DAGSpec]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for d in dags:
        counts[d.topology] = counts.get(d.topology, 0) + 1
    return counts


def _count_hops(dags: list[DAGSpec]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for d in dags:
        key = f"{d.hop_count}_hop"
        counts[key] = counts.get(key, 0) + 1
    return counts
