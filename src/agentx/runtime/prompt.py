"""Execution-time prompt resolution helpers."""

from __future__ import annotations

from typing import Any


class PromptResolver:
    def __init__(self, registry: Any) -> None:
        self._registry = registry

    def resolve(self, dag_id: str, node: Any) -> str:
        """Replace dependency output placeholders in a node prompt."""
        dag = self._registry.get(dag_id)
        if not dag:
            return node.prompt

        prompt = node.prompt
        for dep_id in node.dependencies:
            dep_node = dag.nodes.get(dep_id)
            if dep_node and dep_node.result and "output" in dep_node.result:
                prompt = prompt.replace(
                    f"{{{dep_id}_output}}",
                    dep_node.result["output"].strip(),
                )
        return prompt


__all__ = ["PromptResolver"]
