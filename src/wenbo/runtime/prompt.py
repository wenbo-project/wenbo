"""Execution-time prompt resolution helpers."""

from __future__ import annotations

from typing import Any


class PromptResolver:
    """Resolve direct predecessor output placeholders immediately before execution."""

    def __init__(self, registry: Any) -> None:
        self._registry = registry

    def resolve(self, dag_id: str, node: Any) -> str:
        """Replace {node_id_output} using stripped outputs of direct dependencies.

        Missing graphs or predecessor results leave the prompt or placeholder
        unchanged. This is literal substitution, not a general template engine.
        """
        dag = self._registry.get(dag_id)
        if not dag:
            return node.prompt

        prompt = node.prompt
        # Substitute upstream outputs into prompt placeholders to pass data between nodes.
        for dep_id in node.dependencies:
            dep_node = dag.nodes.get(dep_id)
            if dep_node and dep_node.result and "output" in dep_node.result:
                prompt = prompt.replace(
                    f"{{{dep_id}_output}}",
                    dep_node.result["output"].strip(),
                )
        return prompt


__all__ = ["PromptResolver"]
