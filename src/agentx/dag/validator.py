"""DAG validation helpers."""

from __future__ import annotations

from typing import Optional

from agentx.dag.model import DynamicDAG


class ValidationError(Exception):
    pass


class DAGValidator:
    @staticmethod
    def validate(dag: DynamicDAG) -> Optional[str]:
        error = DAGValidator._check_acyclic(dag)
        if error:
            return error
        error = DAGValidator._check_dependency_integrity(dag)
        if error:
            return error
        return None

    @staticmethod
    def _check_acyclic(dag: DynamicDAG) -> Optional[str]:
        white, gray, black = 0, 1, 2
        color: dict[str, int] = {node_id: white for node_id in dag.nodes}

        def dfs(node_id: str) -> Optional[str]:
            color[node_id] = gray
            node = dag.nodes.get(node_id)
            if node:
                for succ_id in node.successors:
                    if succ_id not in color:
                        continue
                    if color[succ_id] == gray:
                        return f"Cycle detected: {node_id} -> {succ_id}"
                    if color[succ_id] == white:
                        result = dfs(succ_id)
                        if result:
                            return result
            color[node_id] = black
            return None

        for node_id in dag.nodes:
            if color[node_id] == white:
                result = dfs(node_id)
                if result:
                    return result
        return None

    @staticmethod
    def _check_dependency_integrity(dag: DynamicDAG) -> Optional[str]:
        for node in dag.nodes.values():
            for dep_id in node.dependencies:
                if dep_id not in dag.nodes:
                    return f"Node '{node.node_id}' depends on non-existent node '{dep_id}'"
                parent = dag.nodes[dep_id]
                if node.node_id not in parent.successors:
                    return (
                        f"Dependency {dep_id} -> {node.node_id} exists in dependencies "
                        f"but not mirrored in successors"
                    )
            for succ_id in node.successors:
                if succ_id not in dag.nodes:
                    return f"Node '{node.node_id}' has successor '{succ_id}' that does not exist"
                target = dag.nodes[succ_id]
                if node.node_id not in target.dependencies:
                    return (
                        f"Edge {node.node_id} -> {succ_id} exists in successors "
                        f"but not mirrored in dependencies"
                    )
        for edge in dag.edges.values():
            if edge.source not in dag.nodes:
                return f"Edge '{edge.edge_id}' has non-existent source '{edge.source}'"
            if edge.target not in dag.nodes:
                return f"Edge '{edge.edge_id}' has non-existent target '{edge.target}'"
            source = dag.nodes[edge.source]
            target = dag.nodes[edge.target]
            if edge.target not in source.successors:
                return (
                    f"Edge {edge.source} -> {edge.target} exists in edges "
                    f"but not in source successors"
                )
            if edge.source not in target.dependencies:
                return (
                    f"Edge {edge.source} -> {edge.target} exists in edges "
                    f"but not in target dependencies"
                )
        return None


__all__ = ["DAGValidator", "ValidationError"]
