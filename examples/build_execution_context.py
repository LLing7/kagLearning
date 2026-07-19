"""examples/build_execution_context.py

示例脚本：从 task_dag（字典或 JSON）构造 Task 列表、构建 Context，并按拓扑/并行分组执行任务。

用法:
- 直接运行脚本将执行内置示例 DAG：
    python examples/build_execution_context.py
- 或者传入 JSON 文件路径：
    python examples/build_execution_context.py path/to/task_dag.json

说明：此脚本使用仓库中的 Task.create_tasks_from_dag 和 Context 实现。
请在仓库环境中运行（确保 PYTHONPATH 包含仓库根目录，以便导入 kag 模块）。
"""

import asyncio
import json
import sys
from typing import Dict, Any

import networkx as nx

from kag.interface.solver.planner_abc import Task
from kag.interface.solver.context import Context


DEFAULT_TASK_DAG = {
    "0": {
        "executor": "Retriever",
        "dependent_task_ids": [],
        "arguments": {"query": "Who wrote book A?"},
    },
    "1": {
        "executor": "Retriever",
        "dependent_task_ids": [],
        "arguments": {"query": "Who wrote book B?"},
    },
    "2": {
        "executor": "Code",
        "dependent_task_ids": ["0", "1"],
        "arguments": {"query": "Find intersection of results: {{0.output}}, {{1.output}}"},
    },
}


def load_task_dag_from_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data


def build_context_from_dag(task_dag: Dict[str, dict]) -> Context:
    """Parse DAG dict -> Task objects -> Context with tasks added."""
    tasks = Task.create_tasks_from_dag(task_dag)
    ctx = Context()
    for t in tasks:
        ctx.add_task(t)
    return ctx


# 简单的示例执行器：把执行结果写回 task.result
async def fake_executor_invoke(task: Task):
    # 根据 executor 类型模拟不同的行为
    if task.executor.lower().startswith("retriev"):
        # 模拟检索结果列表
        task.update_result([f"doc_for_{task.id}_a", f"doc_for_{task.id}_b"])
    elif task.executor.lower() == "code":
        # 从父任务读取结果并合并
        parent_outputs = []
        for p in task.parents:
            if p.result:
                parent_outputs.extend(p.result)
        task.update_result(sorted(set(parent_outputs)))
    else:
        task.update_result(None)


async def run_pipeline(ctx: Context):
    """按 generation 分批并行执行任务，返回输出节点的结果列表。"""
    # 使用按代（generation）并行执行
    for task_group in ctx.gen_task(group=True):
        # task_group 是一个 Task 列表，同代内可并行
        await asyncio.gather(*[fake_executor_invoke(task) for task in task_group])

    dag = ctx.get_dag()
    output_nodes = [n for n, d in dag.out_degree() if d == 0]
    final_outputs = []
    for node_id in output_nodes:
        task = ctx.get_task(node_id)
        final_outputs.append({
            "task_id": task.id,
            "executor": task.executor,
            "arguments": task.arguments,
            "result": task.result,
        })
    return final_outputs


def print_topology(ctx: Context):
    dag = ctx.get_dag()
    topo = list(nx.topological_sort(dag))
    print("Topological order (task ids):", topo)
    print("Generations (parallel groups):")
    for gen in nx.topological_generations(dag):
        print(" - ", list(gen))


def main(argv):
    if len(argv) > 1:
        path = argv[1]
        task_dag = load_task_dag_from_file(path)
    else:
        task_dag = DEFAULT_TASK_DAG

    print("Input task_dag:")
    print(json.dumps(task_dag, indent=2, ensure_ascii=False))

    ctx = build_context_from_dag(task_dag)
    print_topology(ctx)

    final = asyncio.run(run_pipeline(ctx))
    print("\nFinal outputs:")
    print(json.dumps(final, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main(sys.argv)
