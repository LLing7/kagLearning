# KAG — 任务 DAG、执行上下文 与 参考文本块（Chunk）检索说明（合并版）

说明
- 本文档把“从任务 DAG 生成执行上下文（Context）”与“基于任务 DAG 产生参考文本块（Chunk）”两部分内容合并，列出参与流程的主要类、关键函数签名、输入/输出/返回值、调用流程与注意事项，便于开发者理解与集成。
- 同目录下同时包含一张架构图：docs/kag_architecture.svg，图示流程与组件之间的调用关系。

> 文件位置（仓库）
- 文档: docs/kag_execution_and_chunk.md
- 架构图: docs/kag_architecture.svg

---

## 目录
1. 总览
2. 任务 DAG -> 执行上下文（Context）相关类与关键函数
3. 基于任务 DAG 的参考文本块（Chunk）检索相关类与关键函数
4. 集成调用流程（任务 DAG -> Context -> Retriever -> Generator）
5. 注意事项与最佳实践
6. 附录：参考文件路径

---

## 1. 总览
- KAG 求解流程通常包含：Planner（生成 task_dag）→ 将 DAG 解析为 Task 列表并构建执行上下文 Context → 按拓扑/并行执行任务（Executor/Retriever/Generator）→ 最终生成答案。
- 在该链路中，许多 Task 依赖参考文本块（Chunk）；Chunk 由不同 retriever（向量/文本/图回溯/Outline/AtomicQuery 等）提供，统一以 RetrieverOutput 返回。

---

## 2. 任务 DAG -> 执行上下文（Context）相关类与关键函数

### 2.1 Task (kag/interface/solver/planner_abc.py)
- 作用：表示单个执行单元，携带执行器名、参数、依赖关系、执行结果与内存。
- 关键字段：
  - `id: str`, `executor: str`, `arguments: dict`, `parents: List[Task]`, `children: List[Task]`, `result: Any`, `memory: dict`
- 关键方法：
  - `create_tasks_from_dag(task_dag: Dict[str, dict]) -> List[Task]`
    - 输入：task_dag（dict），每节点需含 `executor`、`dependent_task_ids`（list）、`arguments`（dict）
    - 输出：List[Task]（parent/child 关系已建立）
    - 异常：若依赖引用不存在，会抛 KeyError / ValueError
  - `add_parent(parent_task: Task) -> None`
  - `add_child(child_task: Task) -> None`
  - `update_result(value: Any) -> None`
  - `update_memory(key: str, value: Any) -> None`

### 2.2 PlannerABC (kag/interface/solver/planner_abc.py)
- 作用：从 query 生成任务 DAG（可同步/异步实现）。
- 方法：
  - `invoke(query: str, **kwargs) -> List[Task]`
  - `ainvoke(query: str, **kwargs) -> List[Task]`
  - `decompose_task(task: Task, **kwargs) -> List[Task]`

### 2.3 Context (kag/interface/solver/context.py)
- 作用：管理 Task 集合、构建依赖 DAG、按拓扑或 generation 产出执行顺序。
- 字段：`_tasks: Dict[str, Task]`
- 关键方法：
  - `add_task(task: Task) -> None`
  - `append_task(task: Task) -> None` — 若 task 无父则自动接到 last_task
  - `get_task(task_id: str) -> Optional[Task]`
  - `last_task() -> Optional[Task]`
  - `get_dag() -> networkx.DiGraph` — 构造节点/边并做拓扑校验；若有引用缺失抛 ValueError
  - `gen_task(group: bool=False) -> generator` — `group=True` 返回可并行执行的任务组

### 2.4 SolverPipeline（kag/solver/pipeline/*）
- 作用：编排 planner、executor、generator 的高层流水线。
- 常见方法：
  - `planning(self, query, context, **kwargs) -> List[Task]`
  - `execute_task(self, query, task, context, **kwargs) -> None`
  - `ainvoke(self, query, **kwargs) -> Any`

---

## 3. 基于任务 DAG 的参考文本块（Chunk）检索相关类与关键函数

### 3.1 数据模型（kag/interface/common/model/retriever_data.py）
- `ChunkData`：content, title, chunk_id, score, properties
  - `to_dict() -> dict` 返回序列化字段
- `DocData`：content, title, doc_id, score

### 3.2 RetrieverOutput（kag/interface/solver/retriever_abc.py）
- 结构：`graphs`, `chunks` (List[ChunkData]), `docs` (List[DocData]), `retriever_method`, `summary`, `err_msg`, `task`
- 方法：`to_dict() -> dict`

### 3.3 RetrieverABC（接口）
- 关键方法（子类实现）：
  - `invoke(self, task: Task, **kwargs) -> RetrieverOutput`
    - 输入：`task.arguments`（query/top_k/score_threshold）
    - 返回：`RetrieverOutput`（或包含 `err_msg`）
  - `schema(self) -> dict`

### 3.4 主要检索器实现（概要）
- `VectorChunkRetriever` — 向量检索，流程：cache -> vectorize -> search_vector(content/name) -> 合并/去重/排序 -> return RetrieverOutput(chunks)
- `TextChunkRetriever` — 文本检索，调用 search_api.search_text 并封装 `ChunkData`
- `VectorChunkRetrieverLegacy` — legacy 返回 dict[node_id->{score,content,name}]
- `PprChunkRetriever` — 结合 PageRank + 检索，方法：`calculate_pagerank_scores(start_nodes, top_k) -> dict`
- `AtomicQueryChunkRetriever` — 根据 atomic query 节点回溯 sourceChunk，方法：`recall_doc_by_atomic_query(atomic_query) -> Optional[ChunkData]`
- `OutlineChunkRetriever` — 先检索 outline，再扩展关联 chunk，方法：`get_outlines`, `get_children_outlines`, `get_chunk_data`, `get_related_chunks`

---

## 4. 集成调用流程（任务 DAG -> Context -> Retriever -> Generator）

总体步骤：
1. Planner 生成 `task_dag`（dict / LLM JSON）
2. `tasks = Task.create_tasks_from_dag(task_dag)`
3. `ctx = Context()`; 将 tasks 加入 context
4. 以 generation 为单位并行执行：`for task_group in ctx.gen_task(group=True): parallel execute group`
   - 若 task 为 retriever 类型：`output = retriever.invoke(task, top_k=...)`
   - 若 `output.err_msg` 非空：记录/重试/降级
   - 否则：`task.update_result(output.chunks)` 或把 chunks 写入 context
5. 所有任务完成后，`generator.ainvoke(query, ctx)` 生成最终答案

示例伪代码：

```py
tasks = Task.create_tasks_from_dag(task_dag)
ctx = Context()
for t in tasks:
    ctx.add_task(t)

for task_group in ctx.gen_task(group=True):
    await asyncio.gather(*[execute_task(task, ctx) for task in task_group])

async def execute_task(task, ctx):
    if task.executor in retriever_registry:
        retriever = retriever_registry[task.executor]
        output = retriever.invoke(task, top_k=10)
        if output.err_msg:
            task.update_result(None)
            task.update_memory('retriever_err', output.err_msg)
        else:
            task.update_result(output.chunks)
    else:
        # 其他 executor 的逻辑
        pass

answer = await generator.ainvoke(query, ctx)
```

---

## 5. 注意事项与最佳实践
- DAG 校验：确保 Planner 输出满足格式（executor, dependent_task_ids:list, arguments:dict）。
- 循环检测：`Context.get_dag()` 在拓扑排序时会抛 NetworkXUnfeasible（有环）；规划阶段应避免或处理环。
- 并发与隔离：同代任务可并行；跨代需等待。并发执行中注意线程/协程安全与外部资源限制。
- 缓存与性能：检索器使用 `chunk_cached_by_query_map` 缓存，以降低向量/API 调用成本。注意 TTL 与缓存大小配置。
- 合并/去重：合并多路检索结果按 `chunk_id` 去重并按 score 排序，使用 `score_threshold` 过滤噪声。
- 错误处理与降级：检索失败或返回稀少结果时，可降级到 text retriever 或返回空结果并记录 `err_msg`。

---

## 6. 附录：参考文件路径（仓库）
- kag/interface/solver/planner_abc.py
- kag/interface/solver/context.py
- kag/solver/pipeline/index_pipeline.py
- kag/solver/pipeline/naive_rag_pipeline.py
- kag/solver/pipeline/mcp_pipeline.py
- kag/solver/pipeline/self_cognition_pipeline.py
- kag/interface/common/model/retriever_data.py
- kag/interface/solver/retriever_abc.py
- kag/common/tools/algorithm_tool/chunk_retriever/vector_chunk_retriever.py
- kag/common/tools/algorithm_tool/chunk_retriever/vector_chunk_retriever_legacy.py
- kag/common/tools/algorithm_tool/chunk_retriever/text_chunk_retriever.py
- kag/common/tools/algorithm_tool/chunk_retriever/ppr_chunk_retriever.py
- kag/common/tools/algorithm_tool/chunk_retriever/atomic_query_chunk_retriever.py
- kag/common/tools/algorithm_tool/chunk_retriever/outline_chunk_retriever.py

---

下文链接：
- 文档（Markdown）: https://github.com/LLing7/kagLearning/blob/main/docs/kag_execution_and_chunk.md
- 架构图（SVG）: https://github.com/LLing7/kagLearning/blob/main/docs/kag_architecture.svg
