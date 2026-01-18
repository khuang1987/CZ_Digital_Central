# 并行调度器 (Parallel Orchestrator)

本文档详细介绍 MDDAP V2 系统的核心调度引擎 `scripts/orchestration/run_etl_parallel.py`。

---

## 1. 设计目标

- **高性能**: 利用多核 CPU 并行处理相互独立的 ETL 任务，显著缩短总耗时。
- **稳定性**: 进程隔离，单个任务崩溃不会导致整个调度器崩溃。
- **可观测性**: 统一的缓冲日志机制，彻底解决多进程日志交错混乱的问题。

## 2. 架构原理

调度器采用 **主进程 (Main Process) + 进程池 (Process Pool)** 的架构。

### 2.1 执行阶段 (Stages)

任务被划分为有依赖关系的四个阶段，阶段内部的任务并行执行，阶段之间串行阻塞。

| 阶段 | 描述 | 包含任务示例 |
|:-----|:-----|:------------|
| **Stage 1 (Raw)** | 原始数据抽取 | SAP Routing, SFC Inspection, MES Raw |
| **Stage 2 (WIP)** | 中间层计算 | (目前预留) |
| **Stage 3 (Materialized)** | 聚合与物化 | Refresh Metrics, Create Views |
| **Stage 4 (Validation)** | 校验与统计 | Meta Table Health, Post-check |

### 2.2 缓冲日志 (Buffered Logging)

为了避免多进程同时打印导致的日志乱序 (Interleaving)，调度器实现了**输出捕获**机制：

1. **Capture**: 主进程启动子任务时，配置 `capture_output=True`，静默捕获 `stdout` 和 `stderr`。
2. **Buffer**: 子任务运行期间，所有 `print()` 和 `logging` 输出被暂存在内存缓冲区中。
3. **Flush**: 任务结束后，主进程一次性将缓冲区内容写入主日志文件，并加上 `=== LOGS for TASK: xxx ===` 的分隔符。

---

## 3. 代码结构

核心类与函数说明：

- **`Task` (NamedTuple)**: 定义任务元数据（名称、脚本路径、所属阶段）。
- **`run_task(task)`**: 工作进程函数。执行具体的 Python 脚本，捕获输出并返回 `TaskResult`。
- **`run_stage(stage_tasks)`**: 阶段调度函数。使用 `concurrent.futures.ProcessPoolExecutor` 并发提交任务，并监控完成状态。
- **`main()`**: 程序入口。按顺序执行 Stage 1 -> 4，最后生成执行摘要。

## 4. 配置与扩展

### 4.1 添加新任务

要在调度中加入新写的 ETL 脚本，只需在 `run_etl_parallel.py` 的 `STAGES` 字典中添加条目：

```python
STAGES = {
    "1_Raw_Layer": [
        # ... 现有任务
        Task("My New Task", "data_pipelines/sources/new_source/etl_new.py"),
    ],
    # ...
}
```

### 4.2 调整并发度

默认并发数由 `max_workers` 参数控制（通常默认为 CPU 核心数）。如需限制并发以降低数据库压力，可修改 `ProcessPoolExecutor` 参数。

---

## 5. 执行摘要 (Execution Summary)

每次运行结束后，调度器会输出一份简报：

```text
==================================================
EXECUTION SUMMARY
==================================================
Task Name                     Status   Duration
--------------------------------------------------
etl_sap_routing_raw           SUCCESS  3.2s
etl_mes_batch_output_raw      SUCCESS  12.5s
etl_meta_table_health         SUCCESS  1.1s
--------------------------------------------------
Total Execution Time: 18.4s
==================================================
```

这使得管理员能一眼看清整体健康状况。
