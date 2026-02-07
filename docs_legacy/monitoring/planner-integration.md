# Planner 集成与闭环

监控引擎检测到严重异常 (Critical) 后，会与 Microsoft Planner 进行双向同步，实现“发现-分配-解决-关闭”的闭环。

## 1. 任务生成 (ETL -> Planner)

当触发 `Create_A3` 动作时：

1. **生成 ID**: 系统分配唯一的 `A3Id`。
2. **写入中间表**: 记录写入 SQL Server `TriggerCaseRegistry` 表，状态为 `OPEN`。
3. **推送 Planner**:
    - **标题**: `[A3-20251001-001] | SA_GLOBAL_CRITICAL | 达成率连续3周低于95%`
    - **内容**: 包含详细的违规数据（如具体哪几周、数值分别是多少）。
    - **标签**: 自动打上 `Critical`, `Auto-Generated` 标签。
    - **指派**: 根据规则中的 `Owner` 字段（如生产经理邮箱）自动指派给负责人。

## 2. 状态同步 (Planner -> ETL)

系统每日运行 `etl_planner_tasks_raw.py`，从 Planner 拉取最新的任务状态。

- 如果用户在 Planner 中将任务标记为 **"Completed" (已完成)**。
- 且 ETL 再次扫描发现数据已恢复正常（或不再满足连续违规条件）。
- 系统会在数据库中将该 A3 Case 的状态更新为 **CLOSED**。

## 3. 手动干预

### 3.1 误报处理
如果用户认为某个报警是误报或通过 Planner 无法解决：
1. 在 Planner 任务评论中注明原因。
2. 手动将 Planner 任务设为完成。
3. 系统会在次日同步时将其关闭。

### 3.2 重新打开 (Reopen)
如果一个已关闭的 A3 Case 在短时间内（如2周内）再次复发：
- 系统**不会**生成新的 A3 ID。
- 而是会将旧的 Case 状态从 `CLOSED` 改回 `OPEN`。
- Planner 任务会被重新激活，并在备注中追加 *"Reopened due to recurrence"*。
