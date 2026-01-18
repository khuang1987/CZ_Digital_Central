# MES 指标物化方案（BI 秒开）

## 背景

当前 `dbo.v_mes_metrics` 通过视图实时计算 LT/PT/ST、`PreviousBatchEndTime`、`Setup` 等字段，并且需要关联/聚合外部表（`dbo.raw_sfc`、`dbo.raw_sap_routing`）。在 SQL Server 上，这类实时视图容易出现：

- 查询时触发对外部表的聚合/窗口排序
- 执行计划退化（Nested Loops + Scan / Sort + Spool）
- Power BI 端拖字段即触发重计算，导致刷新/交互变慢

目标是将计算从“查询时”迁移到“刷新时”，使 BI 查询稳定秒开。

## 目标

- BI 端查询 `dbo.v_mes_metrics` 时只读取物化结果，不触发重计算。
- 保持输出字段与口径兼容（`machine`、`Machine(#)`、`LT(d)`、`PT(d)`、`ST(d)`、`CompletionStatus` 等）。
- 支持“原子切换/快速回滚”：刷新成功后再切换当前版本，避免 BI 查询到半成品。

## 新增数据库对象（SQL Server）

- **两张物化表（轮换写入）**
  - `dbo.mes_metrics_snapshot_a`
  - `dbo.mes_metrics_snapshot_b`

- **同义词（指向当前版本，原子切换）**
  - `dbo.mes_metrics_current` -> 指向 `dbo.mes_metrics_snapshot_a` 或 `dbo.mes_metrics_snapshot_b`

- **对外兼容视图（BI 只连这个）**
  - `dbo.v_mes_metrics`：仅做 `SELECT ... FROM dbo.mes_metrics_current`（轻量）

> 说明：使用 A/B 双表 + synonym 的方式可实现“先写入新表 -> 校验 -> 原子切换”。

## 刷新流程（推荐作业步骤）

1. **确定写入目标**
   - 如果 `dbo.mes_metrics_current` 当前指向 `_a`，则本次刷新写入 `_b`（反之亦然）。

2. **构建物化结果（全量或增量）**
   - 从 `dbo.raw_mes` 读取主数据（过滤子批次等规则保持一致）。
   - 关联/聚合外部数据：
     - `dbo.raw_sfc`：按 `BatchNumber + Operation` 得到 `Checkin_SFC`、`ScrapQty`
     - `dbo.raw_sap_routing`：按 `CFN + Operation + Group` 获取“最新一条”标准工时（EH/OEE/SetupTime 等）
   - 计算：
     - `PreviousBatchEndTime`：按机台分组、按 `TrackOutTime` 排序得到上一批结束时间
     - `Setup`：同机台上一批 CFN 与当前 CFN 是否不同
     - `LT(d)` / `PT(d)` / `ST(d)` / `CompletionStatus`

3. **数据校验（轻量）**
   - 校验新表可查询、关键列非空比例、行数与 `raw_mes` 基本一致。

4. **原子切换**
   - 切换 `dbo.mes_metrics_current` 指向新表（A/B 切换）。

5. **回滚策略**
   - 如刷新后发现问题，只需把 `dbo.mes_metrics_current` 切回旧表即可。

## 运维 Runbook（推荐命令）

### 1) 初始化一次（建快照表/同义词/轻量视图）

```powershell
python scripts/_execute_create_view_v2.py
```

该脚本会执行：

- `scripts/_init_mes_metrics_materialized.sql`

### 2) 每次刷新（重算 -> 写入另一张快照表 -> 原子切换）

```powershell
python scripts/_refresh_mes_metrics_materialized.py
```

### 3) 回滚（手工，秒级）

在 SQL Server 执行：

```sql
DROP SYNONYM dbo.mes_metrics_current;
CREATE SYNONYM dbo.mes_metrics_current FOR dbo.mes_metrics_snapshot_a; -- 或 snapshot_b
```

## 口径说明（与数据字典一致）

计算口径以：
- `docs/02_technical/database/data_dictionary/calculation-logic.md`
为准。

本方案的变化点仅为：
- 计算执行时机从“查询时”迁移到“刷新时”
- 输出字段/公式保持不变

## 运维与使用建议

- Power BI 数据源：只连接 `dbo.v_mes_metrics`。
- 刷新触发：建议由项目脚本统一执行（避免手工在 SSMS 中多步操作）。

## 版本历史

- 2026-01-08：引入 MES 指标物化方案（BI 秒开），采用 A/B 双表 + synonym 原子切换。
