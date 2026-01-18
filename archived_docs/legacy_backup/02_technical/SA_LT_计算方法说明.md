# SA & LT 指标计算方法说明

本文档详细整理了 SA (Schedule Attainment) 和 LT (Lead Time) 的数据来源及计算逻辑。

## 1. 数据来源 (Data Sources)

指标计算依赖于以下基础数据表：

| 表名 | 说明 | 关键字段 | 来源 |
| :--- | :--- | :--- | :--- |
| **raw_mes** | MES 生产报工原始数据 | `BatchNumber`, `Operation`, `TrackOutTime`, `EnterStepTime`, `TrackOutQuantity` | MES 系统导出 |
| **raw_sfc** | SFC 批次详细追踪数据 | `BatchNumber`, `Operation`, `TrackInTime` | SFC 系统导出 |
| **raw_sap_routing** | SAP 标准工艺路线 | `StandardTime`, `EH_machine`, `SetupTime`, `OEE` | SAP 系统导出 |
| **dim_calendar** | 工厂日历维度表 | `date`, `is_workday` | 基础配置 |

> **注意**：核心计算逻辑在数据库视图 `v_mes_metrics` 中实现，该视图关联了上述前三张表。

---

## 2. 计算公式 (Calculation Formulas)

### 2.1 LT (Lead Time) - 制造周期

**定义**：从批次进入工序到完成工序的总时长（包含等待时间）。

**计算逻辑**：
$$ LT(d) = \frac{TrackOutTime - StartTime}{24} $$

**StartTime 取值规则**：
1.  **首道工序 (Operation 0010/10)**：
    *   优先级 1: `raw_sfc.TrackInTime` (最准确)
    *   优先级 2: `raw_mes.EnterStepTime`
    *   优先级 3: `raw_mes.TrackInTime`
2.  **其他工序**：
    *   使用 `raw_mes.EnterStepTime`

---

### 2.2 PT (Process Time) - 实际加工时间

**定义**：排除非生产性停机等待后的实际加工时长。

**计算逻辑**：
$$ PT(d) = \frac{TrackOutTime - EffectiveStartTime}{24} $$

**EffectiveStartTime 取值规则**：
通过比较 **当前批次进入时间** (`EnterStepTime`) 与 **同一机台上一批次结束时间** (`PreviousBatchEndTime`) 来判断是否连续生产。

`PreviousBatchEndTime` 的来源：在视图 `v_mes_metrics` 中按 `Machine` 分组、按 `TrackOutTime` 排序，使用窗口函数 `LAG(TrackOutTime)` 得到。

1.  **非连续生产 (有停机)**：
    *   条件：`EnterStepTime > PreviousBatchEndTime`
    *   取值：优先使用 `TrackInTime` (实际开工时间)。如果 `TrackInTime` 为空，则回退使用 `PreviousBatchEndTime`。
2.  **连续生产**：
    *   条件：`EnterStepTime <= PreviousBatchEndTime`
    *   取值：`PreviousBatchEndTime` (上一批刚结束即刻开始)

---

### 2.3 ST (Standard Time) - 标准加工时间

**定义**：基于 SAP 标准工艺参数计算的理论加工天数。

**计算公式**：
$$ ST(d) = \frac{SetupTime + ProductionTime + ChangeoverTime}{24} $$

*   **SetupTime (调试时间)**：
    *   如果 `IsSetup = 'Yes'` (同机台上下批次 CFN 不同)，则取 `SAP_SetupTime`。
    *   否则为 0。
*   **ProductionTime (生产时间)**：
    $$ \frac{(TrackOutQuantity + ScrapQty) \times EH}{3600 \times OEE} $$
    *   `EH` 优先使用 `EH_machine`，否则使用 `EH_labor`。
    *   注：`EH` 单位为秒；`OEE` 为空或为 0 时默认使用 0.77。
*   **ChangeoverTime (换批缓冲)**：
    *   固定值为 0.5 小时。

> **说明**：在 `v_mes_metrics` 中，`ST(d)` 已经包含 **除以 OEE** 的修正，并且已经包含 **0.5 小时**的换批时间。

---

### 2.4 SA (Schedule Attainment) - 达成率

**定义**：对每个工序判断是否超期完成，并基于工序级判断聚合得到达成率。

**判定逻辑（工序级）**：

- 对每条工序记录（`BatchNumber + Operation + Machine` 等粒度），对比 `PT(d)` 与 `ST(d)`。
- 固定容差：8 小时。

$$ Tolerance(d) = \frac{8}{24} = 0.33 $$

$$ IsOverdue = \begin{cases}
1, & PT(d) > ST(d) + Tolerance(d) \\
0, & PT(d) \le ST(d) + Tolerance(d)
\end{cases} $$

**聚合口径**（按 Plant/日期/周/产品/工序等维度均可）：

$$ SA = \frac{\text{OnTime 工序数}}{\text{总工序数}} \times 100\% $$

其中：`OnTime 工序数 = 总工序数 - Overdue 工序数`。

> **统计范围建议**：只统计 `ST(d)` 不为空且 > 0、并且 `PT(d)` 不为空的工序记录。

---

## 3. SQL 实现参考

核心逻辑位于 `data_pipelines/database/schema/init_schema_v2.sql` 中的 `v_mes_metrics` 视图定义（包含 `PT(d)` 与 `ST(d)`）。
