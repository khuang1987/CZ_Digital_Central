# MDDAP 数据库概览（V2）

## 数据库文件

| 文件名 | 架构版本 | 用途 | 状态 |
|--------|----------|------|------|
| `mddap.db` | V1 | ETL 计算后的宽表 | 旧版（可删除） |
| `mddap_v2.db` | V2 | 分层架构（ODS + DWD） | 历史/兼容（迁移期遗留） |

## 权威数据源（交付口径）

- 交付口径：以 **SQL Server** 为唯一权威数据源
  - `localhost\SQLEXPRESS` / `mddap_v2`
- `mddap_v2.db`（SQLite）仅用于迁移期历史实现/兼容性验证，不作为对外交付的数据源

## 推荐阅读顺序

- [数据库V2数据字典](data_dictionary/index.md)

---

## V2 架构（SQL Server：mddap_v2）

### 特点

- 分层设计：原始层（ODS）+ 计算层（DWD）
- ETL 只做简单清洗，不做复杂计算
- 计算逻辑口径由数据库层定义，但为保证 BI 性能，MES 指标采用“物化表 + 原子切换”方式提供
- `PreviousBatchEndTime` / `Setup` 等需要窗口排序的字段在刷新时预计算，避免 BI 查询触发大排序

### 表/视图结构

#### 原始层（ODS）- 前缀 `raw_`

- `raw_mes`：MES 批次产出原始数据
- `raw_sfc`：SFC 批次报工原始数据
- `raw_sfc_inspection`：SFC 产品检验原始数据
- `raw_sap_routing`：SAP 工艺路线原始数据
- `raw_sap_labor_hours`：SAP 工时原始数据
- `raw_calendar`：日历工作日表

#### 计算层（DWD）- 前缀 `v_`

- `v_mes_metrics`：MES 指标视图（含 LT/PT/ST 等计算）

#### 计算层（DWD）- 物化快照（推荐 BI 连接的真实数据来源）

- `mes_metrics_snapshot_a` / `mes_metrics_snapshot_b`：MES 指标物化快照表（A/B 轮换写入）
- `mes_metrics_current`：同义词（synonym），指向当前生效的快照表

> BI 侧仍然只连接 `v_mes_metrics`，但 `v_mes_metrics` 会改为从 `mes_metrics_current` 读取，保证秒开。

#### 系统表

- `etl_file_state`：文件状态（增量检测）
- `etl_run_log`：ETL 运行日志

### 对应 ETL

| ETL 脚本 | 目标表 | 说明 |
|----------|--------|------|
| `etl_mes_batch_output_raw.py` | `raw_mes` | MES 批次产出 |
| `etl_sfc_batch_output_raw.py` | `raw_sfc` | SFC 批次报工 |
| `etl_sfc_inspection_raw.py` | `raw_sfc_inspection` | SFC 产品检验 |
| `etl_sap_routing_raw.py` | `raw_sap_routing` | SAP 工艺路线 |
| `etl_sap_labor_hours.py` | `raw_sap_labor_hours` | SAP 工时（YPP/EH Excel） |

---

## V1 架构（mddap.db）（历史）

### 特点

- ETL 在 Python 中完成所有计算
- 存储计算后的宽表
- 每次增量需要重算 `PreviousBatchEndTime`

### 表结构

- `mes_batch_report`：MES 批次报告（含计算字段）
- `sfc_batch_report`：SFC 批次报告
- `sfc_product_inspection`：SFC 产品检验
- `sap_routing`：SAP 工艺路线
- `etl_file_state`：文件状态
- `etl_run_log`：ETL 日志

---

## 命名规范

### 数据库文件

- `mddap.db`：V1 架构数据库
- `mddap_v2.db`：V2 架构数据库

### 表命名

| 前缀 | 含义 | 示例 |
|------|------|------|
| `raw_` | 原始数据表 | `raw_mes`, `raw_sfc` |
| `v_` | 计算视图 | `v_mes_metrics` |
| `etl_` | ETL 系统表 | `etl_file_state` |

### Schema 文件

- `init_schema.sql`：V1 架构
- `init_schema_v2.sql`：V2 架构

---

## 使用建议

### Power BI 连接

- V1：直接查询 `mes_batch_report`
- V2：查询 `v_mes_metrics`（推荐）

### 新增数据表流程

1. 在 `init_schema_v2.sql` 中添加 `raw_xxx` 表定义
2. 创建对应的 `etl_xxx_raw.py` 脚本
3. 如需计算字段，创建 `v_xxx` 视图
4. 更新/初始化数据库

---

## 版本历史

| 日期 | 版本 | 变更 |
|------|------|------|
| 2025-12-12 | V1 | 初始版本，ETL 计算模式 |
| 2025-12-12 | V2 | 分层架构，视图计算模式 |
