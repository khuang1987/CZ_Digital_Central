# MDDAP 数据库指南

## 数据库

本项目当前使用 **SQL Server** 作为数据库：

- Server: `localhost\SQLEXPRESS`
- Database: `mddap_v2`

## 架构概览

- **原始层 (ODS)**：以 `dbo.raw_` 前缀表存储各来源的清洗后原始数据
- **计算层 (DWD)**：以 `dbo.v_` 前缀视图承载指标/衍生字段计算

### 表结构

#### 原始层 (ODS) - 前缀 `raw_`
```
dbo.raw_mes               # MES 批次产出原始数据
dbo.raw_sfc               # SFC 批次报工原始数据
dbo.raw_sfc_inspection    # SFC 产品检验原始数据
dbo.raw_sap_routing       # SAP 工艺路线原始数据
dbo.raw_calendar          # 日历工作日表
```

#### 计算层 (DWD) - 前缀 `v_`
```
dbo.v_mes_metrics         # MES 指标视图（含 LT/PT/ST 计算）
```

#### 计算层 (DWD) - 物化快照（BI 秒开）
```
dbo.mes_metrics_snapshot_a    # MES 指标物化快照（A）
dbo.mes_metrics_snapshot_b    # MES 指标物化快照（B）
dbo.mes_metrics_current       # synonym：指向当前生效的快照表
```

#### 系统表
```
etl_file_state        # 文件状态（增量检测）
etl_run_log           # ETL 运行日志
```

### 对应 ETL
| ETL 脚本 | 目标表 | 说明 |
|----------|--------|------|
| `etl_mes_batch_output_raw.py` | `raw_mes` | MES 批次产出 |
| `etl_sfc_batch_output_raw.py` | `raw_sfc` | SFC 批次报工 |
| `etl_sfc_inspection_raw.py` | `raw_sfc_inspection` | SFC 产品检验 |
| `etl_sap_routing_raw.py` | `raw_sap_routing` | SAP 工艺路线 |

---

## 命名规范

### 表命名
| 前缀 | 含义 | 示例 |
|------|------|------|
| `raw_` | 原始数据表 | `raw_mes`, `raw_sfc` |
| `v_` | 计算视图 | `v_mes_metrics` |
| `etl_` | ETL 系统表 | `etl_file_state` |

### ETL 脚本命名
```
etl_{数据源}_{业务表}_{数据类型}.py

示例：
- etl_mes_batch_output_raw.py    # MES 批次产出原始数据
- etl_sfc_batch_report_raw.py    # SFC 批次报告原始数据
- etl_sap_routing_raw.py         # SAP 工艺路线原始数据
```

### Schema 文件命名
```
init_schema_v2.sql    # V2 schema（与 SQL Server 表结构对齐）
```

---

## 使用建议

### Power BI 连接
- **V1**：直接查询 `mes_batch_report` 表
- **V2**：查询 `v_mes_metrics` 视图（推荐）

### 新增数据表流程
1. 在 `init_schema_v2.sql` 中添加 `raw_xxx` 表定义
2. 创建对应的 `etl_xxx_raw.py` 脚本
3. 如需计算字段，创建 `v_xxx` 视图
4. 通过项目脚本在 SQL Server 中创建/更新数据库对象

## 常用操作

### 刷新/创建 MES 计算视图

```powershell
python scripts/_execute_create_view_v2.py
```

### 刷新 MES 指标物化快照（推荐 BI 使用）

- 刷新方案说明：
  - `docs/02_technical/database/mes_metrics_materialization.md`

> 物化刷新脚本会采用 A/B 双表轮换写入并原子切换 `dbo.mes_metrics_current`。

```powershell
python scripts/_refresh_mes_metrics_materialized.py
```

### 清空 MES 原始表（仅 raw_mes）

```powershell
python scripts/_clear_raw_mes_sqlserver.py
```

### 将 ProductionOrder 列迁移为 INT（如需要）

```powershell
python scripts/_alter_raw_mes_productionorder_to_int.py
```

---

## 版本历史

| 日期 | 版本 | 变更 |
|------|------|------|
| 2025-12-12 | V1 | 初始版本，ETL 计算模式 |
| 2025-12-12 | V2 | 分层架构，视图计算模式 |
