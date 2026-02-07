# ETL 架构设计

本文档描述 MDDAP V2 数据平台的 ETL 流程架构。

## ETL 流程概览

```
┌─────────────────────────────────────────────────────────────────┐
│                         数据源                                   │
├───────────────────┬───────────────────┬─────────────────────────┤
│   MES Excel       │   SFC Excel       │   SAP Routing Excel     │
│   (SharePoint)    │   (SharePoint)    │   (SharePoint)          │
└─────────┬─────────┴─────────┬─────────┴───────────┬─────────────┘
          │                   │                     │
          ▼                   ▼                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                      ETL 脚本层                                  │
├───────────────────┬───────────────────┬─────────────────────────┤
│ etl_mes_batch_    │ etl_sfc_batch_    │ etl_sap_routing_        │
│ output_raw.py     │ output_raw.py     │ raw.py                  │
└─────────┬─────────┴─────────┬─────────┴───────────┬─────────────┘
          │                   │                     │
          │    ┌──────────────┼──────────────┐      │
          │    │              │              │      │
          ▼    ▼              ▼              ▼      ▼
┌─────────────────────────────────────────────────────────────────┐
│                      SQLite 数据库                               │
│                      (mddap_v2.db)                              │
├───────────────────┬───────────────────┬─────────────────────────┤
│     raw_mes       │     raw_sfc       │   raw_sap_routing       │
└───────────────────┴───────────────────┴─────────────────────────┘
```

---

## ETL 脚本说明

### MES ETL

| 项目 | 说明 |
|------|------|
| **脚本路径** | `data_pipelines/sources/mes/etl/etl_mes_batch_output_raw.py` |
| **配置文件** | `data_pipelines/sources/mes/config/config_mes_batch_report.yaml` |
| **数据源** | `CZ Production - 文档/General/POWER BI 数据源 V2/30-MES导出数据/` |
| **文件格式** | `CMES_Product_Output_*.xlsx` |
| **目标表** | `raw_mes` |

**主要功能**：
1. 读取 MES Excel 文件（支持多工厂）
2. 字段映射和标准化
3. 从 `Resource` 提取机台号
4. 从 `LogicalFlowPath` 提取工艺组编号
5. 生成 `record_hash` 用于去重
6. 写入数据库

### SFC ETL

| 项目 | 说明 |
|------|------|
| **脚本路径** | `data_pipelines/sources/sfc/etl/etl_sfc_batch_output_raw.py` |
| **配置文件** | `data_pipelines/sources/sfc/config/config_sfc_batch_output.yaml` |
| **数据源** | `CZ Production - 文档/General/POWER BI 数据源 V2/70-SFC导出数据/批次报工汇总报表/` |
| **文件格式** | `LC-*.xlsx` |
| **目标表** | `raw_sfc` |

**主要功能**：
1. 读取 SFC 批次报工 Excel 文件
2. 字段映射（中英文列名兼容）
3. 时间字段标准化
4. 去重并写入数据库

### SAP Routing ETL

| 项目 | 说明 |
|------|------|
| **脚本路径** | `data_pipelines/sources/sap/etl/etl_sap_routing_raw.py` |
| **配置文件** | `data_pipelines/sources/sap/config/config_sap_routing.yaml` |
| **数据源** | `General - CZ OPS生产每日产出登记/` |
| **文件格式** | `*Routing及机加工产品清单.xlsx` |
| **目标表** | `raw_sap_routing` |

**主要功能**：
1. 读取 SAP Routing Excel（两个Sheet：Routing + 机加工清单）
2. 合并两个 Sheet 的数据
3. 字段映射和标准化
4. 去重并写入数据库

---

## 运行命令

### 测试模式（推荐首次运行）

```bash
# MES ETL（每工厂最多3个文件，每文件最多1000行）
python -m data_pipelines.sources.mes.etl.etl_mes_batch_output_raw --test

# SFC ETL
python -m data_pipelines.sources.sfc.etl.etl_sfc_batch_output_raw --test

# SAP Routing ETL
python -m data_pipelines.sources.sap.etl.etl_sap_routing_raw --test
```

### 完整模式

```bash
# MES ETL（处理所有文件）
python -m data_pipelines.sources.mes.etl.etl_mes_batch_output_raw

# SFC ETL
python -m data_pipelines.sources.sfc.etl.etl_sfc_batch_output_raw

# SAP Routing ETL
python -m data_pipelines.sources.sap.etl.etl_sap_routing_raw
```

---

## 去重机制

### record_hash 生成规则

| 表 | Hash 组成字段 |
|---|--------------|
| raw_mes | `BatchNumber` + `Operation` + `Machine` + `TrackOutTime` |
| raw_sfc | `BatchNumber` + `Operation` + `TrackOutTime` |
| raw_sap_routing | `ProductNumber` + `Operation` + `factory_code` |

### 去重流程

1. **DataFrame 内部去重**：移除同一批次读取中的重复记录
2. **数据库去重**：跳过已存在于数据库中的记录（基于 `record_hash`）

---

## 文件处理状态跟踪

ETL 使用 `etl_file_status` 表跟踪已处理的文件：

| 字段 | 说明 |
|------|------|
| file_path | 文件完整路径 |
| file_hash | 文件内容哈希 |
| etl_name | ETL 名称 |
| processed_at | 处理时间 |

**作用**：避免重复处理未变化的文件，提高 ETL 效率。

---

## 日志位置

| ETL | 日志目录 |
|-----|---------|
| MES | `logs/mes/etl_mes_raw_YYYYMMDD.log` |
| SFC | `logs/sfc/etl_sfc_raw_YYYYMMDD.log` |
| SAP | `logs/sap/etl_sap_raw_YYYYMMDD.log` |

---

## 相关文档

- [数据架构](data-architecture.md)
- [字段映射](../../reference/data-dictionary/field-mapping.md)
- [ETL操作指南](../../etl/index.md)
