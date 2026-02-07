# 开发者文档

本章节面向开发者和技术人员，提供项目的技术架构、开发规范和变更记录。

## 📁 文档结构

### 架构设计
- [数据架构](architecture/data-architecture.md) - 数据库表结构、视图定义、ER关系图
- [ETL架构](architecture/etl-architecture.md) - ETL流程、数据流向、依赖关系

### 开发规范
- [编码规范](standards/coding-conventions.md) - Python/SQL编码标准
- [命名规范](standards/naming-conventions.md) - 文件、字段、变量命名规则
- [项目结构](standards/project-structure.md) - 目录组织和文件布局

### 变更记录
- [开发日志](changelog/development-log.md) - 重要功能开发和修复记录
- [迁移记录](changelog/migration-log.md) - 架构迁移和重构记录

## 🔗 快速链接

| 资源 | 说明 |
|------|------|
| [数据字典](../reference/data-dictionary/index.md) | 完整字段定义和映射关系 |
| [ETL流程](../etl/index.md) | ETL操作指南 |
| [计算逻辑](../reference/data-dictionary/calculation-logic.md) | LT/PT/ST计算公式 |

## 📋 开发环境

```bash
# Python 版本
Python 3.12+

# 主要依赖
pandas >= 2.0
openpyxl >= 3.1
pyyaml >= 6.0

# 数据库
SQLite 3 (开发/测试)
```

## 🚀 快速开始

### 运行 ETL（测试模式）

```bash
# MES 数据
python -m data_pipelines.sources.mes.etl.etl_mes_batch_output_raw --test

# SFC 数据
python -m data_pipelines.sources.sfc.etl.etl_sfc_batch_output_raw --test

# SAP Routing 数据
python -m data_pipelines.sources.sap.etl.etl_sap_routing_raw --test
```

### 查看计算视图

```sql
-- 查询 v_mes_metrics 视图
SELECT * FROM v_mes_metrics LIMIT 10;
```
