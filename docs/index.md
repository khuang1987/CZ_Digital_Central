# 常州园区数据报表平台

欢迎使用 **常州园区数据报表平台**！这是一个集成了数据采集、清洗、监控与可视化的综合性数据解决方案。

---

## 🏗️ 平台架构与技术路线

本平台采用现代化的数据工程架构，将分散孤立的业务数据转化为可信赖的决策依据。

```mermaid
graph LR
    subgraph "1. Data Sources (数据源)"
        MES[MES 系统]
        SFC[SFC 系统]
        SAP[SAP ERP]
        Excel[Offline Files]
    end

    subgraph "2. Python ETL (数据处理)"
        Clean[Data Cleaning <br/>(清洗与标准化)]
        Logic[Business Logic <br/>(KPI 计算引擎)]
        Alert[Alert Engine <br/>(异常监控)]
    end

    subgraph "3. Data Storage (数据存储)"
        SQL[(SQL Server <br/> Data Warehouse)]
        ODS[ODS 层/原始数据]
        DWD[DWD 层/明细数据]
        DWS[DWS 层/汇总数据]
    end

    subgraph "4. Consumption (应用)"
        PBI[Power BI Reports]
        Planner[Planner Tasks <br/>(闭环追踪)]
        Email[Email Alerts]
    end

    MES & SFC & SAP & Excel --> Clean
    Clean --> Logic
    Logic --> SQL
    SQL --> PBI
    Logic --> Alert
    Alert --> Planner & Email

    classDef source fill:#e1f5fe,stroke:#01579b,stroke-width:2px;
    classDef etl fill:#fff3e0,stroke:#e65100,stroke-width:2px;
    classDef db fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px;
    classDef app fill:#f3e5f5,stroke:#4a148c,stroke-width:2px;

    class MES,SFC,SAP,Excel source;
    class Clean,Logic,Alert etl;
    class SQL,ODS,DWD,DWS db;
    class PBI,Planner,Email app;
```

### 🔁 数据处理全流程

我们的数据之旅经历了以下关键阶段：

1.  **数据采集 (Ingestion)**: 
    *   通过 Python 脚本自动从 MES、SFC、SAP 等系统提取原始数据。
    *   支持处理 Excel 手工账表，打破数据孤岛。

2.  **清洗与聚合 (Cleaning & Aggregation)**:
    *   **标准化**: 统一不同来源的字段命名（如 `Material`, `Batch`）。
    *   **聚合**: 将工单级、批次级数据关联，形成完整的生产履历。
    *   **逻辑计算**: 内置 SA (计划达成率)、ST (标准工时) 等核心算法，确保指标一致性。

3.  **存储与服务 (Storage)**:
    *   数据最终存储于 **SQL Server** 数据仓库中。
    *   分层架构 (ODS/DWD/DWS) 确保了数据的追溯性和查询效率。

4.  **应用与展现 (Visualization)**:
    *   **Power BI**: 直连 SQL Server，提供高性能的交互式报表。
    *   **智能监控**: 异常数据（如工时偏差、生产超期）自动触发预警，推送到 Planner 进行闭环管理。

---

## 🌟 核心价值

*   **单一事实来源 (Single Source of Truth)**: 消除 Excel 满天飞的现状，所有部门基于同一套数据说话。
*   **自动化 (Automation)**: 全流程无人值守运行，不仅解放人力，更消除了人为统计误差。
*   **闭环管理 (Closed Loop)**: 不仅仅是“看”数据，更通过监控引擎驱动问题的“解决”。

---

## 🚀 快速开始 (Quick Start)

根据您的角色，我们为您推荐最佳的使用路径：

### 👨‍💻 我是数据分析师
*   **查看数据定义**: [数据定义与来源](business/data-definitions.md) - 了解字段含义与数据流向。
*   **理解计算逻辑**: [核心计算逻辑](business/logic.md) - 深入 SA、ST、变差等指标的计算公式。
*   **查看 KPI 体系**: [KPI 指标体系](business/kpi/index.md) - 全面的指标字典。

### 🔧 我是数据工程师/开发人员
*   **ETL 开发**: [ETL 流程详解](developer/etl-process.md) - 掌握数据清洗与转换流程。
*   **任务调度**: [并行调度器](developer/orchestrator.md) - 了解多进程并行处理架构。
*   **架构设计**: [架构设计](developer/data-architecture.md) - 系统整体架构图与设计原则。

### 📊 我是业务用户/管理人员
*   **监控概览**: [监控体系概览](monitoring/overview.md) - 了解系统如何监控业务异常。
*   **预警规则**: [预警规则](monitoring/triggers.md) - 查看触发报警的具体条件。
*   **问题排查**: [常见问题 (FAQ)](developer/ops/faq.md) - 快速解决使用中的疑问。

---

## 💡 常用场景导航

| 您的需求 | 推荐文档 |
| :--- | :--- |
| **"这个字段代表什么意思？"** | 👉 [数据定义与来源](business/data-definitions.md) |
| **"SA 指标是怎么算出来的？"** | 👉 [核心计算逻辑](business/logic.md) |
| **"为什么今天的数据没更新？"** | 👉 [故障排查](developer/ops/troubleshooting.md) |
| **"收到了报警邮件，如何处理？"** | 👉 [Planner 闭环集成](monitoring/planner-integration.md) |
| **"我想自己开发 Power BI 报表"** | 👉 [Power Query 代码](developer/pq/index.md) |

---

## 📞 支持与联系

如有任何问题，请联系数据平台负责人：**黄凯 (kai.huang2@medtronic.com)**
