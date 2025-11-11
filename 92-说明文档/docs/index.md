# CZ Ops 数字化数据平台 - 电子说明书

欢迎使用 **CZ Ops 数字化数据平台电子说明书**！

本说明书提供完整的数据字段定义、计算方法、数据来源和使用指南，帮助您快速了解和使用数据平台。

---

## 📊 平台概述

CZ Ops 数字化数据平台是一个集成的生产运营数据分析系统，提供：

- **SA 指标**（Schedule Adherence - 计划达成率）
- **多维度 KPI 指标**（生产、质量、持续改进、供应链等）
- **自动化 ETL 数据处理**
- **Power BI 可视化报表**

---

## 🎯 快速导航

### 核心指标：SA - 计划达成率

**Schedule Adherence（SA）** 是供应链 KPI 的核心指标，已在平台中完整实施。

<div class="grid cards" markdown>

- :material-view-dashboard: **[KPI 指标体系](kpi/index.md)**

    查看完整的 KPI 指标体系架构

- :material-truck-fast: **[供应链 KPI](kpi/supply-chain.md)**

    了解供应链部门的所有 KPI 指标

- :material-chart-timeline: **[SA 指标详细说明](sa/index.md)**

    深入了解 SA 指标的计算方法和使用

- :material-calculator: **[SA 计算示例](sa/examples.md)**

    通过实际案例理解 SA 计算过程

</div>

### KPI 指标
涵盖生产、质量、持续改进、供应链等多个维度的关键绩效指标。

<div class="grid cards" markdown>

- :material-factory: **[生产部门 KPI](kpi/production.md)**

    OEE、生产计划完成率、产能利用率等

- :material-shield-check: **[质量部门 KPI](kpi/quality.md)**

    一次合格率、不合格品率、CAPA 完成率等

- :material-trending-up: **[持续改进 KPI](kpi/ci.md)**

    改善项目完成率、成本节约、认证率等

- :material-truck: **[供应链 KPI](kpi/supply-chain.md)**

    库存周转率、准时交付率、供应商绩效等

</div>

### 数据处理
了解后台 ETL 数据处理流程和 Power Query 查询代码。

<div class="grid cards" markdown>

- :material-cog: **[ETL 流程说明](etl/etl-process.md)**

    了解数据清洗、转换、加载的完整流程

- :material-file-document-edit: **[配置说明](etl/configuration.md)**

    ETL 配置文件的参数说明和修改方法

- :material-microsoft-excel: **[Power Query 代码](pq/index.md)**

    Power BI 中使用的查询代码说明

- :material-refresh: **[增量刷新方案](pq/incremental-refresh.md)**

    了解如何优化 Power BI 数据刷新性能

</div>

---

## 🚀 快速开始

### 对于数据使用者

1. 📖 阅读 **[快速开始指南](guide/quick-start.md)**
2. 🔍 使用顶部搜索框查找具体字段或指标
3. ❓ 遇到问题查看 **[常见问题](guide/faq.md)**

### 对于数据管理者

1. 📂 了解 **[数据更新流程](guide/data-update.md)**
2. ⚙️ 配置 **[ETL 处理](etl/index.md)**
3. 🛠️ 参考 **[故障排查](guide/troubleshooting.md)**

---

## 📚 文档结构

```
📖 电子说明书
├── 🎯 SA 指标
│   ├── 计算方法与定义
│   ├── 字段说明
│   ├── 数据源说明
│   └── 计算示例
├── 📊 KPI 指标
│   ├── 生产部门 KPI
│   ├── 质量部门 KPI
│   ├── 持续改进 KPI
│   └── 供应链 KPI
├── ⚙️ 数据处理
│   ├── ETL 流程说明
│   ├── SA 数据清洗
│   ├── SFC 数据清洗
│   └── 配置说明
├── 📝 Power Query
│   ├── MES 报工记录
│   ├── SFC 报工记录
│   └── 增量刷新方案
├── 📖 使用指南
│   ├── 快速开始
│   ├── 数据更新流程
│   ├── 常见问题
│   └── 故障排查
└── 📌 参考信息
    ├── 文件命名规范
    └── 字典表说明
```

---

## 💡 功能特性

- ✅ **智能搜索**：支持中英文全文搜索
- ✅ **响应式设计**：支持手机、平板、电脑访问
- ✅ **离线使用**：可生成静态 HTML 离线浏览
- ✅ **代码高亮**：支持 Python、SQL、Power Query 等语法高亮
- ✅ **主题切换**：支持亮色/暗色主题切换
- ✅ **版本追踪**：自动显示文档最后更新时间

---

## 📞 联系我们

如有问题或建议，请联系：

- **项目团队**：CZ Ops 数字化团队
- **技术支持**：数据平台技术支持组

---

!!! tip "提示"
    使用顶部搜索框可以快速查找任何字段、指标或概念！

!!! note "版本信息"
    - **文档版本**：v1.0
    - **最后更新**：2025-11-10
    - **适用平台**：CZ Ops 数字化数据平台

