# 快速开始指南

欢迎使用 CZ Ops 数字化数据平台！本指南将帮助您快速上手。

---

## 📖 阅读本文档

### 在线浏览（推荐）

如果您正在通过浏览器查看本文档，您可以：

- 🔍 **使用搜索功能**：点击顶部搜索框，输入关键词快速查找
- 📑 **浏览导航菜单**：左侧导航栏包含所有章节
- 🌓 **切换主题**：点击右上角图标切换亮色/暗色主题
- 📱 **移动端友好**：支持手机、平板浏览

### 离线浏览

如果您使用的是离线版本（HTML 文件）：

1. 双击打开 `site/index.html`
2. 所有功能（搜索、导航等）完全可用
3. 无需网络连接

---

## 🎯 根据角色快速定位

### 我是数据分析师

**您可能需要：**

1. 📊 **了解 SA 指标**
   - [SA 指标概述](../business/kpi/sa.md)
   - [计算方法与定义](../business/kpi/sa.md#-计算方法与定义)
   - [字段说明](../business/kpi/sa.md#-字段说明)

2. 📈 **了解 KPI 指标**
   - [KPI 指标概述](../business/kpi/index.md)
   - [生产部门 KPI](../business/kpi/production.md)
   - [质量部门 KPI](../business/kpi/quality.md)

3. 💡 **Power BI 开发**
   - [Power Query 代码](../developer/pq/index.md)
   - [增量刷新方案](../developer/pq/incremental-refresh.md)

---

### 我是数据工程师

**您可能需要：**

1. ⚙️ **ETL 处理流程**
   - [ETL 流程概述](../developer/etl-process.md)
   - [并行调度器](../developer/orchestrator.md)

2. 🔧 **配置和维护**
   - [数据更新流程](../knowledge-base/operations.md#1-如何运行数据更新)
   - [故障排查](../knowledge-base/troubleshooting.md)

3. 📂 **数据源管理**
   - [数据定义与来源](../business/data-definitions.md)

---

### 我是业务用户

**您可能需要：**

1. 📊 **查看报表**
   - 访问 Power BI 报表
   - 参考 [KPI 指标说明](../business/kpi/index.md)

2. 🔍 **查询字段定义**
   - 参考 [业务逻辑](../business/logic.md)

3. ❓ **解决问题**
   - 查看 [常见问题](../knowledge-base/faq.md)
   - 联系技术支持团队

---

### 我是项目管理者

**您可能需要：**

1. 📋 **项目文档**
   - [项目定义](../archive/legacy_backup/project/01_Define_定义阶段/index.md)
   - [项目进展总览](../archive/legacy_backup/project/progress/index.md)

2. 📊 **整体架构**
   - [架构设计](../developer/architecture/data-architecture.md)

---

## 🔍 常用查询场景

### 场景 1：我想知道某个字段的含义

**方法**：
- [数据定义](../business/data-definitions.md)

---

### 场景 2：我想了解 SA 指标如何计算

**推荐路径：**
1. [业务逻辑](../business/logic.md) - 核心公式
2. [SA 指标概述](../business/kpi/sa.md) - 详细定义

---

### 场景 3：我想更新数据

**推荐路径：**
1. [操作指南](../knowledge-base/operations.md)

---

### 场景 4：我想开发 Power BI 报表

**推荐路径：**
1. [数据源说明](../business/data-definitions.md)
2. [Power Query 代码](../developer/pq/index.md)

---

## ⏭️ 下一步

根据您的角色，选择以下路径之一：

=== "数据分析师"
    👉 继续阅读 [核心业务逻辑](../business/logic.md)

=== "数据工程师"
    👉 继续阅读 [ETL 流程说明](../developer/etl-process.md)

=== "业务用户"
    👉 继续阅读 [KPI 指标说明](../business/kpi/index.md)

=== "项目管理者"
    👉 继续阅读 [监控体系概览](../monitoring/overview.md)

---

!!! tip "提示"
    将本文档加入浏览器收藏夹，方便随时查阅！

