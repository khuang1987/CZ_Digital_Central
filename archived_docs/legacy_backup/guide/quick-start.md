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
   - [SA 指标概述](../kpi/sa.md)
   - [计算方法与定义](../kpi/sa.md#-计算方法与定义)
   - [字段说明](../kpi/sa.md#-字段说明)

2. 📈 **了解 KPI 指标**
   - [KPI 指标概述](../kpi/index.md)
   - [生产部门 KPI](../kpi/production.md)
   - [质量部门 KPI](../kpi/quality.md)

3. 💡 **Power BI 开发**
   - [Power Query 代码](../pq/index.md)
   - [增量刷新方案](../pq/incremental-refresh.md)

---

### 我是数据工程师

**您可能需要：**

1. ⚙️ **ETL 处理流程**
   - [ETL 流程概述](../etl/index.md)
   - [SA 数据清洗](../etl/etl-sa.md)
   - [SFC 数据清洗](../etl/etl-sfc.md)

2. 🔧 **配置和维护**
   - [配置说明](../etl/configuration.md)
   - [数据更新流程](data-update.md)
   - [故障排查](troubleshooting.md)

3. 📂 **数据源管理**
   - [数据源说明](../kpi/sa.md#-数据源说明)

---

### 我是业务用户

**您可能需要：**

1. 📊 **查看报表**
   - 访问 Power BI 报表（链接待补充）
   - 参考 [KPI 指标说明](../kpi/index.md)

2. 🔍 **查询字段定义**
   - 使用顶部搜索框输入字段名
   - 参考 [SA 字段说明](../kpi/sa.md#-字段说明)

3. ❓ **解决问题**
   - 查看 [常见问题](faq.md)
   - 联系技术支持团队

---

### 我是项目管理者

**您可能需要：**

1. 📋 **项目文档**
   - [项目定义](../project/01_Define_定义阶段/index.md)
   - [项目进展总览](../project/progress/index.md)
   - [会议纪要](../project/meetings/index.md)

2. 📊 **整体架构**
   - [数据源说明](../kpi/sa.md#-数据源说明)
   - [ETL 流程](../etl/index.md)

---

## 🔍 常用查询场景

### 场景 1：我想知道某个字段的含义

**方法 1：使用搜索**
1. 点击顶部搜索框
2. 输入字段名（如 "TrackOutTime"）
3. 查看搜索结果

**方法 2：查看字段说明**
- [SA 字段说明](../kpi/sa.md#-字段说明)
- [KPI 指标体系](../kpi/index.md)

---

### 场景 2：我想了解 SA 指标如何计算

**推荐路径：**
1. [SA 指标概述](../kpi/sa.md) - 了解基本概念
2. [计算方法与定义](../kpi/sa.md#-计算方法与定义) - 详细计算公式
3. [计算示例](../kpi/sa.md#-计算示例) - 实际案例演示

---

### 场景 3：我想更新数据

**推荐路径：**
1. [数据更新流程](data-update.md) - 完整操作指南
2. [ETL 处理流程](../etl/etl-process.md) - 了解后台处理
3. [故障排查](troubleshooting.md) - 遇到问题时参考

---

### 场景 4：我想开发 Power BI 报表

**推荐路径：**
1. [数据源说明](../kpi/sa.md#-数据源说明) - 了解数据位置和格式
2. [Power Query 代码](../pq/index.md) - 参考现有查询
3. [增量刷新方案](../pq/incremental-refresh.md) - 优化性能

---

## 学习路径

### 初级（1-2 天）

1. 阅读 [SA 指标概述](../kpi/sa.md#sa-overview)
2. 理解 [计算方法](../kpi/sa.md#calculation-method)
3. 了解 [数据源](../kpi/sa.md#data-sources)
4. 查看 [字段说明](../kpi/sa.md#field-specification)

**目标：** 理解核心概念和数据结构

---

### 中级（3-5 天）

1. 深入学习 [计算方法](../kpi/sa.md#calculation-method)
2. 分析 [计算示例](../kpi/sa.md#calculation-example)
3. 理解 [字段定义](../kpi/sa.md#field-specification)
4. 实践 [计算示例](../kpi/sa.md#calculation-example)

**目标：** 掌握计算逻辑和数据处理流程

---

### 高级（1-2 周）

1. 配置和运行 [ETL 脚本](../etl/etl-sa.md)
2. 开发自定义 Power BI 报表
3. 优化 [增量刷新](../pq/incremental-refresh.md)
4. [故障排查](troubleshooting.md) 和性能优化

**目标：** 独立开发和维护数据平台

---

## 🆘 获取帮助

### 文档内查找

1. **搜索功能**：顶部搜索框
2. **导航目录**：左侧导航栏
3. **页面内目录**：右侧目录（桌面版）

### 常见问题

- [常见问题 FAQ](faq.md)
- [故障排查指南](troubleshooting.md)

### 联系支持

- **技术支持**：数据平台技术支持组
- **业务咨询**：CZ Ops 数字化团队

---

## ⏭️ 下一步

根据您的角色，选择以下路径之一：

=== "数据分析师"
    👉 继续阅读 [SA 指标详细说明](../kpi/sa.md#-计算方法与定义)

=== "数据工程师"
    👉 继续阅读 [ETL 流程说明](../etl/etl-process.md)

=== "业务用户"
    👉 继续阅读 [KPI 指标说明](../kpi/index.md)

=== "项目管理者"
    👉 继续阅读 [项目定义](../project/01_Define_定义阶段/index.md)

---

!!! tip "提示"
    将本文档加入浏览器收藏夹，方便随时查阅！

