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

### 项目管理 / DMAIC 阶段
从“项目视角”快速了解当前数据平台改进项目的背景、进展和各阶段产出。

<div class="grid cards" markdown>

- :material-clipboard-text-outline: **[项目总览](project/progress/index.md)**

    查看整体时间线、里程碑和当前进展状态

- :material-chart-timeline-variant: **[DMAIC 阶段文档](project/01_Define_定义阶段/index.md)**

    从 Define / Measure / Analyze / Improve / Control 逐阶段了解项目沉淀

- :material-account-group: **[会议纪要](project/meetings/index.md)**

    查看关键决策、行动项和历史会议记录

</div>

---

## 🚀 使用指南

=== "快速开始"

    ### 对于数据使用者

    1. 📖 阅读 **[快速开始指南](guide/quick-start.md)**
    2. 🔍 使用顶部搜索框查找具体字段或指标
    3. ❓ 遇到问题查看 **[常见问题](guide/faq.md)**

    ### 对于数据管理者

    1. 📂 了解 **[数据更新流程](guide/data-update.md)**
    2. ⚙️ 配置 **[ETL 处理](etl/index.md)**
    3. 🛠️ 参考 **[故障排查](guide/troubleshooting.md)**

    ---

    ### 📖 快速开始指南

    欢迎使用 CZ Ops 数字化数据平台！本指南将帮助您快速上手。

    #### 🎯 根据角色快速定位

    === "数据分析师"
        **您可能需要：**
        
        1. 📊 **了解 SA 指标**
           - [SA 指标概述](sa/index.md)
           - [计算方法与定义](sa/calculation-definition.md)
           - [字段说明](sa/field-specification.md)
        
        2. 📈 **了解 KPI 指标**
           - [KPI 指标概述](kpi/index.md)
           - [生产部门 KPI](kpi/production.md)
           - [质量部门 KPI](kpi/quality.md)
        
        3. 💡 **Power BI 开发**
           - [Power Query 代码](pq/index.md)
           - [增量刷新方案](pq/incremental-refresh.md)

    === "数据工程师"
        **您可能需要：**
        
        1. ⚙️ **ETL 处理流程**
           - [ETL 流程概述](etl/index.md)
           - [SA 数据清洗](etl/etl-sa.md)
           - [SFC 数据清洗](etl/etl-sfc.md)
        
        2. 🔧 **配置和维护**
           - [配置说明](etl/configuration.md)
           - [数据更新流程](guide/data-update.md)
           - [故障排查](guide/troubleshooting.md)
        
        3. 📂 **数据源管理**
           - [数据源说明](sa/data-sources.md)

    === "业务用户"
        **您可能需要：**
        
        1. 📊 **查看报表**
           - 访问 Power BI 报表（链接待补充）
           - 参考 [KPI 指标说明](kpi/index.md)
        
        2. 🔍 **查询字段定义**
           - 使用顶部搜索框输入字段名
           - 参考 [SA 字段说明](sa/field-specification.md)
        
        3. ❓ **解决问题**
           - 查看 [常见问题](guide/faq.md)
           - 联系技术支持团队

    === "项目管理者"
        **您可能需要：**
        
        1. 📋 **项目文档**
           - [项目定义](project/01_Define_定义阶段/index.md)
           - [项目进展总览](project/progress/index.md)
           - [会议纪要](project/meetings/index.md)
        
        2. 📊 **整体架构**
           - [数据源说明](sa/data-sources.md)
           - [ETL 流程](etl/index.md)

    ---

    #### 🔍 常用查询场景

    **场景 1：我想知道某个字段的含义**
    - 使用顶部搜索框输入字段名（如 "TrackOutTime"）
    - 或查看 [SA 字段说明](sa/field-specification.md)

    **场景 2：我想了解 SA 指标如何计算**
    - [SA 指标概述](sa/index.md) → [计算方法与定义](sa/calculation-definition.md) → [计算示例](sa/examples.md)

    **场景 3：我想更新数据**
    - 查看 [数据更新流程](guide/data-update.md) 标签页

    **场景 4：我想开发 Power BI 报表**
    - [数据源说明](sa/data-sources.md) → [Power Query 代码](pq/index.md) → [增量刷新方案](pq/incremental-refresh.md)

=== "数据更新流程"

    本文档详细说明如何更新数据平台的数据。

    #### 更新流程概览

    ```mermaid
    graph TD
        A[准备原始数据] --> B{更新哪类数据?}
        B -->|MES数据| C[上传MES文件到SharePoint]
        B -->|SFC数据| D[上传SFC文件到SharePoint]
        B -->|标准时间| E[上传Routing文件到SharePoint]
        
        C --> F[运行 run_etl.bat]
        D --> G[运行 etl_sfc.py]
        E --> H[运行 convert_standard_time.py]
        
        G --> F
        H --> F
        
        F --> I{处理成功?}
        I -->|是| J[上传Parquet文件到publish文件夹]
        I -->|否| K[查看日志排查问题]
        
        J --> L[Power BI刷新数据]
        K --> M[修复问题后重新运行]
        M --> F
    ```

    ---

    #### 1. MES 数据更新（常规）

    **步骤：**

    1. **准备数据**：从 MES 系统导出最新数据（`Product Output -CZM -FY26.xlsx`）
    2. **上传到 SharePoint**：上传到 `30-MES` 文件夹
    3. **运行 ETL**：双击运行 `run_etl.bat` 或执行 `python etl_sa.py`
    4. **检查结果**：查看 `logs/etl_sa.log` 确认处理成功
    5. **上传 Parquet**：将生成的 `MES_处理后数据_latest.parquet` 上传到 `publish` 文件夹
    6. **刷新 Power BI**：在 Power BI 中刷新数据源

    ---

    #### 2. SFC 数据更新

    **步骤：**

    1. **准备数据**：从 SFC 系统导出最新数据（`LC-*.csv`）
    2. **上传到 SharePoint**：上传到 `70-SFC` 文件夹
    3. **运行 ETL**：执行 `python etl_sfc.py`
    4. **检查结果**：查看 `logs/etl_sfc.log` 确认处理成功
    5. **上传 Parquet**：将生成的 `SFC_处理后数据_latest.parquet` 上传到 `publish` 文件夹

    ---

    #### 3. 标准时间更新

    **步骤：**

    1. **准备数据**：获取最新的 `1303 Routing及机加工产品清单.xlsx`
    2. **上传到 SharePoint**：上传到 `30-MES` 文件夹
    3. **运行转换**：执行 `python convert_standard_time.py`
    4. **上传 Parquet**：将生成的 `SAP_Routing_yyyymmdd.parquet` 上传到 `publish` 文件夹

    ---

    **详细说明请参考：** [完整数据更新流程文档](guide/data-update.md)

=== "常见问题"

    #### SA 指标相关

    **Q1: SA 指标的计算是在 ETL 还是 Power BI？**

    **A:** 所有 SA 指标的计算都在 **ETL 阶段完成**（`etl_sa.py` 脚本）。

    - ✅ **已在 ETL 计算**：LT(d)、PT(d)、ST(d)、DueTime、Weekend(d)、CompletionStatus
    - ✅ **Power BI 直接读取**：无需二次计算，提高性能
    - ✅ **数据一致性**：所有报表使用相同的计算逻辑

    ---

    **Q2: 为什么 LT 和 PT 不扣除周末？**

    **A:** 这是业务设计决策：

    - **LT（Lead Time）**：反映实际经过的日历时间，包括周末
    - **PT（Process Time）**：反映实际加工时间，包括周末
    - **ST（Standard Time）**：理论加工时间，不考虑周末
    - **DueTime**：计划完工时间，**会跳过周末**（基于工作日日历，采用24小时连续工作制）

    ---

    **Q3: 0010 工序为什么要特殊处理？**

    **A:** 0010 工序是首道工序，具有特殊性：

    1. **Checkin_SFC 优先级最高**
       - 0010 工序：Checkin_SFC → EnterStepTime → TrackInTime
       - 其他工序：EnterStepTime

    2. **业务含义**
       - Checkin_SFC 是批次正式进入生产的时间点
       - 比 EnterStepTime 更准确

    ---

    #### 数据处理相关

    **Q4: ETL 处理失败怎么办？**

    **A:** 按以下步骤排查：

    1. **查看日志**：检查 `logs/etl_sa.log` 或 `logs/etl_sfc.log`
    2. **检查输入文件**：确认文件格式和路径正确
    3. **检查依赖**：确认已安装所有 Python 依赖
    4. **参考故障排查**：查看 [故障排查](guide/troubleshooting.md) 标签页

    ---

    **Q5: Power BI 刷新失败怎么办？**

    **A:** 常见原因和解决方法：

    1. **连接问题**：检查 SharePoint 连接是否正常
    2. **权限问题**：确认有访问 SharePoint 文件的权限
    3. **数据格式问题**：确认 Parquet 文件格式正确
    4. **参考故障排查**：查看 [故障排查](guide/troubleshooting.md) 标签页

    ---

    **详细问题列表请参考：** [完整常见问题文档](guide/faq.md)

=== "故障排查"

    #### 🚨 快速诊断

    根据症状快速定位问题：

    | 症状 | 可能问题 | 解决方法 |
    |------|----------|----------|
    | ETL 脚本运行失败 | 配置、依赖、数据问题 | 查看下方"ETL 运行问题" |
    | Power BI 刷新失败 | 连接、权限、数据问题 | 查看下方"Power BI 问题" |
    | 数据不正确 | 计算逻辑、数据质量 | 查看下方"数据质量问题" |
    | 性能慢 | 数据量、配置问题 | 查看下方"性能优化" |

    ---

    #### ETL 运行问题

    **问题 1：找不到 Python 或模块**

    **解决方法：**

    1. **安装 Python 3.8+**
    2. **安装依赖**：`pip install -r requirements.txt`
    3. **使用虚拟环境（推荐）**

    **问题 2：找不到输入文件**

    **解决方法：**

    1. **检查配置文件**：确认 `config.yaml` 中的路径正确
    2. **检查文件是否存在**：确认文件已上传到 SharePoint
    3. **检查文件格式**：确认文件格式正确（Excel/CSV）

    ---

    #### Power BI 问题

    **问题 1：无法连接到 SharePoint**

    **解决方法：**

    1. **检查网络连接**
    2. **检查权限**：确认有访问 SharePoint 的权限
    3. **检查 URL**：确认 SharePoint 路径正确

    **问题 2：刷新超时**

    **解决方法：**

    1. **使用增量刷新**：参考 [增量刷新方案](pq/incremental-refresh.md)
    2. **优化查询**：减少数据量
    3. **检查网络**：确认网络连接稳定

    ---

    #### 数据质量问题

    **问题 1：SA 达成率异常**

    **可能原因：**

    1. **标准时间缺失**：部分产品/工序缺少标准时间
    2. **时间逻辑错误**：TrackOutTime 早于 TrackInTime
    3. **工作日日历错误**：节假日配置不正确

    **解决方法：**

    1. **检查标准时间表**：确认所有产品/工序都有标准时间
    2. **检查时间字段**：确认时间逻辑正确
    3. **检查工作日日历**：确认节假日配置正确

    ---

    #### 性能优化

    **问题 1：ETL 处理太慢**

    **解决方法：**

    1. **使用 Parquet 格式**：性能提升 90-95%
    2. **使用增量更新**：只处理最近的数据
    3. **优化配置**：调整 chunk_size 等参数

    **问题 2：Power BI 刷新太慢**

    **解决方法：**

    1. **使用增量刷新**：参考 [增量刷新方案](pq/incremental-refresh.md)
    2. **优化查询**：减少不必要的数据
    3. **使用 Parquet 格式**：提升读取速度

    ---

    **详细排查指南请参考：** [完整故障排查文档](guide/troubleshooting.md)

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

