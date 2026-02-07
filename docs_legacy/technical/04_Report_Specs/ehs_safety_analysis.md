# EHS Safety Analysis (EHS 安全看板) 报表逻辑定义

本文档详细定义了 Production Dashboard 中 **EHS Safety（环境、健康与安全）** 分析页面的数据逻辑、界面元素及交互行为。

## 1. 页面元素定义 (UI Elements)

### 1.1 安全绿十字 (Safety Green Cross)
页面核心交互组件，用于可视化展示当月每日安全状态。
-   **逻辑**: 
    -   默认显示绿色 (Safe)。
    -   用户可手动标记为红色 (Incident)。
    -   **校验规则**: 必须完成前一天的状态标记后，才能标记当天（"前一天需要完成标记"）。
-   **交互**: 
    -   点击日期格子弹出状态编辑框。
    -   支持添加备注 (Incident Details)。
-   **数据源**: `safety_green_cross_log` 表 (SQL Server)。

### 1.2 KPI 核心指标卡 (KPI Tiles)
采用与 Labor Dashboard 一致的视觉风格，展示关键安全指标。

-   **Card 1: 安全天数 (Safe Days)**
    -   **逻辑**: 从最后一次“事故 (Incident)”日期起算，截止当前的连续天数。
    -   **计算公式**: `DATEDIFF(day, MAX(IncidentDate), GETDATE())`。
    -   **视觉**: 绿色主色调，强调正面指标。

-   **Card 2: 年累计事故 (YTD Incidents)**
    -   **逻辑**: 本年度记录在案的事故总数。
    -   **数据源**: Microsoft Planner 任务。
    -   **过滤条件**: 任务标签包含 "First Aid", "Recordable Incident", "Safety Incident"。
    -   **视觉**: 红色主色调，强调警示指标。

-   **Card 3: 待处理安全隐患 (Open Internal Hazards)**
    -   **逻辑**: 当前未关闭的安全隐患任务数。
    -   **数据源**: Microsoft Planner 任务。
    -   **过滤条件**: 
        -   Bucket (任务桶) = "Safe" 或 "Safety"。
        -   Status != "Completed"。
    -   **视觉**: 橙色主色调，强调待办事项。

### 1.3 统计图表 (Charts)

-   **各区域隐患分布 (Hazards by Area)**
    -   **类型**: 条形图 (Bar Chart)。
    -   **逻辑**: 按 `TeamName` (区域) 分组统计未关闭的安全任务数量。
    -   **排序**: 降序排列，快速识别高风险区域。

-   **事故清单 (Incident List)**
    -   **类型**: 列表视图。
    -   **逻辑**: 展示 YTD 所有事故任务的简要信息（日期、描述、区域）。

---

## 2. 页面架构与交互 (Layout & Interaction)

### 2.1 三屏布局 (Three-Tier Layout)
-   **第一屏 (Overview)**: 
    -   **左侧**: 安全绿十字 (占 1/3 宽度)，作为操作核心。
    -   **右侧**: KPI 指标卡 + 隐患分布图 + 事故清单。
    -   **目标**: 一眼看清当月状态及核心风险点。

-   **第二屏 (Analysis) - (规划中)**: 
    -   柏拉图分析 (Pareto): 按事故类型/伤害类型统计。
    -   趋势分析: 每月事故发生的趋势图。

-   **第三屏 (Details) - (规划中)**: 
    -   详细任务追踪表。

### 2.2 交互逻辑
-   **月份切换**: 用户可在 Header 区域查看当前月份。目前版本默认加载当前月，后续支持历史月份查询。
-   **实时反馈**: 
    -   修改绿十字状态后，Safe Days 指标应立即重新计算并更新。
    -   Planner 数据源为定时 ETL 同步，非实时，但页面加载时获取最新库中数据。

---

## 3. 数据架构 (Data Architecture)

### 3.1 混合数据源
本页面采用 **SQL Server + Planner** 混合数据模式。

1.  **手动录入数据 (Manual Entry)**
    -   **表名**: `dbo.safety_green_cross_log`
    -   **表格用途**: 存储每日安全状态（绿/红）。
    -   **主键**: `date` (DATE)。

2.  **自动化数据 (Automated Data)**
    -   **表名**: `dbo.planner_tasks` (来源于 SAP ETL 流程)。
    -   **用途**: 统计事故数和隐患数。
    -   **关键字段映射**:
        -   `TeamName` -> 区域 (Area)
        -   `Labels` -> 事故类型 (Incident Type) (e.g., "急救事件", "险兆事故")
        -   `BucketName` -> 任务分类 (Task Category) (e.g., "安全")

### 3.2 API 接口
-   **路径**: `/api/production/ehs`
-   **方法**:
    -   `GET`: 获取当月绿十字数据、KPI 统计、图表数据。
    -   `POST`: 更新某日期的安全状态。

---

## 5. 数据字典与架构 (Data Schema & Dictionary)

### 5.1 Safety Green Cross Log (`safety_green_cross_log`)
存储安全绿十字的每日状态。
| Column Name | Type | Key | Description |
| :--- | :--- | :--- | :--- |
| `date` | `DATE` | PK | 日期 (YYYY-MM-DD) |
| `status` | `NVARCHAR(50)` | | 状态枚举: 'Safe', 'Incident', 'Holiday' |
| `incident_details` | `NVARCHAR(MAX)` | | 事故/异常详情备注 |
| `updated_at` | `DATETIME` | | 最后更新时间 |
| `marked_by` | `NVARCHAR(255)` | | 操作人 (Reserve for Auth) |

### 5.2 Planner Tasks (`planner_tasks`)
来源于 Microsoft Planner 的任务同步表。
| Column Name | Type | Description |
| :--- | :--- | :--- |
| `TaskId` | `NVARCHAR(255)` | Planner 任务唯一 ID (PK) |
| `TaskName` | `NVARCHAR(MAX)` | 任务标题 (通常作为隐患描述) |
| `BucketName` | `NVARCHAR(MAX)` | 所在的桶 (e.g., "安全", "待办事项") |
| `TeamName` | `NVARCHAR(MAX)` | 所属团队/区域 (e.g., "加工中心", "装配") |
| `Labels` | `NVARCHAR(MAX)` | 标签组合 (e.g., "急救事件; 手部伤害") |
| `CreatedDate` | `DATE` | 创建日期 (用于 YTD 统计) |
| `Status` | `NVARCHAR(50)` | 任务状态 (Completed, InProgress, NotStarted) |

### 5.3 业务逻辑映射 (Business Logic Mapping)
| Metric | Logic / Filter |
| :--- | :--- |
| **Open Hazards (隐患)** | `BucketName = '安全'` AND `Status != 'Completed'` |
| **YTD Incidents (事故)** | `Labels` LIKE '%急救事件%' OR `Labels` LIKE '%可记录事故%' OR `Labels` LIKE '%工伤%' |
| **Near Miss (险兆)** | `Labels` LIKE '%险兆事故%' OR `Labels` LIKE '%险兆事件%' |


---

## 4. 最佳实践与建议 (Best Practices)

1.  **视觉管理**:
    -   严格遵循 **红/绿** 语义：绿色代表安全，红色代表事故。避免使用含糊不清的颜色。
2.  **操作闭环**:
    -   发现隐患 -> Planner 创建任务 (入桶 "Safe") -> Dashboard 自动统计 -> 线下整改 -> Planner 关闭任务 -> Dashboard 计数下降。
3.  **数据排查**:
    -   若 YTD 数据不准，请检查 Planner 中任务的 Label 是否正确打标 ("First Aid", "Recordable Incident")。
