# 02 核心指标字典 (KPI Dictionary)

本文档定义了平台内所有核心 KPI 的业务定义、数据来源及计算逻辑。

## 1. 交付类指标 (Delivery)

### 1.1 WIP (在制品)
- **定义**: 当前正在产线各工序停留的批次总数或物料总数。
- **计算逻辑**:
    - 数据源: `raw_mes_wip_cmes` (C-MES) + `raw_sfc_wip_czm` (CZM SFC)。
    - 计算公式: `SUM(qty)` grouped by Area/Operation。
- **口径**: 以每日 07:00 的快照 (Snapshot) 为准。

### 1.2 Overdue WIP (逾期在制品)
- **定义**: 实际停留时间超过标准 LT 的批次。
- **判定规则**: `实际停留天数 > (标准 LT + 8小时/24)`.

## 2. 质量类指标 (Quality)

### 2.1 NC Count (不合格品数)
- **定义**: 选定时间段内新发生的非符合性项 (Non-Conformance) 数量。
- **数据源**: `mddap_v2.dbo.raw_mes_nc_data`.

## 3. 安全类指标 (Safety/EHS)

### 3.1 YTD 事故天数
- **定义**: 自本年度 1 月 1 日起，发生的安全事故/隐患总数。
- **计算逻辑**: 基于 `raw_production_ehs` 中的 `discover_date` 进行聚合。

## 4. 指标计算规范 (Calculation Standards)
- **日期处理**: 统一采用 ISO 8601 格式 (`YYYY-MM-DD`)。
- **空值处理**: 所有数量类指标空值默认为 `0`，日期类空值需保留为 `NULL` 以备逾期分析。
