# Power Query 概述

本章节介绍 Power BI 中使用的 Power Query 查询代码。

---

## Power Query 文件位置

```
10-SA指标/14-PQ查询代码/
├── e2_批次报工记录_MES.pq
├── e4_批次报工记录_SFC.pq
├── e3_产品标准时间_exl.pq
└── 参数_最后刷新时间.pq
```

---

## 主要查询

### 1. MES 批次报工记录

**文件：** `e2_批次报工记录_MES.pq`

**功能：** 从 SharePoint 读取 MES 处理后的 Parquet 文件

**使用方式：**
```m
let
    Source = Parquet.Document(
        Web.Contents("SharePoint路径/MES_处理后数据_latest.parquet")
    )
in
    Source
```

📖 [详细说明](mes-records.md)

---

### 2. SFC 批次报工记录

**文件：** `e4_批次报工记录_SFC.pq`

**功能：** 从 SharePoint 读取 SFC 处理后的 Parquet 文件

📖 [详细说明](sfc-records.md)

---

### 3. 产品标准时间

**文件：** `e3_产品标准时间_exl.pq`

**功能：** 读取标准时间 Parquet 文件

📖 [详细说明](standard-time.md)

---

## 增量刷新

为了优化 Power BI 性能，建议使用增量刷新：

📖 [增量刷新方案详解](incremental-refresh.md)

---

## Power Query 最佳实践

### 1. 使用 Parquet 格式

```m
// 推荐：快速
Source = Parquet.Document(File.Contents("file.parquet"))

// 不推荐：慢
Source = Excel.Workbook(File.Contents("file.xlsx"))
```

### 2. 启用查询折叠

- 使用原生数据源操作
- 避免过早添加自定义列
- 使用筛选和排序

### 3. 减少数据量

```m
// 只选择需要的列
= Table.SelectColumns(Source, {"BatchNumber", "Operation", "SA状态"})

// 筛选数据
= Table.SelectRows(Source, each [TrackOutDate] >= #date(2025, 1, 1))
```

---

## 相关资源

- [数据源说明](../kpi/sa.md#-数据源说明)
- [增量刷新方案](incremental-refresh.md)
- [数据更新流程](../guide/data-update.md)

