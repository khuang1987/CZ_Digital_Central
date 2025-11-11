# Power BI查询代码 - Parquet版本

## 概述

本文件夹包含从Parquet文件读取数据的Power BI查询代码，替代了原来依赖Excel/SharePoint数据源的查询。

所有数据清洗、计算和合并逻辑已在Python ETL脚本中完成，Power Query只需直接从Parquet文件读取处理后的数据。

---

## 文件说明

### 1. `e2_批次报工记录_MES_后处理.pq`
**功能**：读取MES批次报工记录数据

**数据源**：`publish/MES_处理后数据_latest.parquet`

**包含字段**：
- 基础标识：BatchNumber, CFN, Operation, Group, machine等
- 时间字段：TrackInTime, TrackOutTime, EnterStepTime, Checkin_SFC, DueTime
- 计算字段：LT(d), PT(d), ST(d), Weekend(d), CompletionStatus, Tolerance(h)
- 标准时间：OEE, Setup Time (h), Effective Time (h), Machine(#)

**特点**：
- 所有计算已在ETL中完成
- 直接读取Parquet文件，无需额外处理
- 只需进行数据类型转换

---

### 2. `e4_批次报工记录_SFC.pq`
**功能**：读取SFC批次报工记录数据

**数据源**：`publish/SFC_处理后数据_latest.parquet`

**包含字段**：
- 基础标识：BatchNumber, CFN, Operation, Group, machine
- 数量字段：TrackOutQuantity, ScrapQuantity
- 时间字段：Checkin_SFC, TrackOutTime, DueTime
- 计算字段：LT(d), PT(d), ST(d), Weekend(d), CompletionStatus, Tolerance(h)
- 标准时间：OEE, Setup Time (h), Effective Time (h), Machine(#)

**特点**：
- 独立的数据源，不依赖MES数据
- 所有计算已在ETL中完成

---

### 3. `e3_产品标准时间.pq`
**功能**：读取产品标准时间表

**数据源**：`publish/SAP_Routing_*.parquet`（自动查找最新文件）

**包含字段**：
- 匹配键：CFN, Operation, Group
- 标准时间：Machine, Labor, Effective Time (h)
- 其他：OEE, Setup Time (h), Quantity, 备注, 分类

**特点**：
- 自动查找最新的标准时间表文件
- 文件名格式：`SAP_Routing_yyyymmdd.parquet`

---

## 使用说明

### 1. 路径配置

**重要**：所有查询文件已配置为从SharePoint读取Parquet文件。

**SharePoint路径配置**：
```powerquery
SharePointSite = "https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction"
SharePointFolder = "Shared Documents/General/POWER BI 数据源 V2/30-MES导出数据"
```

**文件位置**：
- MES数据：`30-MES导出数据/MES_处理后数据_latest.parquet`
- SFC数据：`30-MES导出数据/SFC_处理后数据_latest.parquet`
- 标准时间表：`30-MES导出数据/SAP_Routing_*.parquet`（自动查找最新文件）

**注意**：
- 确保Parquet文件已上传到SharePoint的30-MES文件夹
- 如果SharePoint路径不同，请修改查询中的`SharePointFolder`变量

### 2. 替代原有查询

**原查询** → **新查询**
- `e2_批次报工记录_MES_后处理.pq` → `e2_批次报工记录_MES_后处理.pq`（新版本）
- `e4_批次报工记录_SFC.pq` → `e4_批次报工记录_SFC.pq`（新版本）
- `e3_产品标准时间_exl.pq` → `e3_产品标准时间.pq`（新版本）

### 3. 数据流程

```
Excel/SharePoint数据源
    ↓
Python ETL脚本处理
    ↓
Parquet文件（publish/）
    ↓
Power BI查询（本文件夹）
    ↓
Power BI报表
```

---

## 与原查询的差异

### 原查询（`14-PQ查询代码/`）
- 从Excel/SharePoint读取原始数据
- 在Power Query中进行数据清洗
- 在Power Query中进行字段计算
- 在Power Query中合并多个数据源

### 新查询（`12查询代码_parquet/`）
- 从Parquet文件读取已处理数据
- 数据清洗已在ETL中完成
- 所有计算已在ETL中完成
- 只需进行数据类型转换和列重排

---

## 优势

1. **性能提升**：Parquet格式读取速度快于Excel
2. **数据一致性**：所有计算逻辑集中在ETL中，避免重复计算
3. **维护简化**：业务逻辑修改只需更新ETL脚本
4. **增量刷新**：ETL支持增量处理，减少数据量

---

## 注意事项

1. **文件路径**：确保Power BI可以访问Parquet文件路径
2. **数据更新**：运行ETL脚本更新Parquet文件后，Power BI刷新即可获取最新数据
3. **字段名称**：确保ETL输出的字段名称与查询中使用的字段名称一致
4. **数据类型**：Parquet文件中的数据类型应该与查询中的类型转换匹配

---

## 数据更新流程

1. 运行ETL脚本：
   ```batch
   cd "10-SA指标\13-SA数据清洗"
   run_all_etl.bat
   ```

2. 在Power BI中刷新数据：
   - 右键点击查询 → 刷新
   - 或使用"全部刷新"功能

---

*文档更新时间：2024-11-06*

