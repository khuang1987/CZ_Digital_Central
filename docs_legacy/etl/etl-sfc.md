# SFC 数据清洗详解

本页面详细说明 `etl_sfc.py` 脚本的功能和实现。

---

## 脚本概述

**文件：** `10-SA指标/13-SA数据清洗/etl_sfc.py`

**主要功能：**
- 处理 SFC 批次报工记录
- 提取 Check In 时间
- 数据清洗和标准化
- 生成 Parquet 文件供 MES ETL 合并使用

---

## 输入输出

**输入：**
- SharePoint `70-SFC` 文件夹
- 文件格式：`LC-yyyymmddhhmmss.csv` 或 Excel

**输出：**
- `SFC_处理后数据_latest.parquet`
- 包含字段：BatchNumber, Operation, Checkin_SFC 等

---

## 运行方式

```bash
python etl_sfc.py
```

---

## 相关资源

- [ETL 流程概述](index.md)
- [数据更新流程](../guide/data-update.md)

