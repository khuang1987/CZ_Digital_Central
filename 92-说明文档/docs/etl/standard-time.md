# 标准时间处理

本页面说明标准时间表的生成和转换过程。

---

## 脚本概述

**文件：** `convert_standard_time.py`

**功能：** 合并 SAP Routing 表和机加工清单，生成标准时间 Parquet 文件

---

## 输入文件

**Excel 文件：** `1303 Routing及机加工产品清单.xlsx`

**Sheet 1: 1303 Routing**
- CFN（产品号）
- Operation（工序号）
- Machine（机器总时间，秒）
- Labor（人工总时间，秒）
- Quantity（数量）

**Sheet 2: 1303机加工清单**
- CFN
- Operation
- OEE（设备综合效率）
- Setup Time (h)（调试时间，小时）

---

## 处理逻辑

```python
# 1. 读取两个 Sheet
routing = pd.read_excel(file, sheet_name='1303 Routing')
machining = pd.read_excel(file, sheet_name='1303机加工清单')

# 2. 计算单件时间
routing['EH_machine(s)'] = routing['Machine'] / routing['Quantity']
routing['EH_labor(s)'] = routing['Labor'] / routing['Quantity']

# 3. 合并
result = routing.merge(
    machining[['CFN', 'Operation', 'OEE', 'Setup Time (h)']],
    on=['CFN', 'Operation'],
    how='left'
)

# 4. 保存 Parquet
result.to_parquet('SAP_Routing_20250110.parquet')
```

---

## 运行方式

```bash
python convert_standard_time.py
```

或双击：`convert_standard_time.bat`

---

## 相关资源

- [ETL 流程概述](index.md)
- [数据源说明 - 产品标准时间表章节](../sa/data-sources.md#23-产品标准时间表)

