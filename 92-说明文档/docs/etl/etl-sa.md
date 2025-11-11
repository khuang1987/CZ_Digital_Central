# SA 数据清洗详解

本页面详细说明 `etl_sa.py` 脚本的功能和实现。

---

## 脚本概述

**文件：** `10-SA指标/13-SA数据清洗/etl_sa.py`

**主要功能：**
- 处理 MES 批次报工记录
- 合并 SFC Check In 数据
- 匹配标准时间参数
- 计算所有 SA 指标字段
- 生成 Parquet 文件供 Power BI 使用

---

## 详细处理流程

详见：[ETL 处理流程详解](etl-process.md)

---

## 运行方式

### 使用批处理文件

```batch
run_etl.bat
```

### 使用命令行

```bash
python etl_sa.py
```

### 使用参数

```bash
# 增量更新
python etl_sa.py --incremental --days 7

# 指定配置文件
python etl_sa.py --config custom_config.yaml
```

---

## 相关资源

- [ETL 流程概述](index.md)
- [ETL 处理流程详解](etl-process.md)
- [配置说明](configuration.md)
- [SA 指标计算方法](../sa/calculation-definition.md)

