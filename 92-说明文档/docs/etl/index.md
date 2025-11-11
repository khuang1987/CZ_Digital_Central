# 数据处理概述

本章节介绍数据平台的 ETL（Extract, Transform, Load）处理流程。

---

## ETL 流程架构

```mermaid
graph TB
    A[SharePoint<br/>原始数据] -->|提取| B[ETL脚本]
    B -->|转换| C[数据清洗<br/>计算<br/>验证]
    C -->|加载| D[Parquet文件]
    D -->|刷新| E[Power BI]
    
    F[SFC数据] -.->|合并| C
    G[标准时间表] -.->|匹配| C
    H[工作日日历] -.->|DueTime计算| C
    
    style A fill:#e1f5ff
    style D fill:#fff4e6
    style E fill:#e8f5e9
    style B fill:#f3e5f5
    style C fill:#f3e5f5
```

---

## 核心 ETL 脚本

### 1. SA 数据清洗（主流程）

**脚本：** `etl_sa.py`

**功能：**
- 读取 MES 原始数据
- 合并 SFC 的 Checkin_SFC
- 匹配标准时间参数
- 计算所有 SA 指标字段
- 生成 Parquet 文件

**输入：**
- MES Excel 文件
- SFC Parquet 文件（已处理）
- 标准时间 Parquet 文件
- 工作日日历 CSV

**输出：**
- `MES_处理后数据_latest.parquet`

📖 [详细说明](etl-sa.md)

---

### 2. SFC 数据清洗

**脚本：** `etl_sfc.py`

**功能：**
- 读取 SFC 原始数据
- 数据清洗和标准化
- 去重和验证
- 生成 Parquet 文件

**输入：**
- SFC CSV/Excel 文件（LC-*.csv）

**输出：**
- `SFC_处理后数据_latest.parquet`

📖 [详细说明](etl-sfc.md)

---

### 3. 标准时间转换

**脚本：** `convert_standard_time.py`

**功能：**
- 合并 Routing 表和机加工清单
- 计算单件时间（秒）
- 生成标准时间 Parquet

**输入：**
- `1303 Routing及机加工产品清单.xlsx`
  - Sheet 1: 1303 Routing
  - Sheet 2: 1303机加工清单

**输出：**
- `SAP_Routing_yyyymmdd.parquet`

📖 [详细说明](standard-time.md)

---

### 4. 工作日日历生成

**脚本：** `generate_calendar.py`

**功能：**
- 生成指定年份的日历
- 标记工作日和节假日
- 支持自定义节假日

**输入：**
- 年份参数

**输出：**
- `日历工作日表.csv`

---

## ETL 运行方式

### 方式 1：批处理文件（推荐）

```batch
# 单独运行
run_etl.bat           # 仅MES
convert_standard_time.bat  # 标准时间

# 批量运行
run_all_etl.bat       # SFC + MES
```

### 方式 2：Python 命令

```bash
# 标准时间
python convert_standard_time.py

# SFC数据
python etl_sfc.py

# MES数据
python etl_sa.py
```

### 方式 3：配置参数运行

```bash
# 增量更新（最近7天）
python etl_sa.py --incremental --days 7

# 指定日期范围
python etl_sa.py --start-date 2025-01-01 --end-date 2025-01-31

# 使用自定义配置
python etl_sa.py --config custom_config.yaml
```

---

## 数据流向

### 完整数据流

```mermaid
graph LR
    A1[MES Excel] --> B1[etl_sa.py]
    A2[SFC CSV] --> B2[etl_sfc.py]
    A3[Routing Excel] --> B3[convert_standard_time.py]
    
    B2 --> C[SFC Parquet]
    B3 --> D[Routing Parquet]
    
    C --> B1
    D --> B1
    E[日历表] --> B1
    
    B1 --> F[MES Parquet]
    F --> G[Power BI]
    
    style A1 fill:#e3f2fd
    style A2 fill:#e3f2fd
    style A3 fill:#e3f2fd
    style C fill:#fff3e0
    style D fill:#fff3e0
    style F fill:#fff3e0
    style G fill:#e8f5e9
```

---

## 主要处理步骤

### MES 数据处理（etl_sa.py）

1. ⬇️ **加载配置** - 读取 `config.yaml`
2. 📖 **读取原始数据** - MES Excel
3. 🔄 **合并 SFC** - 按 BatchNumber + Operation 匹配
4. 🔗 **匹配标准时间** - 按 CFN + Operation 匹配
5. 📊 **计算 LT/PT** - Lead Time 和 Process Time
6. 📐 **计算 ST** - 标准时间
7. 📅 **计算 DueTime** - 基于工作日日历
8. ✅ **判断 SA 状态** - OnTime/Overdue
9. 🔍 **数据质量检查** - 验证必填字段、数据类型
10. 💾 **保存 Parquet** - 输出处理后数据

---

## 配置管理

### 配置文件位置

```
10-SA指标/13-SA数据清洗/config/
├── config.yaml       # MES配置
└── config_sfc.yaml   # SFC配置
```

### 主要配置项

```yaml
# 数据路径
paths:
  input_folder: "SharePoint路径/30-MES"
  output_folder: "SharePoint路径/30-MES导出数据/publish"
  sfc_data: "SFC_处理后数据_latest.parquet"
  routing_data: "SAP_Routing_*.parquet"

# 处理参数
processing:
  default_oee: 0.77
  setup_time_buffer: 0.5
  daily_work_hours: 24  # 24小时连续生产

# 增量更新
incremental:
  enabled: false
  days: 7
```

📖 [配置详细说明](configuration.md)

---

## 日志和监控

### 日志文件

```
logs/
├── etl_sa.log        # MES处理日志
├── etl_sfc.log       # SFC处理日志
└── manifest.csv      # 处理清单
```

### 日志内容

```
2025-01-10 10:30:15 - INFO - ETL处理开始
2025-01-10 10:30:16 - INFO - 读取MES数据: 123,456 条
2025-01-10 10:30:45 - INFO - 合并SFC数据: 匹配 85,234 条
2025-01-10 10:32:10 - WARNING - 缺失标准时间: 234 条记录
2025-01-10 10:34:07 - INFO - 保存Parquet文件成功
2025-01-10 10:34:07 - INFO - ETL处理完成
```

---

## 数据质量保证

### 自动检查项

- ✅ 必填字段完整性
- ✅ 数据类型正确性
- ✅ 时间逻辑合理性
- ✅ 数量非负性
- ✅ OEE 范围（0-1）
- ✅ 日期有效性

### 质量报告

ETL 完成后会生成质量报告：

```
数据质量报告
====================
总记录数: 123,456
有效记录: 123,222 (99.81%)
异常记录: 234 (0.19%)

异常类型统计:
- 缺失Checkin_SFC: 15,234 (12.34%)
- 缺失标准时间: 234 (0.19%)
- 时间逻辑异常: 0 (0.00%)
```

---

## 性能优化

### 处理速度

| 数据量 | 预估时间 | 建议 |
|--------|----------|------|
| < 1万条 | 1-2 分钟 | 全量更新 |
| 1-10万条 | 3-5 分钟 | 全量/增量均可 |
| 10-50万条 | 10-20 分钟 | 建议增量更新 |
| > 50万条 | 30+ 分钟 | 必须增量更新 |

### 优化建议

1. **使用增量更新** - 仅处理最近数据
2. **并行处理** - 启用多核处理
3. **优化配置** - 调整 chunk_size
4. **清理历史数据** - 定期归档

---

## 快速开始

### 首次运行

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置路径
# 编辑 config/config.yaml

# 3. 生成日历
python generate_calendar.py --year 2025 --year 2026

# 4. 处理标准时间
python convert_standard_time.py

# 5. 处理SFC
python etl_sfc.py

# 6. 处理MES
python etl_sa.py
```

### 日常更新

```bash
# 一键更新（推荐）
run_all_etl.bat

# 或手动更新
python etl_sa.py
```

---

## 相关资源

- [ETL 处理流程详解](etl-process.md)
- [SA 数据清洗](etl-sa.md)
- [SFC 数据清洗](etl-sfc.md)
- [配置说明](configuration.md)
- [数据更新流程](../guide/data-update.md)
- [故障排查](../guide/troubleshooting.md)

