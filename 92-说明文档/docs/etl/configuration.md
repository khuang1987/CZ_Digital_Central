# ETL 配置说明

本页面说明 ETL 脚本的配置文件和参数。

---

## 配置文件位置

```
10-SA指标/13-SA数据清洗/config/
├── config.yaml       # MES 数据配置
└── config_sfc.yaml   # SFC 数据配置
```

---

## MES 配置（config.yaml）

```yaml
# 路径配置
paths:
  # 输入文件夹（SharePoint同步路径）
  input_folder: "C:/Users/xxx/SharePoint/30-MES"
  
  # 输出文件夹
  output_folder: "C:/Users/xxx/SharePoint/30-MES导出数据/publish"
  
  # SFC 数据文件
  sfc_data: "SFC_处理后数据_latest.parquet"
  
  # 标准时间文件（使用通配符自动查找最新）
  routing_data: "SAP_Routing_*.parquet"
  
  # 工作日日历
  calendar_file: "日历工作日表.csv"

# 处理参数
processing:
  # 默认 OEE（当标准时间表中为空或0时使用）
  default_oee: 0.77
  
  # 换批时间缓冲（小时）
  setup_time_buffer: 0.5
  
  # 每日工作时间（小时）- 24小时连续生产
  daily_work_hours: 24
  
  # 数据质量检查
  quality_check: true
  
  # 分块处理大小（条）
  chunk_size: 50000

# 增量更新配置
incremental:
  # 是否启用增量更新
  enabled: false
  
  # 增量天数（更新最近N天）
  days: 7
  
  # 或指定日期范围
  start_date: null  # 格式: "2025-01-01"
  end_date: null    # 格式: "2025-01-31"

# 日志配置
logging:
  # 日志级别：DEBUG, INFO, WARNING, ERROR
  level: "INFO"
  
  # 日志文件
  file: "logs/etl_sa.log"
  
  # 是否同时输出到控制台
  console: true

# 输出配置
output:
  # 保留历史备份
  keep_backup: true
  
  # 备份数量（保留最近N个备份）
  backup_count: 5
  
  # Parquet 压缩方式: snappy, gzip, none
  compression: "snappy"
```

---

## SFC 配置（config_sfc.yaml）

```yaml
paths:
  input_folder: "C:/Users/xxx/SharePoint/70-SFC"
  output_folder: "C:/Users/xxx/SharePoint/30-MES导出数据/publish"

processing:
  chunk_size: 50000

logging:
  level: "INFO"
  file: "logs/etl_sfc.log"
  console: true
```

---

## 常用配置修改

### 1. 修改文件路径

```yaml
paths:
  input_folder: "你的SharePoint同步路径/30-MES"
  output_folder: "你的SharePoint同步路径/30-MES导出数据/publish"
```

### 2. 启用增量更新

```yaml
incremental:
  enabled: true
  days: 7  # 只处理最近7天
```

### 3. 修改默认 OEE

```yaml
processing:
  default_oee: 0.80  # 改为 80%
```

### 4. 修改日志级别

```yaml
logging:
  level: "DEBUG"  # 显示更详细的调试信息
```

---

## 配置验证

运行前可验证配置：

```python
import yaml

# 读取配置
with open('config/config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

# 检查路径是否存在
import os
print(f"输入路径存在: {os.path.exists(config['paths']['input_folder'])}")
print(f"输出路径存在: {os.path.exists(config['paths']['output_folder'])}")
```

---

## 相关链接

- [ETL 流程概述](index.md)
- [数据更新流程](../guide/data-update.md)
- [故障排查](../guide/troubleshooting.md)

