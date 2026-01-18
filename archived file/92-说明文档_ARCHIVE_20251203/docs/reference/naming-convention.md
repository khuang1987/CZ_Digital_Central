# 文件命名规范

本文档说明项目中的文件命名规范。

---

## 基本原则

1. **使用有意义的名称**
2. **保持一致性**
3. **避免特殊字符**
4. **包含版本或日期信息**

---

## 文件命名规范

### 原始数据文件

```
Product Output -CZM -FY26.xlsx       # MES数据
LC-20241118161001.csv                # SFC数据
1303 Routing及机加工产品清单.xlsx    # 标准时间
```

### 处理后数据文件

```
MES_处理后数据_latest.parquet        # 最新版本
MES_处理后数据_20250110.parquet      # 日期备份
SFC_处理后数据_latest.parquet
SAP_Routing_20250105.parquet
```

### 脚本文件

```
etl_sa.py              # ETL脚本
etl_sfc.py
convert_standard_time.py
generate_calendar.py
run_etl.bat            # 批处理文件
```

### 配置文件

```
config.yaml
config_sfc.yaml
```

### 日志文件

```
etl_sa.log
etl_sfc.log
manifest.csv
```

---

## Power Query 文件命名

```
e1_批次报工记录_SFC.pq
e2_批次报工记录_MES.pq
e3_产品标准时间.pq
e4_批次报工记录_SFC.pq
参数_最后刷新时间.pq
```

**规则：**
- `e数字_` 前缀：表示实体/查询
- 中文描述：清晰说明用途
- `.pq` 扩展名：Power Query 文件

---

## 文档命名

```
01_ProjectDefinition_项目定义.md
02_KpiSpecification_KPI详细说明.md
SA指标计算方法与定义.md
```

**规则：**
- 编号前缀（可选）：表示顺序
- 英文标题_中文标题：双语命名
- `.md` 扩展名：Markdown 文档

---

## 日期格式

统一使用 ISO 8601 格式：

- 文件中：`yyyymmdd` (如 `20250110`)
- 数据中：`yyyy-mm-dd` (如 `2025-01-10`)
- 时间戳：`yyyymmddhhmmss` (如 `20250110143022`)

---

详细内容请参考原文档：`99_Reference_参考信息/01_文件命名规范.md`

