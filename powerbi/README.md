# Power BI 报表文件夹

## 目录结构

```
powerbi/
├── reports/                        # 报表文件夹
│   └── 01_labor_hours/            # 每个报表一个文件夹
│       ├── 01_labor_hours.pbix    # Power BI 报表文件
│       ├── query_*.sql            # SQL 查询文件
│       └── README.md              # 报表说明
├── datasets/                       # 独立数据集文件
├── templates/                      # 报表模板
└── README.md                       # 本说明文件
```

## 文件夹命名规范

### 格式
`XX_报表名称/`

- **XX**: 两位数序号 (01, 02, 03...)
- 使用下划线 `_` 分隔单词
- 全部小写英文
- 每个报表对应一个文件夹，包含 .pbix 和相关查询文件

### 报表列表
| 序号 | 文件夹 | 说明 |
|------|--------|------|
| 01 | `01_labor_hours/` | 人工工时分析 |
| 02 | `02_production_overview/` | 生产概览 |
| 03 | `03_quality_nc_analysis/` | 质量不合格分析 |
| 04 | `04_wip_tracking/` | WIP 跟踪 |
| 05 | `05_repair_analysis/` | 维修分析 |

## 数据源

所有报表连接到本项目的 SQLite 数据库：
```
data_pipelines/database/mddap_v2.db
```

## 可用数据表

| 表名 | 说明 |
|------|------|
| `raw_sap_labor_hours` | SAP 人工工时数据 |
| `raw_sfc_wip_czm` | SFC WIP 批次流转 (CZM) |
| `raw_sfc_repair` | SFC 维修记录 |
| `raw_sfc_nc` | SFC 不合格异常 |
| `dim_calendar` | 财历日历表 |
| `dim_operation_mapping` | 工序名称映射 |
