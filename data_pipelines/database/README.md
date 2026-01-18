# MDDAP 数据库

## 概述

本项目当前使用 **SQL Server** 作为数据库（本机实例：`localhost\SQLEXPRESS`，数据库名：`mddap_v2`）。

## 文件结构

```
database/
├── schema/               # 表结构定义（参考/对齐用）
│   ├── init_schema.sql   # 历史：旧版 schema（可忽略）
│   └── init_schema_v2.sql # V2 schema（与 SQL Server 表结构对齐）
└── README.md
```

## 数据表

| 表名 | 说明 | 数据来源 |
|------|------|----------|
| `dbo.raw_mes` | MES 批次产出原始数据 | MES Excel 导出 |
| `dbo.raw_sfc` | SFC 批次报工原始数据 | SFC Excel 导出 |
| `dbo.raw_sfc_inspection` | SFC 产品检验原始数据 | SFC Excel 导出 |
| `dbo.raw_sap_routing` | SAP 工艺路线原始数据 | SAP 导出 |
| `etl_run_log` | ETL 运行日志 | 系统自动记录 |

计算视图：

| 视图名 | 说明 |
|--------|------|
| `dbo.v_mes_metrics` | MES 指标计算视图（LT/PT/ST 等） |

## 初始化/更新数据库对象

SQL Server 的建表/表结构维护目前通过项目脚本完成；核心对象包括 `dbo.raw_*` 表与 `dbo.v_mes_metrics` 视图。

刷新/创建 MES 计算视图：

```powershell
python scripts/_execute_create_view_v2.py
```

## 查看数据

### 方式1：SSMS

在 SQL Server Management Studio 中连接：

- Server: `localhost\SQLEXPRESS`
- Database: `mddap_v2`

示例查询：

```sql
SELECT TOP 100 *
FROM dbo.raw_mes
ORDER BY TrackOutTime DESC;
```

### 方式2：Python (pyodbc)

```python
import pyodbc
import pandas as pd

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost\SQLEXPRESS;"
    "DATABASE=mddap_v2;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
)

with pyodbc.connect(conn_str) as conn:
    df = pd.read_sql("SELECT TOP 100 * FROM dbo.v_mes_metrics ORDER BY TrackOutTime DESC", conn)
print(df.head())
```
