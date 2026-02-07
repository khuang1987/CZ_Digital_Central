# 开发日志

记录项目重要功能开发和修复。

---

## 2025-12-13

### V2 数据清洗代码字段名统一

**变更内容**：
- MES ETL：`ProductName` → `ProductNumber`，`Operator` → `TrackOutOperator`，`ERPCode` → `Plant`
- SFC ETL：`CheckinTime` → `TrackInTime`，`CheckinUser` → `TrackInOperator`，`TrackOutUser` → `TrackOutOperator`
- SAP ETL：`MaterialCode` → `ProductNumber`，`Machine` → `EH_machine`，`Labor` → `EH_labor`

**影响范围**：
- `data_pipelines/sources/mes/etl/etl_mes_batch_output_raw.py`
- `data_pipelines/sources/sfc/etl/etl_sfc_batch_output_raw.py`
- `data_pipelines/sources/sap/etl/etl_sap_routing_raw.py`
- `data_pipelines/database/schema/init_schema_v2.sql`

**测试结果**：
| ETL | 读取 | 插入 | 跳过 |
|-----|------|------|------|
| MES | 6000 行 | 5998 条 | 2 条 |
| SFC | 3000 行 | 1536 条 | 1464 条 |
| SAP | 356333 行 | 250674 条 | 105659 条 |

### 文档系统整合

**变更内容**：
- 将 `docs/` 目录内容整合到 `documentation/` 统一文档系统
- 新增开发者文档章节（架构设计、开发规范、变更记录）
- 新增数据字典章节（字段映射、计算逻辑、跨表对照）

---

## 2025-12-12

### V2 架构 ETL 脚本创建

**变更内容**：
- 创建 MES/SFC/SAP 三个数据源的 V2 ETL 脚本
- 采用 ODS原始层 + DWD计算层 分层架构
- 计算逻辑通过数据库视图 `v_mes_metrics` 实现

**新增文件**：
- `data_pipelines/sources/mes/etl/etl_mes_batch_output_raw.py`
- `data_pipelines/sources/sfc/etl/etl_sfc_batch_output_raw.py`
- `data_pipelines/sources/sap/etl/etl_sap_routing_raw.py`
- `data_pipelines/database/schema/init_schema_v2.sql`

### 计算视图 v_mes_metrics 实现

**计算字段**：
- `IsSetup`：同机台上下批次CFN比较
- `PreviousBatchEndTime`：窗口函数计算
- `LT(d)`：实际加工时间（天）
- `PT(d)`：实际加工时间（天），考虑停产期
- `ST(d)`：理论加工时间（天）

---

## 2025-12-09

### ETL 性能优化

**问题**：`apply(axis=1)` 导致大数据集处理缓慢

**解决方案**：改用向量化操作生成 record_hash

```python
# 优化前
df['hash'] = df.apply(lambda row: '|'.join(row.values), axis=1)

# 优化后
record_hash = df_records.iloc[:, 0]
for col in df_records.columns[1:]:
    record_hash = record_hash + '|' + df_records[col]
```

**效果**：处理速度提升约 10 倍

---

## 历史记录

详细历史记录请参考 `docs/archives/` 目录下的归档文档。
