# MES数据分层架构设计方案

## 1. 架构概述

### 1.1 设计目标
- **数据分层管理**：原始数据、清洗数据、发布数据分离
- **增量处理**：支持增量数据同步，避免全量重处理
- **版本控制**：保留历史版本，支持数据回溯
- **Power BI友好**：优化Power BI数据消费性能
- **SharePoint集成**：充分利用现有SharePoint基础设施

### 1.2 分层架构
```
SharePoint/
├── 01_RAW/                 # 原始数据层
│   ├── mes_batch_report/
│   │   ├── 2024/11/23/
│   │   │   ├── mes_batch_report_20241123_1000.xlsx
│   │   │   └── metadata.json
│   └── sap_routing/
├── 02_PROCESSED/           # 清洗处理层
│   ├── mes_batch_report/
│   │   ├── 2024/11/23/
│   │   │   ├── mes_batch_report_20241123.parquet
│   │   │   ├── incremental_20241123_1430.parquet
│   │   │   └── process_metadata.json
│   └── sap_routing/
├── 03_PUBLISH/             # 发布消费层
│   ├── mes_batch_report/
│   │   ├── latest/
│   │   │   └── mes_batch_report_latest.parquet
│   │   ├── daily/
│   │   │   └── mes_batch_report_20241123.parquet
│   │   └── powerbi_metadata.json
│   └── sap_routing/
└── 04_METADATA/            # 元数据管理层
    ├── data_lineage.json
    ├── schema_versions.json
    └── etl_run_logs.json
```

## 2. 数据层详细设计

### 2.1 RAW层（原始数据）
**功能**：存储原始源数据，不做任何处理
- **文件格式**：保持原始格式（Excel、CSV等）
- **命名规范**：`{source}_{yyyyMMdd}_{HHmm}.{ext}`
- **保留策略**：90天
- **访问权限**：只读，ETL系统专用

**元数据文件**：
```json
{
  "source_file": "mes_batch_report.xlsx",
  "upload_time": "2024-11-23T10:00:00Z",
  "file_size": 1024000,
  "checksum": "md5_hash",
  "uploader": "etl_system",
  "data_period": "2024-11-22"
}
```

### 2.2 PROCESSED层（清洗处理）
**功能**：存储清洗后的标准化数据
- **文件格式**：Parquet（列式存储，高压缩）
- **分区策略**：按日期分区（YYYY/MM/DD）
- **增量文件**：`incremental_{yyyyMMdd}_{HHmm}.parquet`
- **保留策略**：1年

**处理元数据**：
```json
{
  "process_id": "proc_20241123_1430",
  "source_files": ["mes_batch_report_20241123_1000.xlsx"],
  "output_files": ["mes_batch_report_20241123.parquet"],
  "process_start": "2024-11-23T14:30:00Z",
  "process_end": "2024-11-23T14:35:00Z",
  "records_processed": 5000,
  "records_valid": 4850,
  "records_invalid": 150,
  "transformations": ["data_cleaning", "field_mapping", "validation"]
}
```

### 2.3 PUBLISH层（发布消费）
**功能**：为Power BI等消费端提供优化数据
- **文件格式**：Parquet（优化查询性能）
- **更新策略**：实时更新latest文件，每日生成快照
- **数据聚合**：可包含预聚合数据
- **保留策略**：latest永久 + 30天历史

**Power BI元数据**：
```json
{
  "table_name": "mes_batch_report",
  "last_update": "2024-11-23T14:35:00Z",
  "total_records": 4850,
  "data_period": "2024-01-01 to 2024-11-23",
  "refresh_frequency": "daily",
  "powerbi_columns": [
    {"name": "BatchNumber", "type": "string"},
    {"name": "Operation", "type": "string"},
    {"name": "LT_days", "type": "double"}
  ]
}
```

## 3. 增量处理机制

### 3.1 增量识别策略
```python
# 基于数据哈希的增量识别
def identify_incremental_data():
    # 1. 计算新文件的哈希指纹
    # 2. 对比已处理文件的哈希
    # 3. 识别真正的新增/变更数据
    # 4. 生成增量文件
```

### 3.2 增量文件命名
- **全量文件**：`{table}_{yyyyMMdd}.parquet`
- **增量文件**：`incremental_{table}_{yyyyMMdd}_{HHmm}.parquet`
- **合并文件**：`{table}_merged_{yyyyMMdd}.parquet`

### 3.3 数据合并逻辑
1. **每日合并**：将当日增量合并到全量文件
2. **智能合并**：基于记录哈希避免重复
3. **版本管理**：保留合并前版本用于回滚

## 4. Power BI集成策略

### 4.1 连接方式
```m
// Power Query 模板
let
    GetLatestData = () =>
        let
            LatestFile = "03_PUBLISH/mes_batch_report/latest/mes_batch_report_latest.parquet",
            Data = Parquet.Document(File.Contents(LatestFile))
        in
            Data,
            
    GetHistoricalData = (DateRange as text) =>
        let
            Files = Folder.Files("03_PUBLISH/mes_batch_report/daily"),
            FilteredFiles = List.Select(Files, each Text.Contains([Name], DateRange)),
            Data = List.Combine(List.Transform(FilteredFiles, each Parquet.Document(File.Contents([Folder Path] & "\" & [Name]))))
        in
            Data
in
    GetLatestData()
```

### 4.2 刷新策略
- **自动刷新**：每日定时刷新
- **增量刷新**：只读取变更的增量文件
- **手动刷新**：支持按需刷新

## 5. 数据血缘和元数据管理

### 5.1 数据血缘追踪
```json
{
  "data_lineage": {
    "mes_batch_report": {
      "source": "01_RAW/mes_batch_report/",
      "processed": "02_PROCESSED/mes_batch_report/",
      "published": "03_PUBLISH/mes_batch_report/",
      "transformations": [
        "raw_to_processed",
        "processed_to_published"
      ],
      "dependencies": ["sap_routing", "sfc_batch_report"]
    }
  }
}
```

### 5.2 Schema版本管理
```json
{
  "schema_versions": {
    "mes_batch_report": {
      "current_version": "v2.1",
      "history": [
        {
          "version": "v2.0",
          "effective_date": "2024-10-01",
          "changes": ["added CompletionStatus field"]
        },
        {
          "version": "v2.1", 
          "effective_date": "2024-11-01",
          "changes": ["updated OEE calculation logic"]
        }
      ]
    }
  }
}
```

## 6. 实施计划

### 6.1 阶段一：基础架构搭建（1周）
- [ ] 创建SharePoint目录结构
- [ ] 配置ETL脚本分层输出
- [ ] 建立元数据管理机制
- [ ] 测试基础文件传输

### 6.2 阶段二：增量处理实现（2周）
- [ ] 实现数据哈希计算
- [ ] 开发增量识别逻辑
- [ ] 建立文件合并机制
- [ ] 测试增量处理性能

### 6.3 阶段三：Power BI集成（1周）
- [ ] 配置Power Query连接
- [ ] 测试自动刷新机制
- [ ] 优化查询性能
- [ ] 用户培训文档

### 6.4 阶段四：监控和优化（持续）
- [ ] 建立数据质量监控
- [ ] 性能优化调整
- [ ] 用户反馈收集
- [ ] 持续改进迭代

## 7. 技术要点总结

### 7.1 关键优势
1. **数据隔离**：原始、处理、发布数据完全分离
2. **增量效率**：避免全量重处理，提升ETL性能
3. **版本安全**：完整的历史版本，支持数据回溯
4. **消费优化**：为Power BI提供最优查询性能
5. **扩展性强**：支持更多数据源和消费端

### 7.2 风险控制
1. **存储管理**：定期清理过期文件，控制存储成本
2. **数据一致性**：建立校验机制，确保数据完整性
3. **性能监控**：监控ETL处理时间和Power BI刷新性能
4. **权限管理**：严格控制各层数据访问权限

### 7.3 成功指标
- ETL处理时间减少50%以上
- Power BI刷新时间减少30%以上
- 数据质量问题减少80%以上
- 用户满意度提升到90%以上

---

**文档版本**: v1.0  
**创建日期**: 2024-11-23  
**负责人**: 数据工程团队  
**审核状态**: 待审核
