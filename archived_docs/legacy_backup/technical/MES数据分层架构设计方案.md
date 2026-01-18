# MES数据分层架构设计方案

## 1. 架构概述

### 1.1 设计目标
- **数据分层管理**：清洗数据、发布数据分离，去除RAW层避免重复存储
- **增量处理**：支持记录级增量数据同步，避免全量重处理
- **版本控制**：保留历史版本，支持数据回溯
- **Power BI友好**：优化Power BI数据消费性能
- **SharePoint集成**：充分利用现有SharePoint基础设施
- **企业级扩展**：支持多系统（MES、SFC等）统一管理

### 1.2 分层架构
```
B2_自动导出程序/
├── MES/                    # MES系统数据
│   ├── batch_report/       # 批次报告实体
│   │   ├── 01_PROCESSED/   # 清洗处理层
│   │   │   └── 2024/11/23/
│   │   │       ├── incremental_mes_batch_report_20241123_1430.parquet
│   │   │       └── mes_batch_report_full_20241123_1430.parquet
│   │   └── 02_PUBLISH/     # 发布消费层
│   │       ├── latest/
│   │       │   └── mes_batch_report_latest.parquet
│   │       └── daily/
│   │           └── mes_batch_report_20241123.parquet
│   └── routing_data/       # 工艺路线实体
│       ├── 01_PROCESSED/
│       └── 02_PUBLISH/
├── SFC/                    # SFC系统数据（未来扩展）
│   ├── batch_report/
│   │   ├── 01_PROCESSED/
│   │   └── 02_PUBLISH/
│   └── quality_data/
│       ├── 01_PROCESSED/
│       └── 02_PUBLISH/
└── 03_METADATA/            # 元数据管理层
    ├── MES/
    │   ├── batch_report/
    │   │   ├── processed_records_hash.parquet
    │   │   ├── watermark.json
    │   │   └── data_lineage.json
    │   └── routing_data/
    └── SFC/
        └── batch_report/
```

## 2. 数据层详细设计

### 2.1 PROCESSED层（清洗处理）
**功能**：存储清洗后的标准化数据，支持增量处理
- **文件格式**：Parquet（列式存储，高压缩）
- **分区策略**：按日期分区（YYYY/MM/DD）
- **增量文件**：`incremental_{system}_{entity}_{yyyyMMdd}_{HHmm}.parquet`
- **全量文件**：`{system}_{entity}_full_{yyyyMMdd}_{HHmm}.parquet`
- **保留策略**：1年

**记录级变更检测**：
- **哈希算法**：MD5(BatchNumber + Operation + TrackOutTime)
- **存储方式**：轻量级哈希文件 `processed_records_hash.parquet`
- **水位线机制**：`watermark.json` 追踪处理进度

**处理元数据**：
```json
{
  "process_id": "etl_20241123_1430",
  "system_name": "MES",
  "entity_name": "batch_report",
  "process_start": "2024-11-23T14:30:00Z",
  "process_end": "2024-11-23T14:35:00Z",
  "incremental_stats": {
    "total_records": 5000,
    "new_records": 250,
    "duplicate_records": 4750,
    "duplicate_percentage": 95.0
  },
  "transformations": [
    "record_level_change_detection",
    "data_cleaning",
    "field_mapping",
    "validation",
    "lt_calculation",
    "pt_calculation",
    "st_calculation",
    "completion_status_calculation"
  ]
}
```

### 2.2 PUBLISH层（发布消费）
**功能**：为Power BI等消费端提供优化数据
- **文件格式**：Parquet（优化查询性能）
- **更新策略**：实时更新latest文件，每日生成快照
- **数据聚合**：可包含预聚合数据
- **保留策略**：latest永久 + 30天历史

**Power BI元数据**：
```json
{
  "table_name": "mes_batch_report",
  "system_name": "MES",
  "entity_name": "batch_report",
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

### 2.3 METADATA层（元数据管理）
**功能**：统一管理所有系统的元数据
- **数据血缘**：`data_lineage.json`
- **记录哈希**：`processed_records_hash.parquet`
- **水位线**：`watermark.json`
- **系统分离**：按系统和实体组织元数据

## 3. 增量处理机制

### 3.1 记录级增量识别策略
```python
# 基于记录哈希的增量识别
def identify_new_records():
    # 1. 加载已处理记录哈希值
    # 2. 计算当前记录的业务键哈希
    # 3. 识别新增/变更记录
    # 4. 只处理真正的新数据
```

### 3.2 增量文件命名
- **增量文件**：`incremental_{system}_{entity}_{yyyyMMdd}_{HHmm}.parquet`
- **全量文件**：`{system}_{entity}_full_{yyyyMMdd}_{HHmm}.parquet`
- **哈希文件**：`processed_records_hash.parquet`

### 3.3 数据合并逻辑
1. **智能合并**：基于业务键哈希避免重复
2. **增量合并**：将当日增量合并到全量文件
3. **去重机制**：`BatchNumber + Operation + TrackOutTime` 复合键
4. **版本管理**：保留合并前版本用于回滚

## 4. Power BI集成策略

### 4.1 连接方式
```m
// Power Query 模板
let
    // 读取最新发布数据
    GetLatestData = () =>
        let
            LatestFile = "B2_自动导出程序/MES/batch_report/02_PUBLISH/latest/mes_batch_report_latest.parquet",
            Data = Parquet.Document(File.Contents(LatestFile))
        in
            Data,
            
    // 获取历史数据
    GetHistoricalData = (DateRange as text) =>
        let
            Files = Folder.Files("B2_自动导出程序/MES/batch_report/02_PUBLISH/daily"),
            FilteredFiles = List.Select(Files, each Text.Contains([Name], DateRange)),
            Data = List.Combine(List.Transform(FilteredFiles, each Parquet.Document(File.Contents([Folder Path] & "\" & [Name]))))
        in
            Data
in
    GetLatestData()
```

### 4.2 刷新策略
- **自动刷新**：每日定时刷新
- **增量刷新**：只读取变更的数据
- **手动刷新**：支持按需刷新

## 5. 数据血缘和元数据管理

### 5.1 数据血缘追踪
```json
{
  "data_lineage": {
    "MES": {
      "batch_report": {
        "processed": "B2_自动导出程序/MES/batch_report/01_PROCESSED/",
        "published": "B2_自动导出程序/MES/batch_report/02_PUBLISH/",
        "metadata": "B2_自动导出程序/03_METADATA/MES/batch_report/",
        "transformations": [
          "record_level_change_detection",
          "data_cleaning",
          "field_mapping",
          "validation",
          "lt_calculation",
          "pt_calculation",
          "st_calculation",
          "completion_status_calculation"
        ],
        "dependencies": ["sap_routing", "sfc_batch_report"]
      }
    }
  }
}
```

### 5.2 记录哈希管理
```json
{
  "hash_management": {
    "algorithm": "MD5",
    "composite_key": "BatchNumber + Operation + TrackOutTime",
    "storage_file": "processed_records_hash.parquet",
    "last_updated": "2024-11-23T14:35:00Z",
    "total_processed_records": 4850
  }
}
```

### 5.3 水位线管理
```json
{
  "watermark": {
    "last_processed_date": "2024-11-23",
    "last_processed_time": "2024-11-23 14:35:00",
    "total_records_processed": 4850,
    "updated_at": "2024-11-23T14:35:00Z"
  }
}
```

## 6. 企业级扩展架构

### 6.1 多系统支持
```
B2_自动导出程序/
├── MES/                    # MES生产执行系统
│   ├── batch_report/       # 批次报告
│   └── routing_data/       # 工艺路线
├── SFC/                    # SFC服务反馈系统
│   ├── batch_report/       # 批次报告
│   └── quality_data/       # 质量数据
├── ERP/                    # ERP企业资源系统（未来）
│   ├── material_master/    # 物料主数据
│   └── production_order/   # 生产订单
└── 03_METADATA/            # 统一元数据管理
    ├── MES/
    ├── SFC/
    └── ERP/
```

### 6.2 标准化命名规范
- **系统名称**：英文大写（MES、SFC、ERP）
- **实体名称**：英文小写下划线（batch_report、quality_data）
- **文件命名**：`{type}_{system}_{entity}_{timestamp}.parquet`
- **目录结构**：`{system}/{entity}/{layer}/`

### 6.3 配置化架构
```yaml
# 系统配置示例
systems:
  MES:
    entities:
      batch_report:
        source_type: "excel"
        primary_keys: ["BatchNumber", "Operation", "TrackOutTime"]
        processing_functions: ["lt_calculation", "pt_calculation"]
  SFC:
    entities:
      batch_report:
        source_type: "excel"
        primary_keys: ["BatchNumber", "Operation", "CompletionTime"]
        processing_functions: ["quality_calculation"]
```

## 7. 实施计划

### 7.1 阶段一：基础架构搭建（1周）
- [ ] 创建企业级目录结构
- [ ] 配置MES批次报告ETL脚本
- [ ] 建立记录级变更检测机制
- [ ] 测试基础增量处理功能

### 7.2 阶段二：增量处理优化（2周）
- [ ] 实现记录级哈希计算
- [ ] 开发智能增量识别逻辑
- [ ] 建立水位线管理机制
- [ ] 测试95%重复数据跳过性能

### 7.3 阶段三：Power BI集成（1周）
- [ ] 配置Power Query连接模板
- [ ] 测试自动刷新机制
- [ ] 优化查询性能
- [ ] 用户培训和文档

### 7.4 阶段四：多系统扩展（2周）
- [ ] 实现SFC系统集成
- [ ] 建立统一元数据管理
- [ ] 配置化架构实现
- [ ] 系统间数据血缘追踪

### 7.5 阶段五：监控和优化（持续）
- [ ] 建立数据质量监控
- [ ] 性能优化调整
- [ ] 用户反馈收集
- [ ] 持续改进迭代

## 8. 技术要点总结

### 8.1 关键优势
1. **存储优化**：去除RAW层，节省95%存储空间
2. **处理效率**：记录级增量检测，只处理5%新数据
3. **企业架构**：系统-实体-分层标准化结构
4. **扩展性强**：支持MES、SFC、ERP等多系统
5. **数据治理**：完整血缘追踪和元数据管理

### 8.2 风险控制
1. **哈希冲突**：MD5算法足够安全，可升级到SHA256
2. **数据一致性**：水位线机制确保处理完整性
3. **性能监控**：监控增量检测和合并性能
4. **权限管理**：按系统和实体分层控制访问

### 8.3 成功指标
- ETL处理时间减少80%以上
- 存储空间节省90%以上  
- Power BI刷新时间减少50%以上
- 支持3+系统统一管理
- 数据质量问题减少90%以上

---

**文档版本**: v2.0  
**创建日期**: 2024-11-23  
**更新日期**: 2024-11-26  
**架构类型**: 企业级增量处理架构  
**负责人**: 数据工程团队  
**审核状态**: 已优化
