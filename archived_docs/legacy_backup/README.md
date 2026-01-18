# 文档中心

## 📚 文档导航

### 01_用户文档
- [KPI监控系统用户指南](01_user_guide/KPI监控系统用户指南.md) - 系统使用说明和常见问题

### 02_技术文档
#### 系统架构
- [KPI监控系统技术文档](02_technical/architecture/KPI监控系统技术文档.md) - 系统架构和技术实现

#### 数据库
- [数据库V2概览](02_technical/database/overview.md) - V2架构、表/视图分层与ETL对应关系
- [数据库V2数据字典](02_technical/database/data_dictionary/index.md) - 字段定义、映射关系与计算逻辑
- [MES 指标物化方案（BI 秒开）](02_technical/database/mes_metrics_materialization.md) - 将 v_mes_metrics 从实时重计算迁移为物化快照表

#### 开发指南
- [编码规范](02_technical/dev_guide/编码规范.md) - 项目编码标准和最佳实践

#### 运维手册
- [数据处理技术路线总览 & 脚本清理建议](02_technical/operations/data_processing_route_and_cleanup.md) - 数据获取→清洗→SQL Server→物化→Parquet 全链路，以及 scripts 清理候选
- [Core Refresh / Full Refresh Runbook（SQL Server Only）](02_technical/operations/core_refresh_runbook.md) - 日常刷新/全量刷新/导出reconcile/常见排障
- *待补充：部署指南*
- *待补充：故障排查*

### 03_项目管理
- [迁移计划](03_project_management/迁移计划.md) - 系统迁移和升级计划

### 04_报告归档
#### 优化报告
- [ETL优化报告_20251209](04_reports_archive/optimization_reports/ETL优化报告_20251209.md)
- [性能测试报告_20251209](04_reports_archive/optimization_reports/性能测试报告_20251209.md)

#### 阶段总结
- [阶段3_文件组织总结](04_reports_archive/phase_summary/阶段3_文件组织总结.md)
- [阶段3.5_脚本组织总结](04_reports_archive/phase_summary/阶段3.5_脚本组织总结.md)
- [阶段4_最终清理总结](04_reports_archive/phase_summary/阶段4_最终清理总结.md)

#### 历史文档
- [完整逻辑验证报告](04_reports_archive/historical_docs/完整逻辑验证报告.md)
- [DueTime修复报告](04_reports_archive/historical_docs/DueTime修复报告.md)

### 05_参考资料
#### 模板
- [ETL迁移模板](05_reference/templates/ETL迁移模板.md)

#### 工具说明
- [项目结构指南](05_reference/tools/项目结构指南.md)

---

## 📝 文档维护规范

请以 [DOCS_RULES.md](DOCS_RULES.md) 为准。
