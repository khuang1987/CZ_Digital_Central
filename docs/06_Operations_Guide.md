# 06 工程与运维指南 (Operations Guide)

本文档面向系统管理员与开发人员，提供日常运维、更新及故障排查的操作流程。

## 1. 日常运行机制 (Cycle)
- **数据刷新**: 
    - 每日 07:15 触发 ETL 脚本抓取。
    - 每周一 08:00 执行全量对齐检查。
- **服务监控**: 通过 PM2 管理 Next.js 进程。

## 2. 故障排查 (Troubleshooting)
- **500 Internal Error**: 
    - **检查项**: 数据库连接字符串是否过期？SQL 类型是否存在 nvarchar 到 float 的强制转换错误 (需使用 `TRY_CAST`)？
    - **日志路径**: `logs/server_stderr.log`.
- **API Timeout**:
    - **检查项**: SQL 查询是否缺少 `WITH (NOLOCK)`？
    - **优化方案**: 是否可采用应用端增强 (App-side Enrichment) 方案？

## 3. 开发流程 (Dev Workflow)
- **代码提交**: 
    - `feat`: 新增看板或功能。
    - `fix`: 修复 SQL 逻辑或 UI 缺陷。
- **环境迁移**: 参考 `docs_legacy/MIGRATION_GUIDE.md` 进行服务器迁移操作。

## 4. 数据库维护
- **索引重建**: 每月对 `mddap_v2` 中的大表执行索引重建，以防查询性能退化。
- **备份策略**: 每日零点自动备份至专用存储路径。
