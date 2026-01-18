# 迁移记录

记录项目架构迁移和重构历史。

---

## 2025-12-13 文档系统整合

### 背景

项目中存在两个文档目录：
- `docs/` - 开发者技术文档
- `documentation/` - MkDocs 电子说明书

两者内容有重复，维护成本高。

### 迁移方案

将所有文档统一到 `documentation/` 目录，按用户和开发者分类：

```
documentation/docs/
├── kpi/                    # 用户：KPI指标
├── etl/                    # 用户：ETL流程
├── reference/              # 用户+开发者：参考资料
│   └── data-dictionary/    # 数据字典
├── developer/              # 开发者：技术文档
│   ├── architecture/       # 架构设计
│   ├── standards/          # 开发规范
│   └── changelog/          # 变更记录
└── ...
```

### 迁移内容

| 原位置 | 新位置 | 说明 |
|--------|--------|------|
| `docs/field_mapping_reference.md` | `documentation/docs/reference/data-dictionary/` | 拆分为多个文档 |
| `docs/CODING_CONVENTIONS.md` | `documentation/docs/developer/standards/coding-conventions.md` | 直接迁移 |
| `docs/PROJECT_STRUCTURE_GUIDE.md` | `documentation/docs/developer/standards/project-structure.md` | 重写 |

### 后续处理

`docs/` 目录保留用于：
- 历史归档文档（`docs/archives/`）
- 迁移报告等历史记录

---

## 2025-12-12 V2 架构迁移

### 背景

V1 架构将所有计算逻辑放在 Python ETL 脚本中，导致：
- 代码复杂度高
- 修改计算逻辑需要重新运行 ETL
- 难以追溯计算过程

### V2 架构设计

采用 **ODS原始层 + DWD计算层** 分层架构：

```
数据源 Excel → ETL脚本 → ODS原始表 → 计算视图(DWD)
```

**优势**：
- 原始数据保持不变，可追溯
- 计算逻辑在视图中实现，修改即时生效
- ETL 脚本简化，只负责数据导入

### 数据库变更

| V1 | V2 | 说明 |
|----|----|----|
| `mddap.db` | `mddap_v2.db` | 新数据库 |
| 计算字段存储在表中 | 计算字段在视图中 | 架构变更 |
| `ERPCode` | `Plant` | 字段重命名 |
| `ProductName` | `ProductNumber` | 字段重命名 |
| `Operator` | `TrackOutOperator` | 字段重命名 |

---

## 2025-12-03 文档系统迁移

### 背景

文档系统从 `92-说明文档` 迁移到 `documentation/` 目录。

### 变更

- 所有 MkDocs 文档移动到 `documentation/docs/`
- 更新 `mkdocs.yml` 配置
- 保持所有功能和内容不变

---

## 历史迁移记录

详细历史记录请参考 `docs/archives/` 目录下的归档文档：
- `MIGRATION_PLAN.md` - 完整迁移计划
- `PHASE_*_SUMMARY.md` - 各阶段迁移总结
