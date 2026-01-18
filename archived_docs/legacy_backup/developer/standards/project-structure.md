# 项目结构

本文档描述 MDDAP 项目的目录组织和文件布局。

---

## 顶层目录结构

```
250418_MDDAP_project/
├── data_pipelines/          # 数据管道（ETL、数据库）
├── documentation/           # 统一文档系统（MkDocs）
├── shared_infrastructure/   # 共享基础设施（工具类）
├── scripts/                 # 独立脚本
├── business_domains/        # 业务领域模块
├── logs/                    # 日志文件
├── mkdocs.yml              # MkDocs 配置
└── README.md               # 项目说明
```

---

## data_pipelines/ 数据管道

```
data_pipelines/
├── sources/                 # 数据源（输入端）
│   ├── mes/                 # MES 数据源
│   │   ├── etl/            # ETL 脚本
│   │   └── config/         # 配置文件
│   ├── sfc/                 # SFC 数据源
│   │   ├── etl/
│   │   └── config/
│   └── sap/                 # SAP 数据源
│       ├── etl/
│       └── config/
├── output/                  # 数据输出（Power Query）
│   ├── mes/queries/
│   ├── sfc/queries/
│   └── sap/queries/
├── database/                # 数据库相关
│   ├── schema/             # Schema 定义
│   │   └── init_schema_v2.sql
│   ├── mddap_v2.db         # SQLite 数据库
│   └── DATABASE_GUIDE.md   # 数据库指南
└── collectors/              # 数据采集器
    └── web/                # Web 采集器
```

---

## documentation/ 文档系统

```
documentation/
├── docs/                    # MkDocs 文档源
│   ├── index.md            # 首页
│   ├── kpi/                # KPI 指标说明（用户）
│   ├── etl/                # ETL 流程说明（用户）
│   ├── pq/                 # Power Query 说明（用户）
│   ├── reports/            # 报表说明（用户）
│   ├── project/            # 项目记录
│   ├── reference/          # 参考资料
│   │   └── data-dictionary/ # 数据字典
│   ├── developer/          # 开发者文档
│   │   ├── architecture/   # 架构设计
│   │   ├── standards/      # 开发规范
│   │   └── changelog/      # 变更记录
│   ├── technical/          # 技术实现文档
│   ├── guide/              # 使用指南
│   └── changelog/          # 更新日志
├── reference/              # 参考资源
├── README.md               # 文档系统说明
└── *.bat                   # 启动脚本
```

---

## shared_infrastructure/ 共享基础设施

```
shared_infrastructure/
├── utils/                   # 工具类
│   ├── etl_utils.py        # ETL 通用工具
│   ├── db_utils.py         # 数据库工具
│   └── file_utils.py       # 文件处理工具
└── config/                  # 全局配置
```

---

## 文档分类说明

### 用户文档（documentation/docs/）

| 目录 | 说明 | 适用人群 |
|------|------|---------|
| `kpi/` | KPI 指标定义和计算说明 | 业务用户 |
| `etl/` | ETL 流程和操作指南 | 数据分析师 |
| `pq/` | Power Query 代码说明 | Power BI 开发者 |
| `reports/` | 报表使用说明 | 业务用户 |
| `guide/` | 使用指南和FAQ | 所有用户 |

### 开发者文档（documentation/docs/developer/）

| 目录 | 说明 | 适用人群 |
|------|------|---------|
| `architecture/` | 数据架构、ETL架构设计 | 开发者 |
| `standards/` | 编码规范、命名规范 | 开发者 |
| `changelog/` | 开发日志、迁移记录 | 开发者 |

### 参考资料（documentation/docs/reference/）

| 目录 | 说明 | 适用人群 |
|------|------|---------|
| `data-dictionary/` | 字段映射、计算逻辑 | 用户 + 开发者 |

---

## 关键文件说明

| 文件 | 说明 |
|------|------|
| `mkdocs.yml` | MkDocs 配置，定义文档导航结构 |
| `data_pipelines/database/schema/init_schema_v2.sql` | V2 数据库 Schema 定义 |
| `data_pipelines/database/mddap_v2.db` | SQLite 数据库文件 |
| `documentation/docs/reference/data-dictionary/*.md` | 数据字典文档 |

---

## 版本记录

| 日期 | 更新内容 |
|------|----------|
| 2025-12-13 | 初始版本，整合 docs/ 到 documentation/ |
