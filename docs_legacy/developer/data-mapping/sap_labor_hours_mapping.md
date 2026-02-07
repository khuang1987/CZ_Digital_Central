# SAP 人工工时数据映射文档

## 1. 数据源概述

### 1.1 数据源位置
```
C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\40-SAP工时
```

### 1.2 数据文件清单

| 文件名 | 记录数 | 列数 | 说明 |
|--------|--------|------|------|
| EH_FY23.xlsx | 235,668 | 24 | FY23历史数据 |
| EH_FY24.xlsx | 188,938 | 25 | FY24历史数据 |
| EH_FY25.xlsx | 413,118 | 24 | FY25历史数据 |
| EH_FY26.xlsx | 179,534 | 27 | FY26历史数据 |
| YPP_M03_Q5003_00000.xlsx | 53,374 | 27 | 最新数据（每日更新） |

**总计约 107 万条记录**

### 1.3 文件格式说明
- 文件格式：Excel (.xlsx)
- 标题行位置：第8行（skiprows=7）
- 更新频率：YPP文件每日早上更新一次
- 数据内容：已完成工序的人工时间统计

---

## 2. 字段映射

### 2.1 完整字段映射表

| 列序号 | 原始列名 | 数据库字段 | 数据类型 | 说明 |
|--------|----------|------------|----------|------|
| 0 | Plant | Plant | TEXT | 工厂代码 |
| 1 | Work Center | WorkCenter | TEXT | 工作中心代码 |
| 2 | (Unnamed) | WorkCenterDesc | TEXT | 工作中心描述 |
| 3 | Cost Center | CostCenter | TEXT | 成本中心代码 |
| 4 | (Unnamed) | CostCenterDesc | TEXT | 成本中心描述 |
| 5 | Material | Material | TEXT | 物料号 |
| 6 | (Unnamed) | MaterialDesc | TEXT | 物料描述 |
| 7 | Material type | MaterialType | TEXT | 物料类型 |
| 8 | MRP Controller | MRPController | TEXT | MRP控制者代码 |
| 9 | (Unnamed) | MRPControllerDesc | TEXT | MRP控制者描述 |
| 10 | Production Scheduler | ProductionScheduler | TEXT | 生产计划员代码 |
| 11 | (Unnamed) | ProductionSchedulerDesc | TEXT | 生产计划员描述 |
| 12 | Order | OrderNumber | TEXT | 生产订单号 |
| 13 | Order type | OrderType | TEXT | 订单类型代码 |
| 14 | (Unnamed) | OrderTypeDesc | TEXT | 订单类型描述 |
| 15 | Operation | Operation | TEXT | 工序号 |
| 16 | Operation Description | OperationDesc | TEXT | 工序描述 |
| 17 | Posting date | PostingDate | TEXT | 过账日期 |
| 18 | Earned Labor Unit | EarnedLaborUnit | TEXT | 工时单位（Hour） |
| 19 | (Machine Time) | MachineTime | REAL | 机器时间 |
| 20 | (Earned Labor Time) | EarnedLaborTime | REAL | 人工工时 |
| 21 | (Actual Quantity) EA | ActualQuantity | REAL | 实际数量 |
| 22 | (Actual Scrap Qty) EA | ActualScrapQty | REAL | 报废数量 |
| 23 | (Target Quantity) EA | TargetQuantity | REAL | 目标数量 |

### 2.2 YPP文件额外字段（FY26及以后）

| 列序号 | 原始列名 | 数据库字段 | 数据类型 | 说明 |
|--------|----------|------------|----------|------|
| 18 | Actual start: Exec / Setup tim | ActualStartTime | TEXT | 实际开始时间 |
| 19 | Actual finish: Exec. Time | ActualFinishTime | TEXT | 实际完成时间 |
| 20 | Actual Finish Exec.Time Operation | ActualFinishDate | TEXT | 实际完成日期 |

**注意**：YPP文件和FY26文件比历史文件多了3列时间字段。

---

## 3. 数据库表设计

### 3.1 表结构

```sql
CREATE TABLE raw_sap_labor_hours (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- 工厂和工作中心
    Plant TEXT,
    WorkCenter TEXT,
    WorkCenterDesc TEXT,
    
    -- 成本中心
    CostCenter TEXT,
    CostCenterDesc TEXT,
    
    -- 物料信息
    Material TEXT,
    MaterialDesc TEXT,
    MaterialType TEXT,
    
    -- MRP和生产计划
    MRPController TEXT,
    MRPControllerDesc TEXT,
    ProductionScheduler TEXT,
    ProductionSchedulerDesc TEXT,
    
    -- 订单信息
    OrderNumber TEXT,
    OrderType TEXT,
    OrderTypeDesc TEXT,
    
    -- 工序信息
    Operation TEXT,
    OperationDesc TEXT,
    
    -- 时间信息
    PostingDate TEXT,
    ActualStartTime TEXT,
    ActualFinishTime TEXT,
    ActualFinishDate TEXT,
    
    -- 工时和数量
    EarnedLaborUnit TEXT,
    MachineTime REAL,
    EarnedLaborTime REAL,
    ActualQuantity REAL,
    ActualScrapQty REAL,
    TargetQuantity REAL,
    
    -- 元数据
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 3.2 索引设计

```sql
-- 物料索引（常用查询）
CREATE INDEX idx_sap_labor_material ON raw_sap_labor_hours(Material);

-- 订单索引（关联查询）
CREATE INDEX idx_sap_labor_order ON raw_sap_labor_hours(OrderNumber);

-- 过账日期索引（时间范围查询）
CREATE INDEX idx_sap_labor_posting_date ON raw_sap_labor_hours(PostingDate);

-- 工作中心索引（分组统计）
CREATE INDEX idx_sap_labor_workcenter ON raw_sap_labor_hours(WorkCenter);

-- 复合唯一索引（去重）
CREATE UNIQUE INDEX idx_sap_labor_unique ON raw_sap_labor_hours(OrderNumber, Operation, PostingDate);
```

---

## 4. ETL 流程

### 4.1 数据导入流程

```
┌─────────────────┐
│   Excel 文件    │
│  (EH_FY*.xlsx)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   读取数据      │
│  skiprows=7     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   列名标准化    │
│  字段映射       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   数据清洗      │
│  类型转换       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   去重处理      │
│  UPSERT         │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   写入数据库    │
│  raw_sap_labor  │
└─────────────────┘
```

### 4.2 去重策略

使用 `OrderNumber + Operation + PostingDate` 作为唯一键：
- 同一订单、同一工序、同一过账日期的记录视为重复
- 重复记录执行 UPDATE 更新，新记录执行 INSERT

### 4.3 增量更新策略

1. **历史数据**：EH_FY*.xlsx 文件一次性导入
2. **增量数据**：YPP 文件每日导入，通过唯一键去重

---

## 5. 数据质量说明

### 5.1 已知问题
- 历史文件（FY23-FY25）缺少实际开始/完成时间字段
- 不同年份文件列数不一致（24-27列）

### 5.2 数据验证规则
- PostingDate 必须为有效日期格式
- OrderNumber 不能为空
- EarnedLaborTime 应为非负数

---

## 6. 更新记录

| 日期 | 版本 | 更新内容 | 作者 |
|------|------|----------|------|
| 2025-12-14 | 1.0 | 初始版本，完成数据源分析和字段映射 | System |
