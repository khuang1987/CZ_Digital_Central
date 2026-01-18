# 计算逻辑说明

本文档详细说明 LT、PT、ST 等核心指标的计算逻辑。

---

## 计算指标概览

| 指标 | 全称 | 单位 | 说明 |
|------|------|------|------|
| **LT(d)** | Lead Time | 天 | 实际加工时间，从进入工序到报工完成 |
| **PT(d)** | Process Time | 天 | 实际加工时间，考虑停产期 |
| **ST(d)** | Standard Time | 天 | 理论加工时间，基于标准工时计算 |
| **IsSetup** | 是否换型 | Yes/No | 同机台上下批次是否更换产品型号 |

---

## IsSetup（是否换型）

### 计算逻辑

```sql
-- 按机台分组，按报工时间排序
PARTITION BY Machine ORDER BY TrackOutTime

-- 上一批次CFN
PreviousCFN = LAG(CFN) OVER (PARTITION BY Machine ORDER BY TrackOutTime)

-- 是否换型判断
IF 当前批次CFN ≠ 上一批次CFN:
    IsSetup = "Yes"  (换型，需要调试时间)
ELSE:
    IsSetup = "No"   (同型号连续生产，无需调试)
```

### 业务含义

- **Yes**：当前批次与上一批次的产品型号（CFN）不同，需要换型调试
- **No**：同型号连续生产，无需调试时间

### 用途

- 决定 ST 计算时是否加入调试时间（SetupTime）

---

## PreviousBatchEndTime（上一批次结束时间）

### 计算逻辑

```sql
PreviousBatchEndTime = LAG(TrackOutTime) OVER (
    PARTITION BY Machine 
    ORDER BY TrackOutTime
)
```

### 业务含义

同一机台上，按报工时间排序后，上一个批次的报工完成时间。

### 用途

- 用于 PT 计算，判断是否有停产期

---

## LT(d) - 实际加工时间

### 计算公式

```
LT(d) = (TrackOutTime - StartTime) / 24
```

### StartTime 确定规则

| 工序 | StartTime 优先级 |
|------|-----------------|
| **0010工序** | `TrackIn_SFC` → `EnterStepTime` → `TrackInTime` |
| **其他工序** | `EnterStepTime` |

### 详细逻辑

```sql
IF Operation = '0010' OR Operation = '10':
    StartTime = COALESCE(TrackIn_SFC, EnterStepTime, TrackInTime)
ELSE:
    StartTime = EnterStepTime

LT(d) = ROUND((julianday(TrackOutTime) - julianday(StartTime)), 2)
```

### 说明

- **0010工序**：首道工序，优先使用 SFC 的 TrackIn 时间
- **其他工序**：使用进入工序时间（上道工序报工时间）
- **不扣除周末**：计算实际日历天数

---

## PT(d) - 实际加工时间（考虑停产期）

### 计算公式

```
PT(d) = (TrackOutTime - StartTime) / 24
```

### StartTime 确定规则

```sql
-- 检测停产期
IF EnterStepTime > PreviousBatchEndTime:
    -- 有停产期：使用 TrackInTime（实际开工时间）
    StartTime = TrackInTime
ELSE:
    -- 正常连续生产：使用 PreviousBatchEndTime
    StartTime = COALESCE(PreviousBatchEndTime, TrackInTime)
```

### 业务含义

| 情况 | StartTime | 说明 |
|------|-----------|------|
| **有停产期** | TrackInTime | 中间有设备停机，使用实际开工时间 |
| **正常生产** | PreviousBatchEndTime | 连续生产，使用上批次结束时间 |
| **首批次** | TrackInTime | 无上批次，使用 TrackIn 时间 |

### 停产期判断

当 `EnterStepTime > PreviousBatchEndTime` 时，说明：
- 物料到达机台后，机台并未立即开始加工
- 中间存在设备停机、换班等待等情况
- 此时应使用 TrackInTime 作为实际加工开始时间

---

## ST(d) - 理论加工时间

### 计算公式

```
ST(d) = (调试时间 + 加工时间 + 换批时间) / 24

其中：
- 调试时间 = IF IsSetup = 'Yes' THEN SetupTime ELSE 0
- 加工时间 = (合格数量 + 报废数量) × 单件工时 / OEE / 3600
- 换批时间 = 0.5 小时（固定）
```

### 详细逻辑

```sql
-- 单件工时（秒），优先使用机器工时
unit_time = COALESCE(EH_machine, EH_labor)

-- 数量
qty = TrackOutQuantity + COALESCE(ScrapQty, 0)

-- OEE（默认0.77）
oee = COALESCE(OEE, 0.77)

-- 调试时间（小时）
setup_hours = CASE 
    WHEN IsSetup = 'Yes' AND SetupTime IS NOT NULL 
    THEN SetupTime 
    ELSE 0 
END

-- ST 计算（小时）
ST_hours = setup_hours + (qty * unit_time / 3600.0 / oee) + 0.5

-- 转换为天
ST(d) = ROUND(ST_hours / 24.0, 2)
```

### 参数说明

| 参数 | 来源 | 默认值 | 说明 |
|------|------|--------|------|
| EH_machine | raw_sap_routing | - | 单件机器工时（秒） |
| EH_labor | raw_sap_routing | - | 单件人工工时（秒） |
| SetupTime | raw_sap_routing | 0 | 调试时间（小时） |
| OEE | raw_sap_routing | 0.77 | OEE系数 |
| ScrapQty | raw_sfc | 0 | 报废数量 |

### 计算示例

假设：
- TrackOutQuantity = 100
- ScrapQty = 5
- EH_machine = 60 秒
- OEE = 0.77
- IsSetup = Yes
- SetupTime = 0.5 小时

```
qty = 100 + 5 = 105
加工时间 = 105 × 60 / 3600 / 0.77 = 2.27 小时
ST_hours = 0.5 + 2.27 + 0.5 = 3.27 小时
ST(d) = 3.27 / 24 = 0.14 天
```

---

## 数据关联

计算视图 `v_mes_metrics` 通过以下方式获取计算所需数据：

```sql
-- MES 与 SFC 关联（获取 TrackInTime 和 ScrapQty）
LEFT JOIN raw_sfc s 
    ON m.BatchNumber = s.BatchNumber 
    AND TRIM(m.Operation) = TRIM(s.Operation)

-- MES 与 SAP Routing 关联（获取标准工时）
LEFT JOIN raw_sap_routing r 
    ON m.CFN = r.CFN 
    AND TRIM(m.Operation) = TRIM(r.Operation)
    AND m."Group" = r."Group"
```

---

## 相关文档

- [字段映射表](field-mapping.md)
- [数据架构](../../architecture/KPI监控系统技术文档.md)
- [跨表对照总表](cross-table-reference.md)
