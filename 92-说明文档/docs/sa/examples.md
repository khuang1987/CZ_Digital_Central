# SA 指标计算示例

本页面通过实际案例演示 SA 指标的完整计算过程。

---

## 示例 1：标准批次计算（按期完成）

### 1.1 基础数据

假设有一个批次的基本信息如下：

| 字段 | 值 |
|------|------|
| BatchNumber | K24A2066 |
| CFN | 858-957-11 |
| Operation | 0020 |
| Operation description | 数控铣 |
| machine | M001 |
| EnterStepTime | 2025-01-10 08:00:00 |
| TrackInTime | 2025-01-10 09:00:00 |
| TrackOutTime | 2025-01-12 16:00:00 |
| StepInQuantity | 100 |

### 1.2 标准时间参数（来自 Routing 表）

| 参数 | 值 |
|------|------|
| EH_machine(s) | 3600（秒） |
| EH_labor(s) | 1800（秒） |
| OEE | 0.80 |
| Setup Time (h) | 0.5 |
| Setup | Yes |

### 1.3 计算过程

#### Step 1: 计算 Lead Time (LT)

```python
# 非 0010 工序，使用 EnterStepTime
LT(d) = (TrackOutTime - EnterStepTime) / 24
      = (2025-01-12 16:00:00 - 2025-01-10 08:00:00) / 24
      = 56 小时 / 24
      = 2.33 天
```

#### Step 2: 计算 Process Time (PT)

```python
# 假设没有 Checkin_SFC，使用 TrackInTime
PT(d) = (TrackOutTime - TrackInTime) / 24
      = (2025-01-12 16:00:00 - 2025-01-10 09:00:00) / 24
      = 55 小时 / 24
      = 2.29 天
```

#### Step 3: 计算标准时间 (ST)

```python
# 1. 单件时间
单件时间_秒 = EH_machine(s) = 3600 秒
单件时间_小时 = 3600 / 3600 = 1.0 小时

# 2. OEE
OEE = 0.80

# 3. 调试时间
Setup = "Yes" → 调试时间 = 0.5 小时

# 4. 计算 ST
ST_小时 = 0.5 + (100 × 1.0 / 0.80) + 0.5
        = 0.5 + 125 + 0.5
        = 126 小时

ST(d) = 126 / 24 = 5.25 天
```

#### Step 4: 计算应完工时间 (DueTime)

假设工作日日历如下：

| 日期 | 是否工作日 |
|------|------------|
| 2025-01-10（五） | True |
| 2025-01-11（六） | False |
| 2025-01-12（日） | False |
| 2025-01-13（一） | True |
| 2025-01-14（二） | True |
| 2025-01-15（三） | True |

```python
# 计划小时数 = 126 小时
# 开始时间 = TrackInTime = 2025-01-10 09:00:00

累加过程（24小时连续工作制）：
- 2025-01-10：工作日，累加 24h（剩余 126-24=102h）
- 2025-01-11：周末，跳过（剩余 102h，weekend_hours+24）
- 2025-01-12：周末，跳过（剩余 102h，weekend_hours+48）
- 2025-01-13：工作日，累加 24h（剩余 102-24=78h）
- 2025-01-14：工作日，累加 24h（剩余 78-24=54h）
- 2025-01-15：工作日，累加 24h（剩余 54-24=30h）
- 2025-01-16：工作日，累加 24h（剩余 30-24=6h）
- 2025-01-17：工作日，累加 6h（剩余 0h）

最终：
DueTime = 2025-01-17 15:00:00（约）
Weekend(d) = 48 小时 / 24 = 2.00 天
```

#### Step 5: 判断完工状态

```python
TrackOutTime = 2025-01-12 16:00:00
DueTime = 2025-01-26 17:00:00

TrackOutTime <= DueTime → CompletionStatus = "OnTime"
SA状态 = 1
```

### 1.4 最终结果

| 计算字段 | 结果 |
|----------|------|
| LT(d) | 2.33 天 |
| PT(d) | 2.29 天 |
| ST(d) | 5.25 天 |
| DueTime | 2025-01-26 17:00:00 |
| Weekend(d) | 1.33 天 |
| CompletionStatus | OnTime |
| SA状态 | 1 |

!!! success "结论"
    该批次提前完成，SA 状态为 **按期（OnTime）**

---

## 示例 2：逾期批次计算

### 2.1 基础数据

| 字段 | 值 |
|------|------|
| BatchNumber | K24B3088 |
| CFN | 858-957-11 |
| Operation | 0030 |
| Operation description | 线切割 |
| TrackInTime | 2025-01-15 08:00:00 |
| TrackOutTime | 2025-01-25 18:00:00 |
| StepInQuantity | 50 |

### 2.2 标准时间参数

| 参数 | 值 |
|------|------|
| EH_machine(s) | 7200（秒） |
| OEE | 0.75 |
| Setup Time (h) | 1.0 |
| Setup | Yes |

### 2.3 计算过程

#### 计算 ST

```python
单件时间_小时 = 7200 / 3600 = 2.0 小时
ST_小时 = 1.0 + (50 × 2.0 / 0.75) + 0.5
        = 1.0 + 133.33 + 0.5
        = 134.83 小时
ST(d) = 134.83 / 24 = 5.62 天
```

#### 计算 DueTime（假设无周末）

```python
# 简化计算：5.62 天 ≈ 6 个工作日
DueTime ≈ 2025-01-23 12:00:00
```

#### 判断完工状态

```python
TrackOutTime = 2025-01-25 18:00:00
DueTime = 2025-01-23 12:00:00

TrackOutTime > DueTime → CompletionStatus = "Overdue"
SA状态 = 0
逾期时间 = 2025-01-25 18:00:00 - 2025-01-23 12:00:00 = 2.25 天
```

### 2.4 最终结果

| 计算字段 | 结果 |
|----------|------|
| ST(d) | 5.62 天 |
| DueTime | 2025-01-23 12:00:00 |
| TrackOutTime | 2025-01-25 18:00:00 |
| CompletionStatus | Overdue |
| SA状态 | 0 |
| 逾期时间 | 2.25 天 |

!!! warning "结论"
    该批次逾期 2.25 天，SA 状态为 **逾期（Overdue）**

---

## 示例 3：0010 工序特殊处理

### 3.1 基础数据

| 字段 | 值 |
|------|------|
| BatchNumber | K25A1001 |
| Operation | 0010 |
| EnterStepTime | 2025-01-20 10:00:00 |
| TrackInTime | 2025-01-20 11:00:00 |
| Checkin_SFC | 2025-01-20 09:30:00 |
| TrackOutTime | 2025-01-21 15:00:00 |

### 3.2 计算 LT（0010 工序特殊逻辑）

```python
# 0010 工序优先级：Checkin_SFC > EnterStepTime > TrackInTime
开始时间 = Checkin_SFC = 2025-01-20 09:30:00

LT(d) = (2025-01-21 15:00:00 - 2025-01-20 09:30:00) / 24
      = 29.5 小时 / 24
      = 1.23 天
```

### 3.3 计算 PT

```python
# PT 优先使用 Checkin_SFC
开始时间 = Checkin_SFC = 2025-01-20 09:30:00

PT(d) = (2025-01-21 15:00:00 - 2025-01-20 09:30:00) / 24
      = 1.23 天
```

!!! info "0010 工序说明"
    0010 工序（首道工序）会优先使用 **Checkin_SFC** 作为起始时间，因为这是批次正式进入生产的时间点。

---

## 示例 4：多批次 SA 达成率计算

### 4.1 数据集

假设某周有以下批次：

| BatchNumber | Operation | CompletionStatus | SA状态 |
|-------------|-----------|------------------|--------|
| K24A2066 | 0020 | OnTime | 1 |
| K24B3088 | 0030 | Overdue | 0 |
| K24C1234 | 0010 | OnTime | 1 |
| K24D5678 | 0040 | OnTime | 1 |
| K24E9012 | 0020 | Overdue | 0 |
| K24F3456 | 0030 | OnTime | 1 |

### 4.2 计算 SA 达成率

```python
总批次数 = 6
按期批次数 = 4（SA状态 = 1 的数量）

SA达成率 = 4 / 6 × 100% = 66.67%
```

### 4.3 分工序分析

| 工序 | 总批次 | 按期批次 | SA达成率 |
|------|--------|----------|----------|
| 0010 | 1 | 1 | 100.00% |
| 0020 | 2 | 1 | 50.00% |
| 0030 | 2 | 1 | 50.00% |
| 0040 | 1 | 1 | 100.00% |

!!! tip "分析建议"
    - 整体 SA 达成率：66.67%
    - 0010、0040 工序表现优秀（100%）
    - 0020、0030 工序需要关注和改进

---

## 示例 5：容差分析

### 5.1 数据

| BatchNumber | TrackOutTime | DueTime | 差异（小时） |
|-------------|--------------|---------|-------------|
| K24A1111 | 2025-01-15 16:00 | 2025-01-15 17:00 | -1（提前） |
| K24A2222 | 2025-01-15 20:00 | 2025-01-15 17:00 | +3（逾期） |
| K24A3333 | 2025-01-16 01:00 | 2025-01-15 17:00 | +8（逾期） |
| K24A4444 | 2025-01-16 02:00 | 2025-01-15 17:00 | +9（逾期） |

### 5.2 不含容差（标准）

```python
容差 = 0 小时

K24A1111: OnTime（-1h ≤ 0）
K24A2222: Overdue（+3h > 0）
K24A3333: Overdue（+8h > 0）
K24A4444: Overdue（+9h > 0）

SA达成率 = 1/4 = 25%
```

### 5.3 含 8 小时容差

```python
容差 = 8 小时

K24A1111: OnTime（-1h ≤ 8h）
K24A2222: OnTime（+3h ≤ 8h）
K24A3333: OnTime（+8h ≤ 8h）
K24A4444: Overdue（+9h > 8h）

SA达成率 = 3/4 = 75%
```

!!! info "容差说明"
    - 标准 SA 计算不包含容差
    - 如需容差分析，可在 Power BI 中创建计算列
    - 常用容差：8 小时（1 个工作日）

---

## Power BI DAX 示例

### 计算 SA 达成率

```dax
SA达成率 = 
DIVIDE(
    COUNTROWS(FILTER('MES数据', 'MES数据'[CompletionStatus] = "OnTime")),
    COUNTROWS('MES数据'),
    0
)
```

### 计算逾期批次数

```dax
逾期批次数 = 
COUNTROWS(FILTER('MES数据', 'MES数据'[CompletionStatus] = "Overdue"))
```

### 计算平均逾期时间（仅逾期批次）

```dax
平均逾期时间_天 = 
AVERAGEX(
    FILTER('MES数据', 'MES数据'[CompletionStatus] = "Overdue"),
    ('MES数据'[TrackOutTime] - 'MES数据'[DueTime]) * 1
)
```

---

## 相关链接

- [计算方法与定义](calculation-definition.md) - 查看详细计算公式
- [字段说明](field-specification.md) - 了解所有字段定义
- [Power Query 代码](../pq/mes-records.md) - 查看 Power BI 查询代码

