# SA 指标计算方法与定义

## 1. SA 指标定义

### 1.1 官方定义

!!! quote "Schedule Adherence Definition"
    Schedule Adherence is a metric revealing whether or not production is adhering to the scheduled operating plan. The metric is defined as a percentage of output over demand. Schedule Adherence plays an important role in production performance.

### 1.2 中文解读

Schedule Adherence（计划达成率）是衡量生产是否按照既定生产计划执行的重要指标。该指标通常以"产出/需求"的百分比来表示，反映了生产实际完成情况与计划之间的吻合程度。

---

## 2. 核心计算公式

### 2.1 SA 达成率计算

\[
SA(\%) = \frac{\text{按期完成的批次数}}{\text{总批次数}} \times 100\%
\]

其中：
- **按期完成批次**：CompletionStatus = "OnTime" 的批次
- **总批次数**：统计时间范围内的所有批次

### 2.2 单批次 SA 状态判断

```
IF TrackOutTime ≤ DueTime THEN
    CompletionStatus = "OnTime"
    SA状态 = 1
ELSE
    CompletionStatus = "Overdue"
    SA状态 = 0
END IF
```

---

## 3. 核心计算逻辑

### 3.1 Lead Time (LT) 计算

**Lead Time** 表示从批次进入工序到完成报工的总时间。

#### 3.1.1 MES 数据 - LT 计算

**0010 工序**（首道工序）：
```
开始时间选择优先级：
1. Checkin_SFC（从 SFC 数据合并）
2. EnterStepTime（进入工序时间）
3. TrackInTime（工序投入时间）

LT(d) = (TrackOutTime - 开始时间) / 24 小时
```

**非 0010 工序**：
```
LT(d) = (TrackOutTime - EnterStepTime) / 24 小时
```

#### 3.1.2 SFC 数据 - LT 计算

**0010 工序**：
```
LT(d) = (TrackOutTime - Checkin_SFC) / 24 小时
```

**非 0010 工序**：
```
LT(d) = (TrackOutTime - EnterStepTime) / 24 小时
```

!!! note "说明"
    - LT 计算不扣除周末
    - 单位：天，保留 2 位小数
    - 空值处理：如果起始时间为空，返回 null

---

### 3.2 Process Time (PT) 计算

**Process Time** 表示批次实际加工时间。

#### 3.2.1 MES 数据 - PT 计算

```python
# 确定开始时间
if Checkin_SFC is not None:
    开始时间 = Checkin_SFC
else:
    开始时间 = TrackInTime

# 计算 PT
PT(d) = (TrackOutTime - 开始时间) / 24  # 转换为天
```

#### 3.2.2 SFC 数据 - PT 计算

```python
PT(d) = (TrackOutTime - Checkin_SFC) / 24  # 转换为天
```

!!! note "说明"
    - PT 计算不扣除周末
    - 单位：天，保留 2 位小数
    - 空值处理：如果起始时间为空，返回 null

---

### 3.3 标准时间 ST(d) 计算

**标准时间** 表示理论上完成该批次所需的时间。

#### 3.3.1 MES 数据 - ST 计算

```python
# 1. 确定单件时间（优先机器时间）
if EH_machine(s) is not None and EH_machine(s) > 0:
    单件时间_秒 = EH_machine(s)
else:
    单件时间_秒 = EH_labor(s)

单件时间_小时 = 单件时间_秒 / 3600

# 2. 确定 OEE（默认 77%）
if OEE is None or OEE == 0:
    OEE = 0.77

# 3. 确定调试时间
if Setup == "Yes":
    调试时间_小时 = Setup_Time_h
else:
    调试时间_小时 = 0

# 4. 计算标准时间
ST_小时 = 调试时间_小时 + (StepInQuantity × 单件时间_小时 / OEE)
ST_天 = round(ST_小时 / 24, 2)
```

#### 3.3.2 SFC 数据 - ST 计算

```python
# 1-3 步骤与 MES 相同

# 4. 计算数量（合格品 + 报废品）
数量 = TrackOutQuantity + ScrapQuantity

# 5. 计算标准时间（包含换批时间）
ST_小时 = 调试时间_小时 + (数量 × 单件时间_小时 / OEE) + 0.5
ST_天 = round(ST_小时 / 24, 2)
```

!!! tip "关键参数说明"
    - **EH_machine(s)**：单件机器加工时间（秒）
    - **EH_labor(s)**：单件人工操作时间（秒）
    - **OEE**：设备综合效率（0-1 之间，默认 0.77）
    - **Setup Time (h)**：调试时间（小时）
    - **换批时间**：SFC 数据固定增加 0.5 小时

---

### 3.4 应完工时间 (DueTime) 计算

**应完工时间** 基于工作日日历表计算，自动跳过非工作日（周末和法定节假日）。

#### 3.4.1 核心算法

```python
def calculate_due_time(start_time, plan_hours, calendar_df):
    """
    按工作日逐天累加工作时间，跳过非工作日
    
    参数:
        start_time: 开始时间（TrackInTime 或 Checkin_SFC）
        plan_hours: 计划小时数（标准时间 + 换批时间）
        calendar_df: 工作日日历表
    
    返回:
        due_time: 应完工时间
        weekend_days: 扣除的非工作日天数
    """
    current_time = start_time
    remaining_hours = plan_hours
    weekend_hours = 0
    
    while remaining_hours > 0:
        current_date = current_time.date()
        
        # 查询日历表判断是否为工作日
        is_workday = calendar_df[
            calendar_df['日期'] == current_date
        ]['是否工作日'].values[0]
        
        if is_workday:
            # 工作日：减少剩余工时（24小时连续工作）
            daily_hours = min(remaining_hours, 24)  # 每天24小时
            remaining_hours -= daily_hours
        else:
            # 非工作日：计入 Weekend
            weekend_hours += 24
        
        # 推进到下一天
        current_time += timedelta(days=1)
    
    due_time = current_time
    weekend_days = round(weekend_hours / 24, 2)
    
    return due_time, weekend_days
```

#### 3.4.2 MES 数据 - DueTime 计算

```python
# 1. 计算计划小时数
单件时间_秒 = EH_machine(s) or EH_labor(s)
单件时间_小时 = 单件时间_秒 / 3600
OEE = OEE or 0.77
调试时间 = Setup_Time_h if Setup == "Yes" else 0

计划小时数 = 调试时间 + (StepInQuantity × 单件时间_小时 / OEE) + 0.5

# 2. 调用算法计算 DueTime
DueTime, Weekend(d) = calculate_due_time(TrackInTime, 计划小时数, 日历表)
```

#### 3.4.3 SFC 数据 - DueTime 计算

```python
# 1. 计算计划小时数
数量 = TrackOutQuantity + ScrapQuantity
计划小时数 = 调试时间 + (数量 × 单件时间_小时 / OEE) + 0.5

# 2. 确定开始时间
开始时间 = TrackInTime if TrackInTime else Checkin_SFC

# 3. 调用算法计算 DueTime
DueTime, Weekend(d) = calculate_due_time(开始时间, 计划小时数, 日历表)
```

!!! warning "重要说明"
    - 应完工时间计算**会跳过非工作日**（周末和法定节假日）
    - 工作日判断基于 `日历工作日表.csv`
    - Weekend(d) 表示从开始到应完工时间之间的非工作日天数
    - 换批时间固定为 0.5 小时

---

### 3.5 完工状态 (CompletionStatus) 判断

```python
if TrackOutTime <= DueTime:
    CompletionStatus = "OnTime"
    SA状态 = 1
else:
    CompletionStatus = "Overdue"
    SA状态 = 0
```

!!! tip "容差说明"
    - 标准计算不包含容差
    - 如需容差分析，可在 Power BI 中添加 8 小时容差：
      ```
      CompletionStatus_WithTolerance = 
      IF(TrackOutTime <= DueTime + 8/24, "OnTime", "Overdue")
      ```

---

## 4. 换批 (Setup) 判断逻辑

### 4.1 MES 数据换批判断

```python
# 按 machine 和 CFN 分组，按 TrackOutTime 排序
# 计算 PreviousBatchEndTime（同一机台上一批次的结束时间）

if PreviousBatchEndTime is None:
    # 第一批次，需要换批
    Setup = "Yes"
else:
    # 检查产品号是否相同
    if CFN == 上一批次的CFN:
        Setup = "No"
    else:
        Setup = "Yes"
```

### 4.2 SFC 数据换批判断

```python
# SFC 数据暂不实施换批判断
Setup = "No"  # 默认值
```

!!! note "换批说明"
    - 换批判断基于同一机台的连续批次
    - 如果产品号（CFN）发生变化，判断为需要换批
    - 换批会增加调试时间（Setup Time）

---

## 5. 计算示例

详细的计算示例请参见：[计算示例](examples.md)

---

## 6. 相关链接

- [字段说明](field-specification.md) - 查看所有字段定义
- [数据源说明](data-sources.md) - 了解数据来源
- [ETL 处理](../etl/etl-sa.md) - 查看数据处理流程

