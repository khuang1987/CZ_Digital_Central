# SFC数据处理计算逻辑梳理

## 一、计算字段概览

SFC数据处理的ETL脚本（`etl_sfc.py`）计算以下字段：

| 字段名 | 计算函数 | 说明 |
|--------|---------|------|
| LT(d) | `calculate_sfc_lt` | 工序周期时间（天） |
| PT(d) | `calculate_sfc_pt` | 加工周期时间（天） |
| ST(d) | `calculate_sfc_st` | 标准时间（天） |
| DueTime | `calculate_sfc_due_time` | 应完工时间 |
| Weekend(d) | `calculate_sfc_weekend_days` | 周末扣除天数 |
| CompletionStatus | `calculate_sfc_completion_status` | 完工状态 |
| Machine(#) | `extract_machine_number_sfc` | 设备编号（数字） |

---

## 二、详细计算逻辑

### 1. LT(d) - 工序周期时间

**函数**: `calculate_sfc_lt(row: pd.Series) -> Optional[float]`

**计算逻辑**:
```python
if operation == "0010":
    LT(d) = (TrackOutTime - Checkin_SFC) / 86400  # 转换为天数
else:
    LT(d) = (TrackOutTime - EnterStepTime) / 86400
```

**与文档对比**:
- ✅ **符合要求**：Operation="0010"时使用Checkin_SFC，其他使用EnterStepTime
- ⚠️ **注意事项**：SFC数据可能没有EnterStepTime字段，此时返回None

**代码位置**: 第377-397行

---

### 2. PT(d) - 加工周期时间

**函数**: `calculate_sfc_pt(row: pd.Series) -> Optional[float]`

**计算逻辑**:
```python
if Checkin_SFC 不为空:
    PT(d) = (TrackOutTime - Checkin_SFC) / 86400
else:
    返回 None
```

**计算逻辑**（已更新）:
```python
if Checkin_SFC 不为空:
    PT(d) = (TrackOutTime - Checkin_SFC) / 86400
elif StartTime 不为空:
    PT(d) = (TrackOutTime - StartTime) / 86400
else:
    返回 None
```

**与文档对比**:
- ✅ **符合要求**：优先使用Checkin_SFC，如果没有则使用StartTime（如果存在）
- ✅ **已修复**：已添加StartTime作为备选字段

**代码位置**: 第400-418行

---

### 3. ST(d) - 标准时间

**函数**: `calculate_sfc_st(row: pd.Series) -> Optional[float]`

**计算逻辑**:
```python
qty = TrackOutQuantity 或 StepInQuantity（优先TrackOutQuantity）
effective_time = Effective Time (h)  # 已在合并标准时间表时计算
oee = OEE（默认0.77）
setup_time = Setup Time (h)（仅当Setup="Yes"时）

total_hours = setup_time + qty * effective_time / oee
ST(d) = total_hours / 24
```

**与文档对比**:
- ✅ **基本符合**：公式正确
- ⚠️ **字段差异**：文档要求使用StepInQuantity，但SFC优先使用TrackOutQuantity
- ✅ **OEE处理**：默认值0.77正确

**代码位置**: 第416-434行

---

### 4. DueTime - 应完工时间

**函数**: `calculate_sfc_due_time(row: pd.Series) -> Optional[datetime]`

**计算逻辑**:
```python
start_time = TrackInTime 或 Checkin_SFC（优先TrackInTime）
planned_hours = setup_time + effective_time * qty / oee
due0 = start_time + planned_hours + 0.5小时
due_final = adjust_weekend_sfc(start_time, due0)  # 调整周末
```

**与文档对比**:
- ✅ **符合要求**：包含0.5小时换批时间
- ✅ **周末调整**：调用adjust_weekend_sfc函数
- ⚠️ **数据源差异**：SFC可能没有TrackInTime，使用Checkin_SFC代替

**代码位置**: 第437-460行

---

### 5. Weekend(d) - 周末扣除天数

**函数**: `calculate_sfc_weekend_days(row: pd.Series) -> Optional[int]`

**计算逻辑**:
```python
# 计算应完工时间（不含周末调整）
planned_hours = setup_time + effective_time * qty / oee
due0 = start_time + planned_hours + 0.5小时

# 统计[start+1d, due0]间的周末天数（第一次）
dates = pd.date_range(start+1d, due0, inclusive='left')
weekend_days = sum(weekday >= 5 for d in dates)  # 周六=5, 周日=6

# 调整后再次检查
d1 = due0 + weekend_days
dates1 = pd.date_range(start+1d, d1, inclusive='left')
weekend_days1 = sum(weekday >= 5 for d in dates1)

# 返回最终周末天数
return weekend_days1
```

**与文档对比**:
- ✅ **符合要求**：统计TrackInTime次日到due0当日之间的周末天数
- ✅ **递归检查**：确保顺延后不再跨周末
- ⚠️ **注意**：返回的是调整后的周末天数，而非初始计算的

**代码位置**: 第487-520行

---

### 6. CompletionStatus - 完工状态

**函数**: `calculate_sfc_completion_status(row: pd.Series) -> Optional[str]`

**计算逻辑**:
```python
if DueTime 或 TrackOutTime 为空:
    return None

tolerance = 8小时
if TrackOutTime <= DueTime + tolerance:
    return "OnTime"
else:
    return "Overdue"
```

**与文档对比**:
- ⚠️ **差异**：文档要求 `TrackOutTime <= DueTime`，但代码增加了8小时容差
- ✅ **容差处理**：实际业务中允许8小时容差是合理的

**代码位置**: 第523-538行

---

### 7. Machine(#) - 设备编号

**函数**: `extract_machine_number_sfc(machine: Any) -> Optional[int]`

**计算逻辑**:
```python
从machine字段中提取第一个数字序列
例如: "M001" → 1, "Q002" → 2, "769" → 769
```

**与文档对比**:
- ✅ **符合要求**：提取纯数字部分
- ⚠️ **注意**：SFC的machine字段经过验证，只包含数字或M/Q/O开头的数字

**代码位置**: 第541-553行

---

## 三、数据预处理逻辑

### 1. 字段映射 (`process_sfc_data`)

**主要处理**:
- 字段重命名（根据`sfc_mapping`配置）
- 数据类型转换（datetime, date, int, float, string）
- Operation字段标准化（补零为4位，处理N/A）
- 工序名称标准化（`standardize_operation_name`）
- machine字段验证（只保留数字或M/Q/O开头的数字）
- CheckInTime清理（N/A转为None，重命名为Checkin_SFC）

**代码位置**: 第148-268行

---

### 2. 标准时间表合并 (`merge_standard_time_sfc`)

**合并逻辑**:
- 从`SAP_Routing_*.parquet`文件读取标准时间表
- 按`CFN + Operation + Group`（如果有Group）或`CFN + Operation`匹配
- 合并字段：`OEE`, `Setup Time (h)`, `Effective Time (h)`
- OEE默认值处理：如果为空，设置为0.77

**代码位置**: 第271-374行

**注意**: `Effective Time (h)`已在`convert_standard_time.py`中计算好，计算公式：
```python
Effective Time (h) = (Machine时间（秒）或Labor时间（秒）) / 3600
```

---

### 3. 指标计算主函数 (`calculate_sfc_metrics`)

**执行顺序**:
1. 计算LT(d)
2. 计算PT(d)
3. 处理OEE默认值（确保为0.77）
4. 检查Effective Time (h)字段
5. 计算ST(d)
6. 计算DueTime和Weekend(d)
7. 计算CompletionStatus
8. 计算Machine(#)

**代码位置**: 第556-599行

---

## 四、发现的问题

### 问题1: PT(d)计算缺少StartTime备选 ✅ 已修复

**位置**: `calculate_sfc_pt`函数（第400-418行）

**问题**: 如果Checkin_SFC为空，直接返回None，但文档要求使用StartTime作为备选

**状态**: ✅ **已修复** - 已添加StartTime作为备选字段检查

---

### 问题2: CompletionStatus的8小时容差

**位置**: `calculate_sfc_completion_status`函数（第523-538行）

**问题**: 代码增加了8小时容差，但文档中没有明确说明

**建议**: 确认是否需要这个容差，或从文档中补充说明

---

### 问题3: ST(d)使用的数量字段

**位置**: `calculate_sfc_st`函数（第416-434行）

**问题**: 代码优先使用TrackOutQuantity，但文档要求使用StepInQuantity

**建议**: 确认SFC数据中哪个字段更准确，或根据业务需求调整优先级

---

### 问题4: DueTime和Weekend(d)的起始时间

**位置**: `calculate_sfc_due_time`和`calculate_sfc_weekend_days`函数

**问题**: SFC数据可能没有TrackInTime，使用Checkin_SFC代替，但这两者的业务含义可能不同

**建议**: 确认SFC数据中是否有TrackInTime字段，或Checkin_SFC是否可以作为替代

---

## 五、与MES数据处理的差异

| 项目 | MES数据 | SFC数据 |
|------|---------|---------|
| 数据源 | 单个Excel文件 | 多个Excel文件（通配符） |
| LT(d)起始时间 | 0010工序用Checkin_SFC，其他用EnterStepTime | 相同 |
| PT(d)起始时间 | 优先Checkin_SFC，否则StartTime | 仅Checkin_SFC（缺少StartTime备选） |
| DueTime起始时间 | TrackInTime | TrackInTime或Checkin_SFC |
| 数量字段 | StepInQuantity | TrackOutQuantity或StepInQuantity |
| 去重逻辑 | 基于BatchNumber+Operation+machine+TrackOutTime | 基于BatchNumber+Operation+Checkin_SFC |
| 增量处理 | 文件级 | 文件级+记录级（hash匹配） |

---

## 六、建议改进

1. **完善PT(d)计算**：添加StartTime作为Checkin_SFC的备选
2. **统一数量字段**：明确SFC数据中应使用哪个数量字段
3. **文档说明**：补充CompletionStatus的8小时容差说明
4. **字段验证**：在处理前检查关键字段是否存在，给出明确提示

---

## 七、代码执行流程

```
1. process_all_sfc_data()
   ├─ 读取SFC文件（支持通配符）
   ├─ 检查增量处理状态
   └─ 逐个文件处理：
      ├─ read_sharepoint_excel() → 读取Excel
      ├─ process_sfc_data() → 字段映射、类型转换、标准化
      ├─ merge_standard_time_sfc() → 合并标准时间表
      ├─ calculate_sfc_metrics() → 计算所有指标
      │   ├─ calculate_sfc_lt()
      │   ├─ calculate_sfc_pt()
      │   ├─ calculate_sfc_st()
      │   ├─ calculate_sfc_due_time()
      │   ├─ calculate_sfc_weekend_days()
      │   ├─ calculate_sfc_completion_status()
      │   └─ extract_machine_number_sfc()
      ├─ filter_incremental_sfc_data() → 增量过滤
      ├─ update_sfc_etl_state() → 更新状态
      └─ merge_with_history() → 与历史数据合并
```

---

## 八、关键配置

### config_sfc.yaml主要配置项：

- `source.sfc_path`: SFC数据路径（支持通配符）
- `source.standard_time_path`: 标准时间表路径（支持通配符）
- `sfc_mapping`: 字段映射配置
- `sfc_types`: 数据类型配置
- `incremental.enabled`: 是否启用增量处理
- `incremental.unique_key_fields`: 唯一键字段（用于去重）
- `deduplicate.sort_field`: 去重排序字段

---

*文档生成时间: 2024-11-06*
*代码版本: etl_sfc.py (1095行)*

