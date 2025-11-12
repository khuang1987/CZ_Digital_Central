# SA 指标字段说明

本页面提供 SA 指标相关数据的完整字段定义、数据类型、说明和示例。

---

## 1. 基础标识字段

| 字段名 | 英文名 | 数据类型 | 说明 | 示例 | 数据来源 |
|--------|--------|----------|------|------|----------|
| 批次号 | BatchNumber | Text | 生产批次唯一标识 | K24A2066 | MES/SFC |
| 产品号 | CFN | Text | 产品编码 | 858-957-11 | MES/SFC |
| 生产订单 | ProductionOrder | Int64 | 生产订单号（整数格式） | 228032737 | MES |
| 工序号 | Operation | Text | 4位工序代码，自动补零 | 0010, 0020 | MES/SFC |
| 工序名称 | Operation description | Text | 标准化后的工序名称 | 数控铣, 数控车 | MES/SFC |
| 工艺路线 | Group | Text | 产品工艺路线分组（提取数字部分） | CZM 50210119 | MES |
| 机台号 | machine | Int64/Text | 生产设备编号（MES为文本，SFC为整数） | M001, 1 | MES/SFC |
| 生产区域 | ProductionArea | Text | 生产区域名称 | CZM 线切割 WEDM | MES |
| 产品类型 | ProductType | Text | 产品类型分类 | CZM | SFC |
| 报工人 | TrackOut_User | Text | 报工人员姓名 | 张三 | SFC |
| Check In人员 | CheckIn_User | Text | Check In人员姓名 | 李四 | SFC |
| VSM | VSM | Text | 生产主管 | Wang Li | MES |
| ERP代码 | ERPCode | Text | ERP系统代码 | 12345 | MES |
| 产品描述 | Product_Description | Text | 产品描述信息 | Implant Device | MES |

---

## 2. 时间相关字段

| 字段名 | 英文名 | 数据类型 | 说明 | 示例 | 数据来源 |
|--------|--------|----------|------|------|----------|
| 进入工序时间 | EnterStepTime | DateTime | 批次进入工序的时间 | 2025-01-15 08:30:00 | MES/SFC |
| 工序投入时间 | TrackInTime | DateTime | 工序开始加工的时间 | 2025-01-15 08:35:00 | MES |
| SFC Check In时间 | Checkin_SFC | DateTime | SFC系统的Check In时间 | 2025-01-15 08:25:00 | SFC（合并到MES） |
| 工序报工时间 | TrackOutTime | DateTime | 工序完成报工的时间 | 2025-01-15 16:30:00 | MES/SFC |
| 报工日期 | TrackOutDate | Date | 工序报工时间的日期部分 | 2025-01-15 | MES（计算得出） |
| 应完工时间 | DueTime | DateTime | 基于计划计算的应完工时间（考虑周末） | 2025-01-15 17:00:00 | 计算得出 |
| 上批结束时间 | PreviousBatchEndTime | DateTime | 同一机台上一批次的结束时间（空值保存为null） | 2025-01-15 06:00:00 | MES/SFC（计算得出） |

### 时间字段说明

!!! info "时间字段详细说明"
    - **EnterStepTime**：批次首次进入该工序的时间
    - **TrackInTime**：工序正式开始加工的时间（可能晚于进入时间）
    - **Checkin_SFC**：SFC 系统记录的 Check In 时间（优先用于 PT 计算）
    - **TrackOutTime**：工序完成并报工的时间
    - **DueTime**：基于标准时间和工作日日历计算的应完工时间

!!! warning "时间格式"
    所有时间字段格式为：`YYYY-MM-DD HH:MM:SS`（24小时制）

---

## 3. 数量相关字段

| 字段名 | 英文名 | 数据类型 | 说明 | 示例 | 数据来源 |
|--------|--------|----------|------|------|----------|
| 工序投入数量 | StepInQuantity | Int64 | 进入工序的物料数量 | 300 | MES |
| 工序产出数量 | TrackOutQuantity | Int64 | 工序完成后的产出数量 | 106 | MES/SFC |
| 报废数量 | ScrapQuantity | Int64 | 工序报废的物料数量 | 0 | SFC |

### 数量字段说明

!!! tip "数量计算规则"
    - **MES 数据 ST 计算**：使用 `StepInQuantity`（投入数量）
    - **SFC 数据 ST 计算**：使用 `TrackOutQuantity + ScrapQuantity`（产出 + 报废）

---

## 4. 标准时间参数字段

| 字段名 | 英文名 | 数据类型 | 说明 | 示例 | 数据来源 |
|--------|--------|----------|------|------|----------|
| 单件机器时间 | EH_machine(s) | Number | 单件机器加工时间（秒） | 3780 | 标准时间表（Machine/Quantity） |
| 单件人工时间 | EH_labor(s) | Number | 单件人工操作时间（秒） | 1800 | 标准时间表（Labor/Quantity） |
| 设备综合效率 | OEE | Number | 设备综合效率（0-1之间；空值或0按0.77处理） | 0.80 | 标准时间表 |
| 调试时间 | Setup Time (h) | Number | 设备调试时间（小时） | 0.50 | 标准时间表 |
| 是否调试 | Setup | Text | 是否需要进行调试（Yes/No） | Yes | MES（计算得出） |

### 标准时间参数说明

!!! info "参数来源"
    这些参数来自 **SAP Routing 标准时间表**，由以下文件合并生成：
    
    - `1303 Routing.csv`：提供 Machine、Labor 时间（秒）
    - `1303机加工清单.csv`：提供 OEE、Setup Time

!!! note "OEE 默认值"
    如果 OEE 为空或为 0，系统自动使用默认值 **0.77**（77%）

---

## 5. 计算字段

| 字段名 | 英文名 | 数据类型 | 说明 | 计算公式 |
|--------|--------|----------|------|----------|
| Lead Time (天) | LT(d) | Number | 工序实际Lead Time | 见[计算方法 - 3.1节](calculation-definition.md#31-lead-time-lt-计算) |
| Process Time (天) | PT(d) | Number | 工序实际加工时间 | 见[计算方法 - 3.2节](calculation-definition.md#32-process-time-pt-计算) |
| 标准时间 (天) | ST(d) | Number | 理论标准加工时间（天，不考虑周末） | 见[计算方法 - 3.3节](calculation-definition.md#33-标准时间-std-计算) |
| 应完工时间 | DueTime | DateTime | 基于计划计算的应完工时间（考虑周末） | 见[计算方法 - 3.4节](calculation-definition.md#34-应完工时间-duetime-计算) |
| 周末扣除天数 | Weekend(d) | Number | TrackIn到应完工之间剔除的周末天数（单位：天） | 算法自动计算 |
| 完工状态 | CompletionStatus | Text | 是否按期完工（不含容差） | OnTime / Overdue |
| 容差小时数 | Tolerance(h) | Number | 固定容差时间（8小时） | 固定值 8.0 |
| 设备编号（数字） | Machine(#) | Number | 从machine字段提取的数字部分 | 从machine字段提取 |

### 计算字段详细说明

#### LT(d) - Lead Time（天）

- **定义**：从批次进入工序到完成报工的总时间
- **计算方式**：不扣除周末
- **单位**：天（保留2位小数）
- **0010工序特殊处理**：优先使用 Checkin_SFC 作为起始时间

#### PT(d) - Process Time（天）

- **定义**：批次实际加工时间
- **计算方式**：不扣除周末
- **单位**：天（保留2位小数）
- **优先级**：优先使用 Checkin_SFC，否则使用 TrackInTime

#### ST(d) - 标准时间（天）

- **定义**：理论标准加工时间
- **包含**：调试时间 + 加工时间（考虑 OEE）
- **不包含**：周末、节假日
- **单位**：天（保留2位小数）

#### DueTime - 应完工时间

- **定义**：基于标准时间和工作日日历计算的理论完工时间
- **特点**：自动跳过非工作日（周末和法定节假日），采用24小时连续工作制
- **算法**：基于日历表逐天累加工作时间（每个工作日24小时）

#### Weekend(d) - 周末扣除天数

- **定义**：从开始时间到应完工时间之间的非工作日总天数
- **包含**：周末 + 法定节假日
- **单位**：天（保留2位小数）

#### CompletionStatus - 完工状态

- **OnTime**：TrackOutTime ≤ DueTime
- **Overdue**：TrackOutTime > DueTime

---

## 6. 工序名称标准化

系统会自动将原始工序名称标准化为以下标准工序：

| 原始工序 | 标准工序 |
|----------|----------|
| CNC Milling, 铣削 | 数控铣 |
| CNC Turning, 车削 | 数控车 |
| WEDM, Wire EDM | 线切割 |
| Sawing | 锯 |
| Chrome Plating | 镀铬 |
| Slicing | 纵切 |
| TIG Welding | 氩弧焊 |
| Marking | 打标 |
| Deep Hole Drilling | 深孔钻 |
| Laser Welding | 激光焊 |
| Benchwork | 钳工 |
| Turning | 车削 |
| Assembly | 装配 |
| Vacuum Heat Treatment | 真空热处理 |
| Heat Treatment | 热处理 |

!!! tip "标准化好处"
    统一的工序名称便于跨数据源分析和报表展示

---

## 7. 字段使用场景

### 7.1 SA 分析常用字段

```
核心字段：
- BatchNumber（批次号）
- Operation（工序号）
- CompletionStatus（完工状态）
- TrackOutTime（报工时间）
- DueTime（应完工时间）
```

### 7.2 趋势分析常用字段

```
时间维度：
- TrackOutDate（报工日期）
- Year（年）
- Month（月）
- Week（周）
```

### 7.3 根因分析常用字段

```
分析维度：
- ProductionArea（生产区域）
- Operation description（工序名称）
- machine（机台号）
- CFN（产品号）
- LT(d)（实际Lead Time）
- ST(d)（标准时间）
```

---

## 8. 数据质量说明

### 8.1 必填字段

以下字段不能为空：
- BatchNumber（批次号）
- CFN（产品号）
- Operation（工序号）
- TrackOutTime（报工时间）

### 8.2 空值处理

| 字段 | 空值处理方式 |
|------|-------------|
| Checkin_SFC | 空值时使用 EnterStepTime 或 TrackInTime |
| OEE | 空值或 0 时使用默认值 0.77 |
| Setup Time (h) | 空值时按 0 处理 |
| PreviousBatchEndTime | 第一批次为空 |

### 8.3 数据类型约束

- **时间字段**：必须为有效的日期时间格式
- **数量字段**：必须为非负整数
- **OEE**：必须在 0-1 之间
- **Setup Time**：必须为非负数

---

## 相关链接

- [计算方法与定义](calculation-definition.md) - 查看详细计算公式
- [数据源说明](data-sources.md) - 了解字段来源
- [计算示例](examples.md) - 查看实际案例

