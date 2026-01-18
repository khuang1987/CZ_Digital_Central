# SA指标CSV模板字段说明

## 文件位置
`10-SA指标/SA指标模板.csv`

## 字段详细说明

### 1. 基础标识字段

| 字段名 | 英文名 | 数据类型 | 说明 | 示例 |
|--------|--------|----------|------|------|
| BatchNumber | 批次号 | Text | 生产批次唯一标识 | K24A2066 |
| CFN | 产品号 | Text | 产品编码 | 858-957-11 |
| ProductionOrder | 生产订单 | Text | 生产订单号 | 228032737 |
| Operation | 工序号 | Text | 4位工序代码，自动补零 | 0010 |
| OperationDescription | 工序名称 | Text | 标准化后的工序名称 | 数控铣 |
| Group | 工艺路线 | Text | 产品工艺路线分组 | CZM 50210119 |
| ResourceCode | 资源代码 | Text | 生产设备编号 | M001 |
| ProductionArea | 生产区域 | Text | 生产区域名称 | CZM 线切割 WEDM |

### 2. 时间相关字段

| 字段名 | 英文名 | 数据类型 | 说明 | 示例 |
|--------|--------|----------|------|------|
| EnterStepTime | 进入工序时间 | DateTime | 批次进入工序的时间 | 2025-01-15 08:30:00 |
| StepInTime | 工序投入时间 | DateTime | 工序开始加工的时间 | 2025-01-15 08:35:00 |
| StartTime | 工序开工时间 | DateTime | 实际开始加工的时间 | 2025-01-15 08:40:00 |
| TrackOutTime | 工序报工时间 | DateTime | 工序完成报工的时间 | 2025-01-15 16:30:00 |
| CheckinSFC | SFC Check In时间 | DateTime | SFC系统的Check In时间 | 2025-01-15 08:25:00 |
| DueTime | 应完工时间 | DateTime | 基于计划计算的应完工时间 | 2025-01-15 17:00:00 |
| TrackOutDate | 报工日期 | Date | 工序报工时间的日期部分 | 2025-01-15 |

### 3. 数量相关字段

| 字段名 | 英文名 | 数据类型 | 说明 | 示例 |
|--------|--------|----------|------|------|
| StepInQuantity | 工序投入数量 | Number | 进入工序的物料数量 | 300 |
| TrackOutQuantity | 工序产出数量 | Number | 工序完成后的产出数量 | 106 |
| StepDuration | 工序持续时间 | Number | 工序实际持续时间（分钟） | 219939 |

### 4. 标准时间参数字段

| 字段名 | 英文名 | 数据类型 | 说明 | 示例 |
|--------|--------|----------|------|------|
| Machine(h) | 机器时间 | Number | 单件机器加工时间（小时，保留2位小数） | 1.05 |
| Labor(h) | 人工时间 | Number | 单件人工操作时间（小时，保留2位小数） | 0.50 |
| OEE | 设备综合效率 | Percentage | 设备综合效率（百分比；空值或0按77%处理） | 0.80 |
| SetupTime(h) | 调试时间 | Number | 设备调试时间（小时） | 0.50 |

### 5. 计算字段

| 字段名 | 英文名 | 数据类型 | 说明 | 计算公式 |
|--------|--------|----------|------|----------|
| LT(d) | Lead Time (天) | Number | 工序实际Lead Time | TrackOutTime - EnterStepTime (或CheckinSFC) |
| PT(d) | Process Time (天) | Number | 工序实际加工时间 | TrackOutTime - StartTime (优先CheckinSFC) |
| ST(d) | 标准时间 (天) | Number | 理论标准加工时间（天） | (SetupTime + 数量×EffectiveTime/OEE) / 24 |
| CompletionStatus | 完工状态 | Text | 是否按期完工 | OnTime/Overdue |
| Weekend(d) | 周末扣除天数 | Number | TrackIn到应完工之间剔除的周末整天数 | 计算得出 |
| Machine(#) | 设备编号（数字） | Number | 从ResourceCode提取的数字部分 | 提取数字 |
| Setup | 换批标识 | Text | 是否需要换批 | Yes/No |
| EffectiveTime(h) | 有效时间 | Number | 有效加工时间 | Machine(h) 或 Labor(h) |
| SA状态 | SA状态 | Number | 单个批次SA判断 | 1(按期)/0(逾期) |
| SA达成率 | SA达成率 | Percentage | 时间范围SA统计 | 按期批次数/总批次数 |

## 数据填写说明

### 1. 必填字段
- BatchNumber: 批次号不能为空
- CFN: 产品号不能为空  
- Operation: 工序号不能为空
- TrackOutTime: 报工时间不能为空

### 2. 计算字段规则
- **LT(d)**: 0010工序使用CheckinSFC，其他工序使用EnterStepTime
- **PT(d)**: 优先使用CheckinSFC，否则使用StartTime
- **ST(d)**: 优先使用Machine(h)，否则使用Labor(h)；OEE默认77%
- **CompletionStatus**: TrackOutTime <= DueTime 为OnTime，否则为Overdue
- **SA状态**: CompletionStatus为OnTime时为1，否则为0

### 3. 数据格式要求
- 时间字段: YYYY-MM-DD HH:MM:SS
- 日期字段: YYYY-MM-DD
- 数值字段: 保留2位小数
- 百分比字段: 0.00-1.00格式

### 4. 工序名称标准化
系统会自动将原始工序名称标准化为以下标准工序：
- 数控铣、数控车、线切割、锯、镀铬、纵切、氩弧焊、打标、深孔钻、激光焊、钳工、车削、装配、真空热处理、热处理

## 使用建议

1. **数据完整性**: 确保必填字段完整，避免空值影响计算
2. **时间一致性**: 确保时间字段的逻辑关系正确
3. **数量验证**: 检查投入数量和产出数量的合理性
4. **标准时间**: 定期更新标准时间参数，确保计算准确性
5. **SA分析**: 使用SA状态和SA达成率进行多维度分析

## 注意事项

- 模板文件仅包含字段标题，需要填入实际数据
- 计算字段可以通过Power Query或Excel公式自动计算
- 建议定期备份数据，确保数据安全
- 异常数据需要及时处理，避免影响整体分析结果


