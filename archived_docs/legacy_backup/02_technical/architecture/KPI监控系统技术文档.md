# KPI监控系统技术文档

## 系统架构

```
数据源 → ETL处理 → SQLite数据库 → 矩阵生成 → 触发检测 → Power Automate → Planner任务
```

## 核心组件

### 1. 数据库表结构

#### KPI_Data
- KPI_Id: KPI标识符 (1=Lead Time, 2=Schedule Attainment, 3=Safety Issue Rank)
- Tag: 标签 (GLOBAL或安全标签名称)
- CreatedDate: 创建日期
- Progress: 进度值 (Lead Time=小时, SA=百分比, Safety=排名)

#### dim_calendar
- date: 日期
- fiscal_year: 财年
- fiscal_week: 财周

#### KPI_Definition
- Id: KPI ID
- Name: KPI名称

### 2. 输出文件

#### kpi_global_matrix.csv
数值型KPI矩阵，用于监控：
- Lead Time: 制造周期（小时）
- Schedule Attainment: 达成率（%）
- 触发条件：连续3周超过阈值

#### pareto_top_matrix.csv
柏拉图排名矩阵，用于监控：
- 各安全标签的排名（1-10）
- 触发条件：连续3周排名前3

#### kpi_trigger_results.tsv
完整触发结果，包含15个字段：
- A3Id: 唯一标识符 (格式: A3-YYYYMMDD-NNNN)
- Category: 分类 (GLOBAL或安全标签)
- TriggerType: 触发类型代码
- TriggerName: KPI名称
- TriggerLevel: 严重等级 (Critical/Warning)
- TriggerDesc: 触发描述
- ConsecutiveWeeks: 连续周数
- WeeklyDetails: 详细信息
- TriggerStatus: 触发状态 (TRIGGER)
- LastUpdate: 最后更新时间
- CaseStatus: 案例状态 (OPEN)
- IsCurrentTrigger: 是否当前触发 (Yes)
- OpenedAt: 开启日期
- ClosedAt: 关闭日期
- PlannerTaskId: Planner任务ID

### 3. 触发逻辑

#### Lead Time触发
```python
if lead_time > 24 and consecutive_weeks >= 3:
    trigger_alert()
```

#### Schedule Attainment触发
```python
if attainment < 95 and consecutive_weeks >= 3:
    trigger_alert()
```

#### Safety Issue Rank触发
```python
if rank <= 3 and consecutive_weeks >= 3:
    trigger_alert()
```

## 部署说明

### 1. 环境要求
- Python 3.8+
- pandas
- sqlite3
- 定时任务调度器（Windows Task Scheduler或cron）

### 2. 执行流程
1. 运行ETL脚本更新数据
2. 运行KPI聚合脚本
3. 运行监控脚本生成触发结果
4. Power Automate定期读取TSV文件
5. 自动创建Planner任务

### 3. 配置文件

#### kpi_rules.csv
定义KPI规则：
- RuleCode: 规则代码
- KPI_Id: KPI ID
- ThresholdValue: 阈值
- ConsecutiveOccurrences: 连续次数
- TriggerLevel: 触发级别

### 4. 关键脚本

#### generate_full_trigger_results.py
主脚本，执行：
1. 读取数据库数据
2. 生成矩阵文件
3. 检测触发条件
4. 输出TSV和CSV文件

## 维护指南

### 添加新KPI
1. 在KPI_Definition表添加新KPI
2. 在kpi_rules.csv添加规则
3. 在生成脚本添加检测逻辑

### 修改触发条件
1. 更新kpi_rules.csv中的阈值
2. 如需复杂逻辑，修改检测函数

### 故障排查
1. 检查数据库连接
2. 验证数据完整性
3. 查看脚本日志
4. 确认输出文件权限

## 性能优化

1. 使用索引优化查询
2. 批量处理数据
3. 定期清理历史数据
4. 使用增量更新

## 安全考虑

1. 数据库文件权限控制
2. 输出文件访问限制
3. Power Automate权限最小化
4. 敏感信息脱敏
