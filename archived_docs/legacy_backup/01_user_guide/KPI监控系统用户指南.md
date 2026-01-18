# KPI监控系统用户指南

## 概述

本监控系统自动检测关键绩效指标(KPI)的异常情况，并通过Power Automate自动创建Planner任务进行跟踪。

## 文件说明

### 核心文件

1. **kpi_trigger_results.tsv** - 触发结果文件
   - 包含所有触发的警报信息
   - Power Automate读取此文件创建Planner任务
   - 包含15个字段：A3Id、Category、TriggerType等

2. **kpi_global_matrix.csv** - 数值型KPI矩阵
   - 用于监控Lead Time和Schedule Attainment等数值指标
   - 每行代表一周，每列代表一个KPI
   - 便于快速查看趋势和判断触发条件

3. **pareto_top_matrix.csv** - 柏拉图排名矩阵
   - 用于监控安全问题的排名情况
   - 每行代表一周，每列代表一个安全标签
   - 显示各安全问题的排名（1=最高优先级）

## 使用流程

### 1. 查看触发警报
- 打开 `kpi_trigger_results.csv` 查看所有当前触发的警报
- 关注状态为"OPEN"且"IsCurrentTrigger"为"Yes"的记录

### 2. 分析数值型KPI
- 打开 `kpi_global_matrix.csv`
- Lead Time > 24小时 触发警报
- Schedule Attainment < 95% 触发警报
- 查看连续异常的周数

### 3. 分析安全问题排名
- 打开 `pareto_top_matrix.csv`
- 排名1-3表示需要重点关注的安全问题
- 连续3周排名前3会触发警报
- 空白表示该周无相关数据

### 4. 跟踪处理
- Power Automate会自动为每个触发创建Planner任务
- 任务标题格式：`A3Id | Category | TriggerName`
- 任务描述包含触发详情和历史数据

## 常见问题

**Q: 为什么有些安全标签在某些周没有数据？**
A: 该周没有发生该类型的安全问题，所以没有排名数据。

**Q: 触发警报后需要做什么？**
A: 查看对应的Planner任务，分析根本原因，制定改进措施，并在完成后关闭任务。

**Q: 如何查看历史趋势？**
A: 在对应的矩阵文件中按时间顺序查看数值或排名变化。

## 联系支持

如有问题，请联系系统管理员或查看技术文档。
