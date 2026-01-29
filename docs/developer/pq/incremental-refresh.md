# Power Query 增量刷新方案

本文档详细说明如何配置 Power BI / Power Query 的增量刷新以优化性能。

## 1. 原理
利用 `RangeStart` 和 `RangeEnd` 参数过滤数据，仅刷新最近变更的数据分区。

## 2. 配置步骤

### 2.1 设置参数
在 Power Query 编辑器中创建两个 `DateTime` 类型的参数：
- `RangeStart`: `2024-01-01 00:00:00`
- `RangeEnd`: `2024-12-31 00:00:00`

### 2.2 应用过滤
对主时间列（如 `TrackOutTime`）应用过滤器：
```powerquery
= Table.SelectRows(Source, each [TrackOutTime] >= RangeStart and [TrackOutTime] < RangeEnd)
```

### 2.3 配置增量策略
在 Power BI Desktop 中：
1. 右键表 -> **增量刷新**。
2. 开启增量刷新。
3. 设置：
   - 存档数据：2 年
   - 增量刷新：7 天

## 3. 注意事项
- 必须使用支持查询折叠的数据源（SQL Server 支持，CSV/Excel 不支持）。
- 本地 Excel 无法真正实现服务端增量刷新，但可减少 PBI 服务端的处理量。
