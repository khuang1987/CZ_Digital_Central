# 增量刷新方案

本页面介绍如何在 Power BI 中配置增量刷新以优化性能。

---

## 为什么需要增量刷新？

### 问题

- **全量刷新慢**：每次刷新所有历史数据
- **资源消耗大**：内存和时间消耗高
- **不必要**：历史数据不会改变

### 解决方案

**增量刷新**：只刷新最近的数据（如最近90天），历史数据保留不动

---

## 配置步骤

### 1. 创建日期参数

在 Power Query 中创建两个参数：

```m
// RangeStart 参数
RangeStart = #datetime(2024, 1, 1, 0, 0, 0) meta [IsParameterQuery=true, Type="DateTime"]

// RangeEnd 参数
RangeEnd = #datetime(2025, 12, 31, 23, 59, 59) meta [IsParameterQuery=true, Type="DateTime"]
```

### 2. 修改查询使用参数

```m
let
    Source = Parquet.Document(
        Web.Contents("SharePoint路径/MES_处理后数据_latest.parquet")
    ),
    
    // 使用参数筛选数据
    FilteredRows = Table.SelectRows(Source, 
        each [TrackOutTime] >= RangeStart and [TrackOutTime] < RangeEnd
    )
in
    FilteredRows
```

### 3. 配置增量刷新策略

在 Power BI Desktop：

1. 右键表 → **增量刷新**
2. 勾选 **启用增量刷新**
3. 配置策略：
   - **存档数据**：存储 2 年的数据
   - **增量刷新数据**：刷新最近 90 天
   - 勾选 **在刷新前获取最新数据**

---

## 策略示例

### 策略 1：日常使用

```
存档数据: 2 年
增量刷新: 90 天
```

- 保留 2 年历史
- 每次只刷新最近 90 天

### 策略 2：频繁更新

```
存档数据: 1 年
增量刷新: 30 天
```

- 保留 1 年历史
- 每次只刷新最近 30 天

### 策略 3：最小刷新

```
存档数据: 6 个月
增量刷新: 7 天
```

- 保留 6 个月历史
- 每次只刷新最近 7 天

---

## 性能对比

| 场景 | 全量刷新 | 增量刷新 |
|------|----------|----------|
| 数据量 | 100万条 | 3万条 |
| 刷新时间 | 10 分钟 | 1 分钟 |
| 内存使用 | 2GB | 500MB |

---

## 注意事项

### 1. 仅 Power BI Service 支持

- Power BI Desktop 可配置，但不执行
- 发布到 Service 后才生效

### 2. 需要 Premium 或 Pro 工作区

- 个人工作区不支持
- 需要高级容量

### 3. 数据源要求

- 支持 Parquet、SQL 等
- 不支持 Excel（建议转 Parquet）

---

## 最佳实践

1. **合理设置时间范围**
   - 不要太短（频繁刷新浪费）
   - 不要太长（性能提升不明显）
   - 推荐：30-90 天

2. **使用 Parquet 格式**
   - 比 Excel/CSV 快 5-10 倍
   - 支持高效筛选

3. **定期维护**
   - 监控刷新失败
   - 调整策略

---

## 相关链接

- [Power Query 概述](index.md)
- [数据更新流程](../guide/data-update.md)
- [性能优化章节](../guide/troubleshooting.md#性能优化)

