# PowerBI增量刷新配置指南

## 🚀 快速解决方案

### 方案1: 使用分区数据（推荐⭐）

#### 步骤1: 创建数据分区
```bash
# 运行分区脚本
cd "07_工具脚本"
python ../01_核心ETL程序/create_incremental_partitions.py
```

#### 步骤2: 在PowerBI中使用分区数据
在Power Query编辑器中，用以下代码替换原有数据源：

```powerquery
let
    // 读取分区元数据
    GetPartitionMetadata = (partitionPath as text) =>
        let
            JsonContent = Json.Document(File.Contents(partitionPath & "/partition_metadata.json")),
            partitions = JsonContent[partitions],
            partitionList = Record.FieldValues(partitions)
        in
            partitionList,
    
    // 获取最新分区数据
    GetLatestPartitions = (partitionPath as text, daysBack as number) =>
        let
            allPartitions = GetPartitionMetadata(partitionPath),
            recentPartitions = List.Sort(
                List.Select(allPartitions, each (DateTime.LocalNow() - DateTime.FromText(_[date]) <= #duration(daysBack, 0, 0, 0))),
                {{"date", Order.Descending}}
            ),
            partitionData = List.Transform(
                recentPartitions,
                each Parquet.Document(File.Contents(partitionPath & "/" & _[file]))
            ),
            combinedData = Table.Combine(partitionData)
        in
            combinedData,
    
    // 主数据源 - 只获取最近7天数据
    Source = GetLatestPartitions("你的分区目录路径", 7),
    #"Changed Type" = Table.TransformColumnTypes(Source, {{"TrackOutDate", type date}})
in
    #"Changed Type"
```

**效果**: 数据加载时间减少80%+，从108,121行减少到约20,000行（最近7天）

---

### 方案2: 智能刷新调度器

#### 步骤1: 启动智能调度器
```bash
# 安装依赖
pip install schedule

# 启动调度器
cd "07_工具脚本"
python smart_refresh_scheduler.py
```

#### 步骤2: 配置定时刷新
调度器会：
- 每4小时检查数据变化
- 数据变化>5%时自动执行ETL
- 每天8:00和18:00强制检查
- 自动创建增量分区

---

### 方案3: PowerBI原生增量刷新

#### 步骤1: 准备增量数据
确保数据包含：
- 日期字段（TrackOutDate）
- 主键字段（BatchNumber + Operation + machine）

#### 步骤2: 在PowerBI Service中配置
1. 上传报表到PowerBI Service
2. 进入数据集设置
3. 启用"增量刷新"
4. 配置刷新策略：
   - 增量刷新周期：每天
   - 保留历史数据：30天
   - 标识日期列：TrackOutDate

---

## 📊 性能对比

| 方案 | 加载时间 | 内存占用 | 数据量 | 实施难度 |
|------|----------|----------|--------|----------|
| 全量刷新 | 5-10分钟 | 高 | 108,121行 | 低 |
| 分区数据 | 1-2分钟 | 低 | 20,000行 | 中 |
| 智能调度 | 30秒 | 极低 | 变化量 | 高 |

## 🎯 推荐配置

### 日常使用（推荐）
```powerquery
Source = GetLatestPartitions("分区路径", 7)  // 最近7天
```

### 月度报告
```powerquery
Source = GetLatestPartitions("分区路径", 30)  // 最近30天
```

### 年度分析
```powerquery
Source = GetLatestPartitions("分区路径", 365)  // 最近1年
```

## 🔧 维护建议

1. **每日**: 智能调度器自动运行
2. **每周**: 检查分区文件大小
3. **每月**: 清理超过90天的旧分区
4. **每季**: 重新评估刷新策略

## 📞 故障排除

### 问题1: 分区文件不存在
**解决**: 运行 `create_incremental_partitions.py` 重新创建分区

### 问题2: 数据加载缓慢
**解决**: 减少daysBack参数，只加载必要的时间范围

### 问题3: 内存不足
**解决**: 使用分区数据 + 减少加载天数

---

## ⚡ 立即开始

1. **快速体验**: 运行分区脚本，在PowerBI中使用最近3天数据
2. **生产环境**: 启动智能调度器，配置PowerBI增量刷新
3. **长期优化**: 根据使用情况调整刷新策略

选择适合您需求的方案开始优化！
