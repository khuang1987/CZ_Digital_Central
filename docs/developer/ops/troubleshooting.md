# 故障排查指南

本文档帮助您快速诊断和解决常见问题。

---

## 🚨 快速诊断

### 问题分类

根据症状快速定位：

| 症状 | 可能问题 | 跳转到 |
|------|----------|--------|
| ETL 脚本运行失败 | 配置、依赖、数据问题 | [ETL 运行问题](#etl-运行问题) |
| Power BI 刷新失败 | 连接、权限、数据问题 | [Power BI 问题](#power-bi-问题) |
| 数据不正确 | 计算逻辑、数据质量 | [数据质量问题](#数据质量问题) |
| 性能慢 | 数据量、配置问题 | [性能优化](#性能优化) |

!!! note "提示"
    点击上方链接可快速跳转到对应章节，或使用页面右侧目录导航。

---

## ETL 运行问题

### 问题 1：找不到 Python 或模块

**错误信息：**
```
'python' 不是内部或外部命令
```

或

```
ModuleNotFoundError: No module named 'pandas'
```

**解决方法：**

1. **安装 Python 3.8+**
   ```bash
   python --version
   ```

2. **安装依赖**
   ```bash
   pip install -r requirements.txt
   ```

3. **使用虚拟环境（推荐）**
   ```bash
   python -m venv venv
   venv\Scripts\activate
   pip install -r requirements.txt
   ```

---

### 问题 2：找不到输入文件

**错误信息：**
```
FileNotFoundError: [Errno 2] No such file or directory: 'xxxxx.xlsx'
```

**排查步骤：**

1. **检查配置文件**
   
   打开 `config/config.yaml`：
   ```yaml
   input:
     mes_folder: "路径/30-MES"
     sfc_folder: "路径/70-SFC"
   ```

2. **检查文件是否存在**
   ```bash
   # 检查目录内容
   dir "路径\30-MES"
   ```

3. **检查文件名模式**
   - MES: `Product Output*.xlsx`
   - SFC: `LC-*.csv` 或 `.xlsx`
   - Routing: `1303 Routing*.xlsx`

4. **检查权限**
   - SharePoint 是否已同步？
   - 本地文件夹是否有读取权限？

**解决方法：**

- 更新配置文件中的路径
- 确保文件已下载并放在正确位置
- 检查文件名是否匹配模式

---

### 问题 3：内存不足

**错误信息：**
```
MemoryError: Unable to allocate array
```

**排查步骤：**

1. **检查数据量**
   ```python
   import pandas as pd
   df = pd.read_excel('input.xlsx')
   print(f"记录数: {len(df)}")
   print(f"内存占用: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
   ```

2. **检查可用内存**
   - 任务管理器 → 性能 → 内存

**解决方法：**

1. **分块处理**
   
   修改配置启用分块：
   ```yaml
   processing:
     chunk_size: 10000  # 每次处理1万条
   ```

2. **增量更新**
   ```yaml
   incremental_update: true
   incremental_days: 30  # 只处理最近30天
   ```

3. **升级硬件**
   - 至少 8GB RAM
   - 推荐 16GB RAM

---

### 问题 4：日期解析错误

**错误信息：**
```
ValueError: time data '2025/01/10' does not match format '%Y-%m-%d'
```

**原因：**
- 日期格式不一致
- Excel 日期格式问题

**解决方法：**

1. **检查原始数据**
   
   打开 Excel 查看日期格式

2. **修改配置**
   
   在 `config.yaml` 中指定日期格式：
   ```yaml
   date_format: "%Y/%m/%d"  # 或其他格式
   ```

3. **清理数据**
   
   使用 Excel 统一日期格式为 `YYYY-MM-DD`

---

### 问题 5：工作日日历缺失

**错误信息：**
```
FileNotFoundError: '日历工作日表.csv' not found
```

**解决方法：**

生成日历文件：

```bash
python generate_calendar.py --year 2025 --year 2026
```

检查生成的文件：

```csv
日期,星期几,是否工作日,节假日名称
2025-01-01,星期三,False,元旦
2025-01-02,星期四,True,
...
```

---

## Power BI 问题

### 问题 1：无法连接到 SharePoint

**错误信息：**
```
DataSource.Error: Web.Contents failed to get contents from 'https://...'
```

**排查步骤：**

1. **检查网络连接**
   - 能否打开 SharePoint 网站？
   - 公司 VPN 是否连接？

2. **检查权限**
   - 是否有访问 SharePoint 文件夹的权限？
   - 尝试在浏览器中打开文件

3. **检查认证**
   - Power BI: 数据源设置 → 编辑凭据
   - 选择"组织帐户"认证

**解决方法：**

1. **重新认证**
   ```
   文件 → 选项和设置 → 数据源设置
   → 选择 SharePoint
   → 编辑权限 → 编辑
   → 使用组织帐户登录
   ```

2. **使用本地文件路径**（临时方案）
   
   在 Power Query 中修改路径：
   ```
   Source = Parquet.Document(File.Contents("C:\本地路径\MES_处理后数据_latest.parquet"))
   ```

---

### 问题 2：刷新超时

**错误信息：**
```
Timeout expired. The timeout period elapsed prior to completion.
```

**解决方法：**

1. **增加超时时间**
   
   Power BI Desktop:
   ```
   文件 → 选项和设置 → 选项
   → 数据加载
   → 命令超时（分钟）: 30
   ```

2. **使用增量刷新**
   
   参考：[增量刷新方案](../pq/incremental-refresh.md)

3. **优化查询**
   - 减少不必要的列
   - 使用 Parquet 而非 Excel
   - 启用查询折叠

---

### 问题 3：数据类型错误

**错误信息：**
```
Expression.Error: We cannot convert the value "xxx" to type Number
```

**原因：**
- 数据类型不匹配
- 空值或异常值

**解决方法：**

1. **检查数据类型**
   
   Power Query:
   ```
   右键列 → 更改类型 → 选择正确类型
   ```

2. **处理空值**
   ```m
   = Table.ReplaceValue(
       Source,
       null,
       0,  // 或其他默认值
       Replacer.ReplaceValue,
       {"列名"}
   )
   ```

3. **清理异常值**
   ```m
   = Table.SelectRows(
       Source,
       each [数量字段] <> null and [数量字段] >= 0
   )
   ```

---

### 问题 4：数据未更新

**症状：**
- Power BI 显示的是旧数据
- 刷新成功但数据不变

**排查步骤：**

1. **检查 Parquet 文件**
   - 文件时间戳是否最新？
   - 文件大小是否正常？

2. **检查 Power BI 连接**
   - 是否连接到正确的文件？
   - 是否使用 `_latest.parquet`？

3. **检查数据筛选**
   - 是否有日期筛选器？
   - 筛选器范围是否包含最新数据？

**解决方法：**

1. **强制刷新**
   ```
   Power BI Desktop: 主页 → 刷新
   Power BI Service: 刷新数据集
   ```

2. **清除缓存**
   ```
   文件 → 选项和设置 → 选项
   → 数据加载 → 清除缓存
   ```

3. **重新加载数据源**
   ```
   Power Query 编辑器 → 主页 → 刷新预览
   ```

---

## 数据质量问题

### 问题 1：SA 达成率异常

**症状：**
- SA 达成率过高或过低
- 所有批次都是 OnTime 或 Overdue

**排查步骤：**

1. **检查 DueTime 计算**
   ```python
   # 读取数据
   df = pd.read_parquet('MES_处理后数据_latest.parquet')
   
   # 检查 DueTime
   print(df[['BatchNumber', 'TrackInTime', 'DueTime', 'TrackOutTime']].head())
   
   # 检查逻辑
   df['预期状态'] = df['TrackOutTime'] <= df['DueTime']
   df['实际状态'] = df['CompletionStatus'] == 'OnTime'
   print(df[df['预期状态'] != df['实际状态']])
   ```

2. **检查工作日日历**
   ```python
   calendar = pd.read_csv('日历工作日表.csv')
   print(calendar.head())
   print(f"工作日比例: {calendar['是否工作日'].mean():.2%}")
   ```

3. **检查标准时间参数**
   ```python
   print(df[['CFN', 'Operation', 'EH_machine(s)', 'OEE', 'Setup Time (h)']].describe())
   ```

**解决方法：**

- 更新工作日日历
- 检查标准时间表
- 重新运行 ETL

---

### 问题 2：缺失字段值

**症状：**
- Checkin_SFC 大量为空
- 标准时间参数缺失

**排查步骤：**

1. **统计缺失情况**
   ```python
   df = pd.read_parquet('MES_处理后数据_latest.parquet')
   
   # 缺失统计
   print(df.isnull().sum())
   print(f"\nCheckin_SFC缺失率: {df['Checkin_SFC'].isnull().mean():.2%}")
   print(f"OEE缺失率: {df['OEE'].isnull().mean():.2%}")
   ```

2. **检查原始数据**
   - SFC 数据是否完整？
   - 标准时间表是否包含所有产品？

**解决方法：**

1. **补充 SFC 数据**
   - 导出最新 SFC 数据
   - 运行 `etl_sfc.py`
   - 重新运行 `etl_sa.py`

2. **补充标准时间**
   - 更新 `1303 Routing及机加工产品清单.xlsx`
   - 运行 `convert_standard_time.py`
   - 重新运行 `etl_sa.py`

---

### 问题 3：异常值

**症状：**
- LT 或 PT 异常大或小
- 负数数量
- 未来日期

**排查方法：**

```python
df = pd.read_parquet('MES_处理后数据_latest.parquet')

# 检查异常值
print("LT 异常:")
print(df[df['LT(d)'] > 365])  # 超过1年
print(df[df['LT(d)'] < 0])    # 负数

print("\n数量异常:")
print(df[df['StepInQuantity'] < 0])

print("\n日期异常:")
print(df[df['TrackOutTime'] > pd.Timestamp.now()])
```

**解决方法：**

1. **数据清洗**
   
   在 ETL 脚本中添加验证：
   ```python
   # 过滤异常值
   df = df[
       (df['LT(d)'] >= 0) & 
       (df['LT(d)'] <= 365) &
       (df['StepInQuantity'] >= 0)
   ]
   ```

2. **修正原始数据**
   - 检查 MES 导出是否正确
   - 修正时间字段

---

## 性能优化

### 问题 1：ETL 处理太慢

**优化方法：**

1. **使用增量更新**
   ```yaml
   incremental_update: true
   incremental_days: 7
   ```

2. **并行处理**
   ```yaml
   processing:
     parallel: true
     workers: 4  # CPU核心数
   ```

3. **优化代码**
   - 使用向量化操作
   - 避免循环
   - 使用 Parquet 而非 Excel

---

### 问题 2：Power BI 刷新太慢

**优化方法：**

1. **增量刷新**（最有效）
   
   参考：[增量刷新方案](../pq/incremental-refresh.md)

2. **减少列**
   
   在 Power Query 中删除不用的列

3. **使用 DirectQuery**（适用于大数据集）
   
   而不是 Import 模式

4. **优化数据模型**
   - 删除不必要的关系
   - 使用计算列而非度量值（适当时）

---

## 日志分析

### 查看 ETL 日志

```bash
# 查看最新日志
type logs\etl_sa.log

# 查找错误
findstr "ERROR" logs\etl_sa.log

# 查找警告
findstr "WARNING" logs\etl_sa.log
```

### 日志级别

- **INFO**: 正常信息
- **WARNING**: 警告（不影响运行）
- **ERROR**: 错误（影响部分功能）
- **CRITICAL**: 严重错误（程序终止）

---

## 获取支持

如果以上方法都无法解决问题：

### 1. 收集信息

- 错误信息截图
- 日志文件：`logs/etl_sa.log`
- 配置文件：`config/config.yaml`
- 数据样本（脱敏后）

### 2. 联系支持

- **技术支持团队**
- **CZ Ops 数字化团队**

### 3. 提供详细描述

- 问题现象
- 重现步骤
- 已尝试的解决方法
- 系统环境（Python 版本、操作系统等）

---

## 常用调试技巧

### Python 调试

```python
# 添加调试输出
import pandas as pd

df = pd.read_parquet('input.parquet')
print(f"记录数: {len(df)}")
print(f"列名: {df.columns.tolist()}")
print(f"前5行:\n{df.head()}")
print(f"数据类型:\n{df.dtypes}")
print(f"缺失值:\n{df.isnull().sum()}")
```

### Power Query 调试

```m
// 查看每一步结果
let
    Source = ...,
    Step1 = ...,
    _ = Text.From(Table.RowCount(Step1)),  // 调试：显示行数
    Step2 = ...
in
    Step2
```

---

## 相关链接

- [数据更新流程](data-update.md)
- [常见问题 FAQ](faq.md)
- [ETL 配置说明](../etl/configuration.md)

