# 项目编码规范

## 目录结构

### 数据管道 (data_pipelines)

```
data_pipelines/
├── sources/           # 数据来源（输入端）
│   ├── mes/           # MES数据源
│   ├── sfc/           # SFC数据源
│   └── sap/           # SAP数据源
├── output/            # 数据输出（输出端）
│   ├── mes/queries/   # MES相关Power Query代码
│   ├── sfc/queries/   # SFC相关Power Query代码
│   └── sap/queries/   # SAP相关Power Query代码
└── collectors/        # 数据采集器
```

---

## Power Query 编码规范

### 文件命名

- 格式：`{数据源}_{业务实体}_{数据类型}.m`
- 示例：`sfc_equipment_repair_records.m`

### 空值检查（防止枚举错误）

**问题**：使用 `{0}` 索引访问空表时会报错 `Expression.Error: 枚举中没有足够的元素来完成该操作`

**解决方案**：在访问索引前检查表是否为空

```powerquery
// ❌ 错误写法 - 可能导致枚举错误
FirstFile = FilteredFiles{0},
SheetData = TargetSheet{0}[Data],

// ✅ 正确写法 - 添加空值检查
Result = if Table.RowCount(FilteredFiles) = 0 
    then #table({}, {})  // 返回空表
    else let
        FirstFile = FilteredFiles{0},
        // 继续处理...
    in
        ProcessedData
```

### 多数据源合并

```powerquery
// 过滤空表后合并，避免合并空表导致的错误
TablesToMerge = List.Select({Table1, Table2}, each Table.RowCount(_) > 0),
AppendedQuery = if List.Count(TablesToMerge) = 0 
    then #table({}, {}) 
    else Table.Combine(TablesToMerge)
```

### Sheet名称兼容

```powerquery
// 同时支持多个可能的Sheet名称
TargetSheet = Table.SelectRows(
    ExcelContent, 
    each [Item] = "SheetName1" or [Item] = "SheetName2"
)
```

### 列操作安全处理

```powerquery
// 安全移除列（列可能不存在）
ColumnsToRemove = {"Column1", "Column2"},
ExistingColumns = List.Select(ColumnsToRemove, each List.Contains(Table.ColumnNames(Data), _)),
Result = if List.Count(ExistingColumns) > 0 
    then Table.RemoveColumns(Data, ExistingColumns) 
    else Data
```

---

## Python ETL 编码规范

### 性能优化

**避免使用 `apply(axis=1)`**：对大数据集逐行操作非常慢

```python
# ❌ 慢 - 逐行apply
df['hash'] = df.apply(lambda row: '|'.join(row.values), axis=1)

# ✅ 快 - 向量化操作
df_records = df[key_fields].fillna('').astype(str)
record_hash = df_records.iloc[:, 0]
for col in df_records.columns[1:]:
    record_hash = record_hash + '|' + df_records[col]
df['hash'] = record_hash
```

### 增量处理

- **文件级增量**：通过文件修改时间和大小判断是否需要重新处理
- **记录级增量**：通过唯一键哈希判断是否为新记录

### 配置文件

- 使用 `pattern` 通配符支持多文件匹配
- 示例：`pattern: "C:\\path\\to\\CMES_Product_Output_*.xlsx"`

---

## 版本记录

| 日期 | 更新内容 |
|------|----------|
| 2025-12-12 | 初始版本，添加Power Query空值检查规范 |
