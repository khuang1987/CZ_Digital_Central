# MES 报工记录查询

本页面说明 Power BI 中 MES 数据的 Power Query 代码。

---

## 基本查询

```m
let
    // 从 SharePoint 读取 Parquet 文件
    Source = Parquet.Document(
        Web.Contents("https://yoursharepoint.com/.../MES_处理后数据_latest.parquet")
    ),
    
    // 设置数据类型（通常自动识别）
    #"Changed Type" = Table.TransformColumnTypes(Source,{
        {"BatchNumber", type text},
        {"CFN", type text},
        {"Operation", type text},
        {"TrackOutTime", type datetime},
        {"DueTime", type datetime},
        {"LT(d)", type number},
        {"PT(d)", type number},
        {"ST(d)", type number},
        {"CompletionStatus", type text}
    })
in
    #"Changed Type"
```

---

## 增量刷新版本

参见：[增量刷新方案](incremental-refresh.md)

---

## 相关链接

- [Power Query 概述](index.md)
- [数据源说明](../sa/data-sources.md)

