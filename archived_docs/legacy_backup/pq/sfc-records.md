# SFC 报工记录查询

SFC 数据 Power Query 代码（占位符页面）

---

## 基本查询

```m
let
    Source = Parquet.Document(
        Web.Contents("SharePoint路径/SFC_处理后数据_latest.parquet")
    )
in
    Source
```

---

完整内容待补充。

