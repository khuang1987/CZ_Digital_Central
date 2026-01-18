# A1_ETL_Output 分区输出命名规范（02_CURATED_PARTITIONED）

## 目标

统一 `A1_ETL_Output/02_CURATED_PARTITIONED` 下所有“按月分区”数据集的目录结构与文件命名，确保：

- Power BI / Power Query 可稳定按目录递归读取
- 文件命名可直接表达“数据集 + 年月”
- 不再使用 `p_month=YYYY-MM/data.parquet` 这种目录结构

## 适用范围

- 目录：
  - `.../A1_ETL_Output/02_CURATED_PARTITIONED/`
- 适用对象：
  - 所有按月分区输出的数据集（MES、SFC、SAP 等）

## 目录结构规范

每个数据集使用一个独立目录（dataset folder），并按“年”建立子目录：

```
02_CURATED_PARTITIONED/
  <dataset>/
    <YYYY>/
      <prefix>_<YYYYMM>.parquet
```

- `<dataset>`：数据集目录名（例如 `mes_batch_report`、`sfc_wip_czm`）
- `<YYYY>`：4 位年份，例如 `2025`
- `<prefix>_<YYYYMM>.parquet`：按月分区文件名
  - `<prefix>`：数据集前缀（与 dataset 不一定相同）
  - `<YYYYMM>`：6 位年月，例如 `202501`

### 示例（MES）

```
02_CURATED_PARTITIONED/
  mes_batch_report/
    2025/
      mes_metrics_202501.parquet
      mes_metrics_202502.parquet
```

## 文件命名规范

- 仅输出“按月分区”文件，不输出整年汇总文件
- 文件名格式：

```
<prefix>_<YYYYMM>.parquet
```

- 示例：
  - `mes_metrics_202501.parquet`
  - `sfc_wip_czm_202512.parquet`

## Power Query（M）读取约定

Power Query 推荐：

- 使用 `SharePoint.Files(...)` + 递归读取子目录
- 通过文件名匹配 `prefix_YYYYMM.parquet`
- 从文件名解析 `p_month`（格式 `YYYY-MM`）用于后续筛选/增量刷新

### MES 示例（etl_mes_batch_output.m）

- FolderKey：`/a1_etl_output/02_curated_partitioned/mes_batch_report/`
- 选择 parquet：
  - `Extension = .parquet`
  - `Name` 以 `mes_metrics_` 开头
- `p_month` 通过 `mes_metrics_<YYYYMM>.parquet` 解析得到 `YYYY-MM`

> 注意：读取逻辑不再依赖目录名 `p_month=` 或固定文件名 `data.parquet`。

## 导出脚本实现位置

### 1) MES 专用导出

- 脚本：`scripts/_export_mes_to_parquet.py`
- 输出：`02_CURATED_PARTITIONED/mes_batch_report/<YYYY>/mes_metrics_<YYYYMM>.parquet`

### 2) 通用分区导出（其它数据集）

- 脚本：`scripts/export_core_to_a1.py`
- 规则：`02_CURATED_PARTITIONED/<dataset>/<YYYY>/<prefix>_<YYYYMM>.parquet`

#### prefix 映射

- 配置字典：`PARTITIONED_EXPORT_PREFIX`
- 默认：若未配置，则 `prefix = dataset`
- 当前已配置：
  - `mes_batch_report` -> `mes_metrics`

如需其它数据集使用不同前缀，在 `PARTITIONED_EXPORT_PREFIX` 中新增映射即可。
