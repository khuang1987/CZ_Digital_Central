# 数据清洗与增量合并（SFC Excel → 分区化 Parquet）

## 目录结构
- config/config.yaml: 源路径、列映射、主键、分区与输出配置
- staging/: 临时与异常文件
- publish/: 产出分区数据，形如 `publish/year=YYYY/month=MM/*.parquet`
- logs/: 日志与 manifest（已处理文件清单）

## 使用步骤
1) 安装依赖（建议在公司标准 Python 环境）
```bash
pip install -r requirements.txt
```

2) 配置源路径
- 编辑 `config/config.yaml` 中的 `source.root_path`，粘贴本机 SharePoint 同步目录到 `70-SFC导出数据` 的绝对路径
- 可按需设置 `schema.mappings`、`schema.dtypes`、`deduplication.primary_key` 与 `deduplication.order_by_timestamp`

3) 本地运行
```bash
run_etl.bat
```
输出将写入 `publish/year=YYYY/month=MM/*.parquet`。

## Power BI 连接
- 在 Power BI Desktop 选择“获取数据 → Parquet”，指向 `publish/` 或某个具体分区目录
- 可选择将目录作为数据源（使用文件夹模式）以一次性加载所有分区

## 运行策略
- 每次运行只处理“新增或发生变化”的 Excel（基于 `logs/manifest.csv`）
- 出错文件会被复制到 `staging/_errors/` 并在日志中记录

## 常见配置示例
```yaml
schema:
  normalize_column_names: true
  mappings:
    lot_id: [批次号, 批次编号]
    start_time: [开始时间, 报工开始]
    end_time: [结束时间, 报工结束]
  dtypes:
    lot_id: string
    start_time: datetime64[ns]
    end_time: datetime64[ns]
  datetime:
    columns: [start_time, end_time]
    timezone: Asia/Shanghai

deduplication:
  primary_key: [lot_id]
  order_by_timestamp: end_time
```

## 计划任务
- 可在 Windows 任务计划程序中创建任务：
  - 程序/脚本：`run_etl.bat`
  - 起始于：本目录
  - 触发器：每日/每小时

## 注意
- 首次运行会扫描较多文件，建议先配置 `runtime.max_files_per_run` 小规模试跑
- 如果需要 CSV 作为输出，将 `output.format` 设置为 `csv`
