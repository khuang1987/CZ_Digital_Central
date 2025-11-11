# SA指标数据清洗

## 概述

本工具用于本地清洗和计算SA指标数据，将Power Query的数据处理逻辑转换为Python实现。

**主要功能：**
- 从SharePoint同步的Excel文件读取数据
- 数据处理和清洗
- 数据匹配（合并SFC、标准时间等）
- 分组计算（TrackInTime、Setup、DueTime等指标）
- 输出为Parquet格式，直接供Power BI使用

**参考：** 80-数据清洗的处理方法，但增加了完整的数据计算逻辑

## 目录结构

```
13-SA数据清洗/
├── etl_sa.py          # MES数据ETL脚本
├── etl_sfc.py         # SFC数据ETL脚本
├── config/
│   ├── config.yaml    # MES配置文件
│   └── config_sfc.yaml # SFC配置文件
├── logs/              # 日志文件
├── publish/           # 输出文件（Parquet格式）
├── requirements.txt   # Python依赖
├── run_etl.bat        # 运行MES脚本（单独）
├── run_all_etl.bat    # 运行完整流程（推荐）
└── README.md          # 本文档
```

## 安装依赖

```bash
pip install -r requirements.txt
```

## 配置说明

### 1. 编辑配置文件

编辑 `config/config.yaml`：

#### 数据源路径
```yaml
source:
  # MES数据路径（必需）
  mes_path: "C:\\路径\\到\\MES\\数据\\Product Output*.xlsx"
  
  # SFC数据路径（可选）
  sfc_path: "C:\\路径\\到\\SFC\\数据.xlsx"
  
  # 标准时间数据路径（可选）
  standard_time_path: "C:\\路径\\到\\标准时间\\数据.xlsx"
```

#### 字段映射
如果Excel列名与标准列名不一致，需要配置映射：
```yaml
mes_mapping:
  "ERPCode": "PlantCode"
  "Material_Name": "CFN"
  # ... 其他映射
```

### 2. 输出路径

输出文件保存在 `publish/` 目录，文件命名格式：`MES_处理后数据_YYYYMMDD.parquet`

如需保存到SharePoint同步文件夹，修改配置：
```yaml
output:
  base_dir: "C:\\路径\\到\\SharePoint\\同步文件夹\\publish"
```

## 使用方法

### ⚠️ 重要：运行顺序

**MES数据处理依赖SFC数据**，因此需要按以下顺序运行：

1. **先运行SFC数据处理**：生成 `SFC_处理后数据_latest.parquet`
2. **再运行MES数据处理**：从SFC文件合并 `Checkin_SFC` 字段

### 推荐方法：使用完整流程脚本（Windows）

```bash
run_all_etl.bat
```

这个脚本会按顺序自动运行：
1. `etl_sfc.py` - 处理SFC数据
2. `etl_sa.py` - 处理MES数据（需要SFC数据）

### 方法2：单独运行脚本

如果需要单独运行：

```bash
# 1. 先运行SFC数据处理
python etl_sfc.py

# 2. 再运行MES数据处理
python etl_sa.py
```

或使用批处理文件：
```bash
run_etl.bat  # 仅运行MES（会提示需要先运行SFC）
```

**注意**：如果SFC数据文件不存在，MES处理时 `Checkin_SFC` 字段将为空，但不会报错。

## 处理流程

1. **读取数据**
   - 从配置的路径读取MES Excel文件
   - 可选：读取SFC数据和标准时间数据

2. **基础处理**（对应Power Query: e2_批次报工记录_MES_基础前处理.pq）
   - 字段映射和重命名
   - 类型转换
   - 过滤无效数据（如批次号包含"-"）
   - 工序名称标准化
   - 工序号补零

3. **分组计算**
   - 按ResourceCode分组
   - 计算TrackInTime（基于上一行的TrackOutTime）
   - 计算Setup（基于CFN连续性）

4. **数据匹配**
   - 合并SFC数据（从 `SFC_处理后数据_latest.parquet` 读取，按批次号、工序号、工序名称匹配 `Checkin_SFC`）
   - 合并标准时间数据（按CFN、Operation、Group）

5. **指标计算**（对应Power Query: e2_批次报工记录_MES_后处理.pq）
   - LT(d): Lead Time（工序周期时间）
   - PT(d): Process Time（加工周期时间）
   - ST(d): Standard Time（标准时间）
   - DueTime: 应完工时间（包含周末调整）
   - Weekend(d): 周末扣除天数
   - CompletionStatus: 完工状态
   - Machine(#): 设备编号提取

6. **保存结果**
   - 输出为Parquet格式
   - 文件保存在publish目录

## Power BI连接

### 从本地文件读取
```
获取数据 → 文件 → Parquet → 选择 publish/MES_处理后数据_YYYYMMDD.parquet
```

### 从SharePoint读取（如果publish目录同步到SharePoint）
```
获取数据 → SharePoint文件夹 → 选择SharePoint中的publish文件夹 → 筛选.parquet文件
```

## 注意事项

1. **首次使用**：
   - 确保配置文件中的路径正确
   - 根据实际Excel列名调整字段映射
   - 先测试小数据量

2. **数据更新**：
   - 每次运行会生成新的日期版本文件
   - 建议定期清理旧文件

3. **错误处理**：
   - 查看 `logs/etl_sa.log` 了解详细错误信息
   - 配置 `runtime.on_error` 控制错误时的行为

4. **性能优化**：
   - 大文件处理可能需要较长时间
   - 建议在数据量较小时先测试

## 日志

日志文件保存在 `logs/etl_sa.log`，包含：
- 处理进度
- 错误信息
- 性能统计

## 计算逻辑说明

详细的计算逻辑请参考：
- `10-SA指标/e2_批次报工记录_MES_算法说明.md`
- `10-SA指标/SA指标计算方法与定义.md`

## 计划任务

可在Windows任务计划程序中创建任务：
- **程序/脚本**：`run_all_etl.bat`（推荐，会按顺序运行SFC和MES）
- **起始于**：本目录
- **触发器**：每日/每小时

如果只运行MES，使用 `run_etl.bat`，但需要确保SFC数据已更新。
