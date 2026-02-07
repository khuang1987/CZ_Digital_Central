# 智能监控体系概览

MDDAP 不仅仅是一个被动的数据展示平台，它还内置了**主动监控引擎 (Alert Engine)**，能够实时扫描业务异常并驱动闭环处理。

## 1. 核心理念：从"看报表"到"做任务"

传统的 BI 流程是用户每天查看报表发现问题。我们引入了 **A3 Trigger** 机制：

1. **Monitor (监控)**: 系统每日自动扫描 KPI 趋势。
2. **Detect (侦测)**: 如果满足预设的[触发规则](triggers.md)（如连续3周不达标）。
3. **Act (行动)**: 
    - **Warning**: 发送邮件提醒。
    - **Critical**: 自动生成 **A3 Case** 并推送到 **Planner** 任务系统。
4. **Close (闭环)**: 责任人在 Planner 中完成整改任务后，系统自动关闭异常案卷。

```mermaid
graph LR
    Data[KPI 数据] --> Engine[监控引擎]
    Rule[规则库] --> Engine
    
    Engine --包含 Warning--> Email[邮件通知]
    Engine --包含 Critical--> A3[生成 A3 Case]
    
    A3 --> Planner[Microsoft Planner\n(生成任务)]
    User[责任人] -->|处理任务| Planner
    
    Planner -->|状态同步| Engine
    Engine -->|任务完成| DB[关闭 Case]
```

## 2. 关键组件

### 2.1 A3 Trigger Case
每一个严重的异常都会被分配一个唯一的 **A3 ID** (如 `A3-20251001-001`)。这相当于一个“工单”或“案卷”，用于追踪这个异常的整个生命周期。

### 2.2 Rule Engine (规则引擎)
系统通过 `data_pipelines/monitoring/config/kpi_rules.csv` 管理规则。支持配置：
- **阈值 (Threshold)**: 如 < 95%。
- **连续性 (Consecutive)**: 如“连续 3 周”。
- **级别 (Level)**: Warning 或 Critical。

### 2.3 自动去重与抑制
为了防止报警风暴，系统内置了智能逻辑：
- **抑制冗余**: 如果同一个问题同时触发了 Warning 和 Critical，只报 Critical。
- **防止重复**: 如果一个 A3 Case 已经是 `OPEN` 状态，新的违规数据会合并到旧 Case 中，而不会生成新工单。
