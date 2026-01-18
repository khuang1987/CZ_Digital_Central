# Power BI + Power Automate 实施指南

## 方案架构
```
SQL Lite → Python脚本 → CSV文件 → Power BI → 数据警报 → Power Automate → Planner
```

## 第一步：Power BI 报告创建

### 1.1 导入数据
1. 打开 Power BI Desktop
2. 点击"获取数据" → "文本/CSV"
3. 选择文件：`kpi_trigger_results.csv`
4. 重复步骤导入：
   - `kpi_category_stats.csv`
   - `kpi_summary.csv`

### 1.2 创建视觉对象

#### 视觉对象1：触发状态卡片
1. 选择"卡片图"
2. 字段：TriggerStatus
3. 格式设置：
   - 数据标签：大
   - 条件格式：
     - 值 = "TRIGGER" → 红色
     - 值 = "OK" → 绿色

#### 视觉对象2：触发分类表格
1. 选择"表格"
2. 列：
   - Category（分类）
   - TriggerType（触发类型）
   - TriggerLevel（严重等级）
   - TriggerDesc（触发描述）
   - ConsecutiveWeeks（连续周数 - 仅适用于持续高发）
   - WeeklyDetails（详情）
   - CaseStatus（OPEN/CLOSED/REOPENED）
   - IsCurrentTrigger（Yes/No）
   - A3Id（唯一键，用于Planner去重）
3. 排序：按 TriggerLevel 降序 (Critical > High > Medium)

#### 视觉对象3：按类型统计
1. 选择"环形图"
2. 图例：TriggerType
3. 值：Category (计数)

#### 视觉对象4：统计矩阵
1. 选择"矩阵"
2. 行：Category
3. 值：TotalCount
4. 条件格式：
   - IsTriggered = "Yes" → 浅红色背景

#### 视觉对象5：KPI指标
1. 选择"切片器"
2. 字段：TriggerStatus
3. 选择"卡片图"
4. 度量值：
```dax
Trigger Count = 
CALCULATE(
    COUNTROWS('kpi_trigger_results'),
    'kpi_trigger_results'[TriggerStatus] = "TRIGGER"
)
```

### 1.3 （可选）数据警报
当前采用方案 B（无需 Power BI 警报）。如需备用方案，可按以下度量创建警报：
```dax
OpenTriggers =
CALCULATE(
    COUNTROWS('kpi_trigger_results'),
    'kpi_trigger_results'[CaseStatus] IN {"OPEN","REOPENED"},
    'kpi_trigger_results'[IsCurrentTrigger] = "Yes"
)
```
在 Power BI Service 卡片上添加警报即可（可留空不配）。

### 1.4 发布报告
1. 点击"文件" → "发布" → "发布到Power BI"
2. 选择工作区
3. 等待发布完成

## 第二步：设置自动刷新

### 2.1 在Power BI Service中设置
1. 打开发布的报告
2. 点击"数据集" → "设置"
3. "计划刷新"：
   - 保留你的数据
   - 刷新频率：每天
   - 时间：07:05（比Python脚本晚5分钟）
   - 时区：选择你的时区
4. 保存

### 2.2 配置网关（如果需要）
如果CSV在本地：
1. 安装Power BI网关
2. 配置数据源
3. 测试连接

## 第三步：Power Automate 流程（方案 B，无需 Power BI 警报）
**Flow name（建议英文）：** `KPI-Trigger-CsvToPlanner`

### 3.1 详细步骤（含首次配置提示）
1. **Trigger：Recurrence（定时）**  
   - Interval：15 或 30 分钟（按需）  
   - Time zone：选本地时区  
   - Start time：可留空（立即按间隔运行）；如需指定，填 ISO 时间例如 `2025-12-19T07:00:00`

2. **Get file content**（OneDrive/SharePoint，指向 `kpi_trigger_results.tsv`）
   - 建议使用 .tsv 文件以避免 CSV 中逗号/引号解析问题。
   - 确认返回的是“File content”（二进制）。

3. **Initialize variable**  
   - Name: `rows`  
   - Type: Array  
   - Value (Expression, do not use quotes):
   ```text
   split(replace(replace(base64ToString(body('Get_file_content')?['$content']),'\r\n','\n'),'﻿',''),'\n')
   ```

4. **Filter array** (过滤空行或无 Tab 的行)
   - From: `variables('rows')`
   - Condition: 
     - Value: `length(split(item(), decodeUriComponent('%09')))` (Expression)
     - Operator: `is greater than`
     - Value: `1`

5. **Select**（将 TSV 行转对象，From 用 `@{skip(body('Filter_array'),1)}` 跳过表头）  
   在 “Peek code” 替换 map（注意用 `decodeUriComponent('%09')` 代表 Tab）：  
   ```json
   "map": {
     "A3Id": "@{trim(split(item(), decodeUriComponent('%09'))[0])}",
     "Category": "@{trim(split(item(), decodeUriComponent('%09'))[1])}",
     "TriggerType": "@{trim(split(item(), decodeUriComponent('%09'))[2])}",
     "TriggerName": "@{trim(split(item(), decodeUriComponent('%09'))[3])}",
     "TriggerLevel": "@{trim(split(item(), decodeUriComponent('%09'))[4])}",
     "TriggerDesc": "@{split(item(), decodeUriComponent('%09'))[5]}",
     "ConsecutiveWeeks": "@{trim(split(item(), decodeUriComponent('%09'))[6])}",
     "WeeklyDetails": "@{split(item(), decodeUriComponent('%09'))[7]}",
     "TriggerStatus": "@{trim(split(item(), decodeUriComponent('%09'))[8])}",
     "LastUpdate": "@{trim(split(item(), decodeUriComponent('%09'))[9])}",
     "CaseStatus": "@{trim(split(item(), decodeUriComponent('%09'))[10])}",
     "IsCurrentTrigger": "@{trim(split(item(), decodeUriComponent('%09'))[11])}",
     "OpenedAt": "@{trim(split(item(), decodeUriComponent('%09'))[12])}",
     "ClosedAt": "@{trim(split(item(), decodeUriComponent('%09'))[13])}",
     "PlannerTaskId": "@{trim(split(item(), decodeUriComponent('%09'))[14])}"
   }
   ```

6. **Filter array 2**（筛选符合条件的触发）  
   - From：Select 的输出  
   - Expression：  
   ```text
   @and(
     or(
       equals(item()?['CaseStatus'],'OPEN'),
       equals(item()?['CaseStatus'],'REOPENED')
     ),
     equals(item()?['IsCurrentTrigger'],'Yes')
   )
   ```

7. **Apply to each**（输入：Filter array 2 输出）  
   1) **List tasks (Planner)** 同一 Group/Plan，获取现有任务  
   2) **Filter array 3**（去重）：From 用 `body('List_tasks')?['value']`，表达式：  
      ```text
      @contains(item()?['title'], items('Apply_to_each')?['A3Id'])
      ```  
   3) **Condition**：`length(body('Filter_array_3'))` > 0  
      - If Yes：跳过（已有同 A3Id 任务）  
      - If No：  
        a. **Create a task (Planner)**  
           - Title: `@{items('Apply_to_each')['A3Id']} | @{items('Apply_to_each')['Category']} | @{items('Apply_to_each')['TriggerName']}`  
           - Bucket Id: 选择指定的 Bucket（如“需要升级 TIER 4”）。若动态需查 Bucket Id。  
           - Due Date: `@{addDays(utcNow(),3)}`（或自定）  
           - Start Date Time：`@{utcNow()}`  
        b. **Update task details**（写描述 & checklist）  
           - Task Id: 来自 Create a task 的输出  
           - Description/Notes（含换行）：  
             ```text
             @{concat(
               items('Apply_to_each')['TriggerDesc'],
               '\n',
               items('Apply_to_each')['WeeklyDetails']
             )}
             ```  
           - Checklist：在 “Checklist” 字段点击 "Switch to input entire array" (如有) 或使用以下 JSON 数组格式（注意 isChecked 必须是 **false** 布尔值，不能是字符串）：  
             ```json
             [
               { "id": "1", "title": "Assess trigger", "isChecked": false },
               { "id": "2", "title": "Confirm A3 and assign owner", "isChecked": false }
             ]
             ```  
         c. （可选）通知：邮件/Teams，包含 A3Id/Category/TriggerDesc。  
            - 邮件示例（Outlook Send an email (V2)）：  
              - To：相关负责团队/个人  
              - Subject：`[KPI Alert] New A3 Trigger Created: @{items('Apply_to_each')['A3Id']} - @{items('Apply_to_each')['Category']}`  
              - Body（HTML）：  
                ```html
                <p>A3Id: @{items('Apply_to_each')['A3Id']}</p>
                <p>Category: @{items('Apply_to_each')['Category']}</p>
                <p>Trigger: @{items('Apply_to_each')['TriggerDesc']}</p>
                <p>Weekly Details:<br>@{items('Apply_to_each')['WeeklyDetails']}</p>
                <p>链接：Planner 任务已创建，可在同 Plan 查看</p>
                ```  

7. **常见问题/防错**
   - 找不到 Recurrence：点击 “Skip” 进入编辑器后，右上 “+ Add trigger” 搜索 Recurrence（Built-in → Schedule）。
   - Description 字段不在 Create a task：需额外加 **Update task details** 动作写描述。
   - CSV 末尾空行报错：可在 Filter array 增加条件 `length(item()?['A3Id']) > 0`。
   - 去重要点：A3Id 作为唯一键，List tasks + contains(A3Id) 做去重；如任务量大，建议改用 SharePoint/Dataverse 索引表按 A3Id 查重。

> 去重要点：A3Id 作为唯一键，List tasks + contains(A3Id) 做去重；无 Description 字段的动作需用 “Update task details” 写描述。

## 第四步：自动化数据更新

### 4.1 方案A：使用 Python 自动更新服务 (推荐)
 这是一个轻量级的 Python 后台服务，适合在个人电脑或服务器上运行。

 1. **安装依赖**
    ```powershell
    pip install -r requirements.txt
    ```

 2. **启动服务 (单次运行)**
    ```powershell
    python "data_pipelines/monitoring/etl/etl_kpi_aggregation.py"
    ```

 3. **调整每日运行时间（可选）**
    建议使用 Windows 任务计划程序定期运行上述脚本。

### 4.2 方案B：创建Windows任务计划（运行一次导出）
1. 打开"任务计划程序"
2. 创建基本任务
3. 名称：KPI数据更新
4. 触发器：每天 07:00
5. 操作：启动程序
   - 程序：python
   - 参数："data_pipelines/monitoring/etl/etl_kpi_aggregation.py"
   - 起始位置：项目根目录

### 4.3 方案C：Power Automate Desktop
 如果允许安装：
 1. 创建桌面流
 2. 设置定时触发
 3. 运行Python脚本
 4. 上传到OneDrive

## 第五步：测试流程

### 5.1 单元测试
1. **Python脚本测试**
   ```bash
   python "data_pipelines/monitoring/etl/etl_kpi_aggregation.py"
   ```

2. **Power BI测试**
   - 刷新数据
   - 验证警报条件
   - 手动触发警报

3. **Power Automate测试**
   - 使用测试数据
   - 验证每个步骤
   - 检查Planner任务创建

### 5.2 集成测试
1. 修改测试数据（让新分类触发）
2. 运行完整流程
3. 验证所有组件协同工作

## 优势总结

### 技术优势
- ✅ 无需Azure权限
- ✅ 绕过DLP限制
- ✅ 使用标准Microsoft组件
- ✅ 可视化展示丰富

### 业务优势
- ✅ 易于理解和维护
- ✅ 可扩展性强
- ✅ 审计追踪完整
- ✅ 用户体验良好

## 注意事项

1. **时序问题**
   - Python脚本：07:00
   - Power BI刷新：07:05
   - 确保顺序正确

2. **文件路径**
   - 保持CSV文件路径不变
   - 使用绝对路径避免问题

3. **权限管理**
   - Power BI需要访问CSV位置
   - Power Automate需要相关服务权限

4. **监控建议**
   - 设置Power Automate失败通知
   - 定期检查Power BI刷新历史
   - 监控Python脚本执行日志

## 扩展功能

### 未来可以添加
1. **多KPI类型支持**
   - 在Python脚本中添加更多触发条件
   - Power BI添加更多视觉对象

2. **智能通知**
   - 根据分类严重程度发送不同通知
   - 添加Teams通知

3. **自动报告**
   - 生成周报/月报
   - 发送给管理层

4. **移动端支持**
   - Power BI移动应用
   - 推送通知
