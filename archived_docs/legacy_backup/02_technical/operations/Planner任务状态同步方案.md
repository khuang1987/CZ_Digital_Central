# Power Automate - Planner任务状态同步方案

## 方案概述

当Planner中的A3任务被关闭时，需要同步更新本地触发系统的状态。以下是实现方案：

## 方案一：Power Automate导出任务状态（推荐）

### 流程设计

1. **触发器：定时触发**
   - 频率：每30分钟
   - 时间：整点和半点

2. **获取Planner任务列表**
   - 使用 "List tasks" 动作
   - 筛选条件：只获取已完成的任务

3. **筛选A3相关任务**
   - 过滤标题包含 "A3-" 的任务
   - 检查完成时间

4. **导出状态文件**
   - 生成CSV文件到指定目录
   - 包含字段：Id, Title, Status, CompletedDateTime

5. **触发本地同步脚本**
   - 使用Power Automate Desktop或Windows任务计划

### Power Automate Flow配置

```yaml
Flow Name: Planner-Task-Status-Export

Trigger:
  Type: Recurrence
  Interval: 30
  Time zone: +08:00

Actions:
  1. List tasks (Planner)
     - Group ID: [指定Group ID]
     - Plan ID: [指定Plan ID]
  
  2. Filter array
     - From: List tasks output
     - Condition: 
       - Title contains 'A3-'
       - PercentComplete equals 100
  
  3. Select
     Map:
       - Id: item()?['id']
       - Title: item()?['title']
       - Status: 'Completed'
       - CompletedDateTime: item()?['completedDateTime']
  
  4. Create CSV table
     - From: Select output
  
  5. Create file (OneDrive/SharePoint)
     - Folder Path: /data_pipelines/monitoring/input/
     - File Name: planner_task_status.csv
     - File Content: CSV table output
```

## 方案二：Microsoft Graph API（技术性强）

### 前置要求
1. Azure AD应用注册
2. Graph API权限：Tasks.ReadWrite
3. 认证配置

### Python实现示例
```python
from msgraph import GraphServiceClient
from azure.identity import ClientSecretCredential

class PlannerGraphAPI:
    def __init__(self, tenant_id, client_id, client_secret):
        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )
        self.client = GraphServiceClient(credential)
    
    def get_completed_tasks(self, group_id, plan_id):
        """获取已完成的任务"""
        tasks = self.client.planner.plans[plan_id].tasks.get()
        completed = []
        
        for task in tasks.value:
            if task.percent_complete == 100 and "A3-" in task.title:
                completed.append({
                    'Id': task.id,
                    'Title': task.title,
                    'CompletedDateTime': task.completed_date_time.isoformat()
                })
        
        return completed
```

## 方案三：数据库直接存储（最简单）

### 修改Power Automate创建任务流程

在创建Planner任务时，同时更新数据库：

1. **创建任务后获取Task ID**
2. **更新本地数据库**
   ```sql
   UPDATE TriggerCaseRegistry 
   SET PlannerTaskId = '@{outputs('Create_task')?['body/id']}',
       LastUpdate = datetime('now')
   WHERE A3Id = '@{items('Apply_to_each')?['A3Id']}'
   ```

3. **定期检查状态**
   - 使用Graph API检查任务状态
   - 或等待Power Automate在任务完成时触发

## 推荐实施方案

### 第一阶段：基础实现
1. 使用Power Automate导出任务状态到CSV
2. 本地Python脚本读取CSV并更新状态
3. 集成到每日批处理中

### 第二阶段：优化改进
1. 实现Graph API直接查询
2. 添加实时状态更新
3. 实现双向同步

## 文件位置

- 导出位置：`data_pipelines/monitoring/input/planner_task_status.csv`
- 同步脚本：`planner_task_sync.py`
- 日志文件：`data_pipelines/monitoring/logs/planner_sync.log`
