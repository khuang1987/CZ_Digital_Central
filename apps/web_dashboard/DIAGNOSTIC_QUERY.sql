-- Diagnostic Query for EHS Incidents

-- 1. Check if ANY labels exist that match the keyword '急救'
PRINT '--- SEARCHING FOR LABELS ---'
SELECT DISTINCT CleanedLabel 
FROM planner_task_labels 
WHERE CleanedLabel LIKE N'%急救%'
ORDER BY CleanedLabel;

-- 2. Check tasks that WOULD be matched by the current logic
PRINT '--- MATCHING TASKS (Label LIKE %急救%) ---'
SELECT 
    t.TaskId,
    t.TaskName,
    t.CreatedDate,
    l.CleanedLabel,
    t.BucketName,
    t.TeamName
FROM planner_tasks t
LEFT JOIN planner_task_labels l ON t.TaskId = l.TaskId
WHERE l.CleanedLabel LIKE N'%急救%'
AND t.CreatedDate >= '2024-04-27' -- Fiscal Year Start (Approx)
ORDER BY t.CreatedDate DESC;

-- 3. Check specific tasks that might be missing labels but have it in title (for reference)
PRINT '--- TASKS WITH TITLE MATCH (For Comparison) ---'
SELECT 
    t.TaskId,
    t.TaskName,
    t.CreatedDate,
    l.CleanedLabel
FROM planner_tasks t
LEFT JOIN planner_task_labels l ON t.TaskId = l.TaskId
WHERE t.TaskName LIKE N'%急救%'
AND t.CreatedDate >= '2024-04-27'
ORDER BY t.CreatedDate DESC;
