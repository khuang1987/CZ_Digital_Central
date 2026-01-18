-- ============================================================
-- 计算层 (DWD): MES 指标计算视图 - SQL Server 版本
-- 匹配策略: CFN + Operation + Group 三字段匹配
-- ST(d) 计算按照技术文档定义：包含 OEE、SetupTime、ScrapQty、换批时间
-- ============================================================

-- 删除旧视图
IF OBJECT_ID('dbo.v_mes_metrics', 'V') IS NOT NULL
    DROP VIEW dbo.v_mes_metrics;
GO

CREATE VIEW dbo.v_mes_metrics AS
WITH
mes_with_prev AS (
    SELECT 
        m.id,
        m.BatchNumber,
        m.Operation,
        CASE
            WHEN mt._machine_trim IS NULL OR mt._machine_trim = '' THEN NULL
            ELSE UPPER(LEFT(mt._machine_trim, 1)) + SUBSTRING(mt._machine_trim, 2, 8000)
        END AS machine,
        mc.machine_code AS machine_code,
        m.EnterStepTime,
        m.TrackInTime,
        m.TrackOutTime,
        m.Plant,
        m.Product_Desc,
        m.ProductNumber,
        m.CFN,
        m.ProductionOrder,
        m.OperationDesc AS [Operation description],
        m.[Group],
        m.StepInQuantity,
        m.TrackOutQuantity,
        m.TrackOutOperator,
        m.factory_name,
        m.source_file,
        m.created_at,
        m.updated_at,
        
        -- 关联 SFC TrackIn 时间和报废数量
        s.TrackInTime AS Checkin_SFC,
        s.ScrapQty,

        -- 关联 SAP Routing 数据
        r.EH_machine AS SAP_EH_machine,
        r.EH_labor AS SAP_EH_labor,
        r.Quantity AS SAP_Quantity,
        r.SetupTime AS SAP_SetupTime,
        r.OEE AS SAP_OEE,
        
        -- 计算 PreviousBatchEndTime（使用窗口函数，按 Machine 分组）
        LAG(m.TrackOutTime) OVER (
            PARTITION BY COALESCE(mc.machine_code, mt._machine_trim)
            ORDER BY m.TrackOutTime
        ) AS PreviousBatchEndTime,
        
        -- 计算 Setup（同机台上下批次CFN是否相同）
        CASE 
            WHEN LAG(m.CFN) OVER (PARTITION BY COALESCE(mc.machine_code, mt._machine_trim) ORDER BY m.TrackOutTime) IS NULL THEN 'Yes'
            WHEN m.CFN != LAG(m.CFN) OVER (PARTITION BY COALESCE(mc.machine_code, mt._machine_trim) ORDER BY m.TrackOutTime) THEN 'Yes'
            ELSE 'No'
        END AS Setup
    FROM dbo.raw_mes m
    CROSS APPLY (SELECT LTRIM(RTRIM(m.Machine)) AS _machine_trim) mt
    CROSS APPLY (
        SELECT
            CASE
                WHEN mt._machine_trim IS NULL OR mt._machine_trim = '' THEN NULL
                WHEN CHARINDEX(' ', mt._machine_trim) = 0 THEN NULL
                WHEN CHARINDEX(' ', mt._machine_trim, CHARINDEX(' ', mt._machine_trim) + 1) = 0
                THEN SUBSTRING(mt._machine_trim, CHARINDEX(' ', mt._machine_trim) + 1, 50)
                ELSE SUBSTRING(
                    mt._machine_trim,
                    CHARINDEX(' ', mt._machine_trim) + 1,
                    CHARINDEX(' ', mt._machine_trim, CHARINDEX(' ', mt._machine_trim) + 1) - CHARINDEX(' ', mt._machine_trim) - 1
                )
            END AS _machine_token
    ) mk
    CROSS APPLY (
        SELECT
            CASE
                WHEN mk._machine_token IS NULL OR mk._machine_token = '' THEN NULL
                WHEN mk._machine_token NOT LIKE '%[^0-9]%' THEN mk._machine_token
                WHEN mk._machine_token LIKE 'M%' AND SUBSTRING(mk._machine_token, 2, 50) NOT LIKE '%[^0-9]%' THEN mk._machine_token
                WHEN mk._machine_token LIKE 'Q%' AND SUBSTRING(mk._machine_token, 2, 50) NOT LIKE '%[^0-9]%' THEN mk._machine_token
                WHEN mk._machine_token LIKE 'O%' AND SUBSTRING(mk._machine_token, 2, 50) NOT LIKE '%[^0-9]%' THEN mk._machine_token
                ELSE NULL
            END AS machine_code
    ) mc
    OUTER APPLY (
        SELECT
            MIN(rs.TrackInTime) AS TrackInTime,
            SUM(ISNULL(rs.ScrapQty, 0)) AS ScrapQty
        FROM dbo.raw_sfc rs
        WHERE rs.BatchNumber = m.BatchNumber
            AND LTRIM(RTRIM(rs.Operation)) = LTRIM(RTRIM(m.Operation))
    ) s
    OUTER APPLY (
        SELECT TOP 1
            rr.EH_machine,
            rr.EH_labor,
            rr.Quantity,
            rr.SetupTime,
            rr.OEE
        FROM dbo.raw_sap_routing rr
        WHERE rr.CFN = m.CFN
            AND rr.Operation = m.Operation
            AND rr.[Group] = m.[Group]
            AND rr.CFN IS NOT NULL AND rr.CFN != ''
            AND rr.Operation IS NOT NULL AND rr.Operation != ''
            AND rr.[Group] IS NOT NULL AND rr.[Group] != ''
        ORDER BY ISNULL(rr.updated_at, rr.created_at) DESC
    ) r
    -- 过滤子批次：排除 BatchNumber 中包含 -0 的记录（如 -001, -002 等）
    WHERE m.BatchNumber NOT LIKE '%-0%'
)

SELECT 
    id,
    BatchNumber,
    Operation,
    machine,
    CASE 
        WHEN COALESCE(machine_code, machine) IS NULL THEN NULL
        WHEN PATINDEX('%[0-9]%', COALESCE(machine_code, machine)) = 0 THEN NULL
        ELSE TRY_CONVERT(int,
            CASE 
                WHEN PATINDEX('%[^0-9]%', SUBSTRING(COALESCE(machine_code, machine), PATINDEX('%[0-9]%', COALESCE(machine_code, machine)), 50)) = 0
                THEN SUBSTRING(COALESCE(machine_code, machine), PATINDEX('%[0-9]%', COALESCE(machine_code, machine)), 50)
                ELSE SUBSTRING(
                    COALESCE(machine_code, machine),
                    PATINDEX('%[0-9]%', COALESCE(machine_code, machine)),
                    PATINDEX('%[^0-9]%', SUBSTRING(COALESCE(machine_code, machine), PATINDEX('%[0-9]%', COALESCE(machine_code, machine)), 50)) - 1
                )
            END
        )
    END AS [Machine(#)],
    EnterStepTime,
    TrackInTime,
    TrackOutTime,
    CAST(TrackOutTime AS DATE) AS TrackOutDate,
    Plant,
    Product_Desc,
    ProductNumber,
    CFN,
    ProductionOrder,
    [Operation description],
    [Group],
    StepInQuantity,
    TrackOutQuantity,
    TrackOutOperator,
    [Setup],
    SAP_SetupTime AS [Setup Time (h)],
    SAP_OEE AS OEE,
    SAP_EH_machine AS EH_machine,
    SAP_EH_labor AS EH_labor,
    factory_name,
    Checkin_SFC,
    ScrapQty,
    PreviousBatchEndTime,
    
    -- 单件工时（秒）：优先使用 EH_machine，否则使用 EH_labor
    CASE 
        WHEN SAP_EH_machine IS NOT NULL AND SAP_EH_machine > 0 THEN SAP_EH_machine
        WHEN SAP_EH_labor IS NOT NULL AND SAP_EH_labor > 0 THEN SAP_EH_labor
        ELSE NULL
    END AS unit_time,
    
    -- LT(d) 计算逻辑
    CASE 
        WHEN TrackOutTime IS NULL THEN NULL
        WHEN LTRIM(RTRIM(Operation)) IN ('0010', '10') THEN
            CASE 
                WHEN Checkin_SFC IS NOT NULL THEN 
                    ROUND(CAST(DATEDIFF(SECOND, Checkin_SFC, TrackOutTime) AS FLOAT) / 86400.0, 2)
                WHEN EnterStepTime IS NOT NULL THEN 
                    ROUND(CAST(DATEDIFF(SECOND, EnterStepTime, TrackOutTime) AS FLOAT) / 86400.0, 2)
                WHEN TrackInTime IS NOT NULL THEN 
                    ROUND(CAST(DATEDIFF(SECOND, TrackInTime, TrackOutTime) AS FLOAT) / 86400.0, 2)
                ELSE NULL
            END
        ELSE
            CASE 
                WHEN EnterStepTime IS NOT NULL THEN 
                    ROUND(CAST(DATEDIFF(SECOND, EnterStepTime, TrackOutTime) AS FLOAT) / 86400.0, 2)
                WHEN TrackInTime IS NOT NULL THEN 
                    ROUND(CAST(DATEDIFF(SECOND, TrackInTime, TrackOutTime) AS FLOAT) / 86400.0, 2)
                ELSE NULL
            END
    END AS [LT(d)],
    
    -- PT(d) 计算逻辑（按文档定义：使用 EffectiveStartTime）
    -- EffectiveStartTime 取值规则：
    -- 1. 非连续生产（EnterStepTime > PreviousBatchEndTime）：优先用 TrackInTime，否则用 PreviousBatchEndTime
    -- 2. 连续生产（EnterStepTime <= PreviousBatchEndTime）：用 PreviousBatchEndTime
    CASE 
        WHEN TrackOutTime IS NULL THEN NULL
        WHEN PreviousBatchEndTime IS NULL THEN NULL
        -- 非连续生产：有停机等待
        WHEN EnterStepTime IS NOT NULL AND EnterStepTime > PreviousBatchEndTime THEN
            ROUND(CAST(DATEDIFF(SECOND, 
                COALESCE(TrackInTime, PreviousBatchEndTime), 
                TrackOutTime) AS FLOAT) / 86400.0, 2)
        -- 连续生产：上一批结束即刻开始
        ELSE
            ROUND(CAST(DATEDIFF(SECOND, PreviousBatchEndTime, TrackOutTime) AS FLOAT) / 86400.0, 2)
    END AS [PT(d)],
    
    -- ST(d) 计算逻辑（按技术文档定义）
    -- ST(d) = (调试时间 + 加工时间 + 换批时间) / 24
    CASE 
        WHEN TrackOutQuantity IS NULL OR TrackOutQuantity <= 0 THEN NULL
        WHEN SAP_EH_machine IS NULL AND SAP_EH_labor IS NULL THEN NULL
        ELSE
            ROUND(
                (
                    -- 调试时间（小时）：Setup = 'Yes' 时加入 SetupTime
                    CASE 
                        WHEN [Setup] = 'Yes' AND SAP_SetupTime IS NOT NULL AND SAP_SetupTime > 0 
                        THEN SAP_SetupTime 
                        ELSE 0 
                    END +
                    
                    -- 加工时间（小时）= (数量 × 单件工时秒 / 3600) / OEE
                    (
                        (TrackOutQuantity + ISNULL(ScrapQty, 0)) * 
                        CASE 
                            WHEN SAP_EH_machine IS NOT NULL AND SAP_EH_machine > 0 THEN SAP_EH_machine
                            ELSE ISNULL(SAP_EH_labor, 0)
                        END / 3600.0
                    ) / ISNULL(SAP_OEE, 0.77) +
                    
                    -- 换批时间（小时）：固定 0.5 小时
                    0.5
                ) / 24.0,  -- 转换为天
                2
            )
    END AS [ST(d)],
    
    -- CompletionStatus 计算逻辑（使用 PT(d) + 8小时容差）
    CASE 
        WHEN TrackOutTime IS NULL THEN 'Incomplete'
        WHEN PreviousBatchEndTime IS NULL THEN 'NoBaseline'
        WHEN SAP_EH_machine IS NULL AND SAP_EH_labor IS NULL THEN 'NoStandard'
        WHEN TrackOutQuantity IS NULL OR TrackOutQuantity <= 0 THEN 'NoQuantity'
        
        -- 计算 PT(d)
        WHEN ROUND(CAST(DATEDIFF(SECOND, PreviousBatchEndTime, TrackOutTime) AS FLOAT) / 86400.0, 2) <= 0 THEN 'Abnormal'
        
        -- OnTime 判断：PT(d) <= ST(d) + 0.33 (8小时容差)
        WHEN ROUND(CAST(DATEDIFF(SECOND, PreviousBatchEndTime, TrackOutTime) AS FLOAT) / 86400.0, 2) 
             <= ROUND(
                    (
                        CASE WHEN Setup = 'Yes' AND SAP_SetupTime IS NOT NULL AND SAP_SetupTime > 0 THEN SAP_SetupTime ELSE 0 END +
                        ((TrackOutQuantity + ISNULL(ScrapQty, 0)) * 
                         CASE WHEN SAP_EH_machine IS NOT NULL AND SAP_EH_machine > 0 THEN SAP_EH_machine ELSE ISNULL(SAP_EH_labor, 0) END / 3600.0
                        ) / ISNULL(SAP_OEE, 0.77) +
                        0.5
                    ) / 24.0,
                    2
                ) + 0.33
             THEN 'OnTime'
        ELSE 'Overdue'
    END AS CompletionStatus

FROM mes_with_prev;
GO
