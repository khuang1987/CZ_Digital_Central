-- ============================================================
-- 计算层 (DWD): MES 指标计算视图 - SQL Server 版本
-- 匹配策略: CFN + Operation + Group 三字段匹配
-- ============================================================

-- 删除旧视图
IF OBJECT_ID('dbo.v_mes_metrics', 'V') IS NOT NULL
    DROP VIEW dbo.v_mes_metrics;
GO

CREATE VIEW dbo.v_mes_metrics AS
WITH
-- SFC 按 BatchNumber + Operation 聚合
sfc_agg AS (
    SELECT
        BatchNumber,
        LTRIM(RTRIM(Operation)) AS Operation,
        MIN(TrackInTime) AS TrackInTime,
        SUM(ISNULL(ScrapQty, 0)) AS ScrapQty
    FROM dbo.raw_sfc
    GROUP BY BatchNumber, LTRIM(RTRIM(Operation))
),

-- Routing 按 CFN + Operation + Group 去重
sap_routing_dedup AS (
    SELECT *
    FROM (
        SELECT
            r.*, 
            ROW_NUMBER() OVER (
                PARTITION BY r.CFN, LTRIM(RTRIM(r.Operation)), r.[Group]
                ORDER BY ISNULL(r.updated_at, r.created_at) DESC
            ) AS rn
        FROM dbo.raw_sap_routing r
        WHERE r.CFN IS NOT NULL AND r.CFN != ''
            AND r.Operation IS NOT NULL AND r.Operation != ''
            AND r.[Group] IS NOT NULL AND r.[Group] != ''
    ) t
    WHERE rn = 1
),

mes_with_prev AS (
    SELECT 
        m.id,
        m.BatchNumber,
        m.Operation,
        m.Machine,
        m.EnterStepTime,
        m.TrackInTime,
        m.TrackOutTime,
        m.Plant,
        m.Product_Desc,
        m.ProductNumber,
        m.CFN,
        m.ProductionOrder,
        m.OperationDesc,
        m.[Group],
        m.StepInQuantity,
        m.TrackOutQuantity,
        m.TrackOutOperator,
        m.factory_name,
        m.source_file,
        m.created_at,
        m.updated_at,
        
        -- 关联 SFC TrackIn 时间
        s.TrackInTime AS TrackIn_SFC,
        s.ScrapQty,

        -- 关联 SAP 标准时间（仅使用 CFN + Operation + Group 匹配）
        r.StandardTime AS ST_SAP,
        r.EH_machine AS SAP_EH_machine,
        r.EH_labor AS SAP_EH_labor,
        r.Quantity AS SAP_Quantity,
        r.SetupTime AS SAP_SetupTime,
        r.OEE AS SAP_OEE,
        
        -- 计算 PreviousBatchEndTime（使用窗口函数，按 Machine 分组）
        LAG(m.TrackOutTime) OVER (
            PARTITION BY m.Machine 
            ORDER BY m.TrackOutTime
        ) AS PreviousBatchEndTime,
        
        -- 计算 IsSetup（同机台上下批次CFN是否相同）
        CASE 
            WHEN LAG(m.CFN) OVER (PARTITION BY m.Machine ORDER BY m.TrackOutTime) IS NULL THEN 'Yes'
            WHEN m.CFN != LAG(m.CFN) OVER (PARTITION BY m.Machine ORDER BY m.TrackOutTime) THEN 'Yes'
            ELSE 'No'
        END AS IsSetup
    FROM dbo.raw_mes m
    LEFT JOIN sfc_agg s 
        ON m.BatchNumber = s.BatchNumber 
        AND LTRIM(RTRIM(m.Operation)) = LTRIM(RTRIM(s.Operation))
    LEFT JOIN sap_routing_dedup r
        ON m.CFN = r.CFN
        AND m.Operation = r.Operation
        AND m.[Group] = r.[Group]
)

SELECT 
    id,
    BatchNumber,
    Operation,
    Machine,
    EnterStepTime,
    TrackInTime,
    TrackOutTime,
    CAST(TrackOutTime AS DATE) AS TrackOutDate,
    Plant,
    Product_Desc,
    ProductNumber,
    CFN,
    ProductionOrder,
    OperationDesc,
    [Group],
    StepInQuantity,
    TrackOutQuantity,
    TrackOutOperator,
    IsSetup,
    SAP_SetupTime AS SetupTime,
    SAP_OEE AS OEE,
    SAP_EH_machine AS EH_machine,
    SAP_EH_labor AS EH_labor,
    factory_name,
    TrackIn_SFC,
    ScrapQty,
    ST_SAP,
    PreviousBatchEndTime,
    
    -- LT(d) 计算逻辑
    CASE 
        WHEN TrackOutTime IS NULL THEN NULL
        WHEN LTRIM(RTRIM(Operation)) IN ('0010', '10') THEN
            CASE 
                WHEN TrackIn_SFC IS NOT NULL THEN 
                    ROUND(CAST(DATEDIFF(SECOND, TrackIn_SFC, TrackOutTime) AS FLOAT) / 86400.0, 2)
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
    END AS LT_d,
    
    -- PT(d) 计算逻辑
    CASE 
        WHEN TrackOutTime IS NULL THEN NULL
        WHEN PreviousBatchEndTime IS NULL THEN NULL
        ELSE ROUND(CAST(DATEDIFF(SECOND, PreviousBatchEndTime, TrackOutTime) AS FLOAT) / 86400.0, 2)
    END AS PT_d,
    
    -- ST(d) 计算逻辑：单件时间 * 数量 / 1440（分钟转天）
    CASE 
        WHEN ST_SAP IS NOT NULL AND TrackOutQuantity IS NOT NULL AND TrackOutQuantity > 0 THEN
            ROUND((ST_SAP * TrackOutQuantity) / 1440.0, 2)
        ELSE NULL
    END AS ST_d,
    
    -- CompletionStatus 计算逻辑
    CASE 
        WHEN TrackOutTime IS NULL THEN 'Incomplete'
        
        -- 0010工序逻辑
        WHEN LTRIM(RTRIM(Operation)) IN ('0010', '10') THEN
            CASE 
                WHEN TrackIn_SFC IS NOT NULL THEN 
                    CASE 
                        WHEN ROUND(CAST(DATEDIFF(SECOND, TrackIn_SFC, TrackOutTime) AS FLOAT) / 86400.0, 2) <= 0 THEN 'Abnormal'
                        WHEN ST_SAP IS NULL THEN 'NoStandard'
                        WHEN TrackOutQuantity IS NULL OR TrackOutQuantity <= 0 THEN 'NoQuantity'
                        WHEN ROUND(CAST(DATEDIFF(SECOND, TrackIn_SFC, TrackOutTime) AS FLOAT) / 86400.0, 2) 
                             <= ROUND((ST_SAP * TrackOutQuantity) / 1440.0, 2) THEN 'OnTime'
                        ELSE 'Delayed'
                    END
                WHEN EnterStepTime IS NOT NULL THEN 
                    CASE 
                        WHEN ROUND(CAST(DATEDIFF(SECOND, EnterStepTime, TrackOutTime) AS FLOAT) / 86400.0, 2) <= 0 THEN 'Abnormal'
                        WHEN ST_SAP IS NULL THEN 'NoStandard'
                        WHEN TrackOutQuantity IS NULL OR TrackOutQuantity <= 0 THEN 'NoQuantity'
                        WHEN ROUND(CAST(DATEDIFF(SECOND, EnterStepTime, TrackOutTime) AS FLOAT) / 86400.0, 2) 
                             <= ROUND((ST_SAP * TrackOutQuantity) / 1440.0, 2) THEN 'OnTime'
                        ELSE 'Delayed'
                    END
                WHEN TrackInTime IS NOT NULL THEN 
                    CASE 
                        WHEN ROUND(CAST(DATEDIFF(SECOND, TrackInTime, TrackOutTime) AS FLOAT) / 86400.0, 2) <= 0 THEN 'Abnormal'
                        WHEN ST_SAP IS NULL THEN 'NoStandard'
                        WHEN TrackOutQuantity IS NULL OR TrackOutQuantity <= 0 THEN 'NoQuantity'
                        WHEN ROUND(CAST(DATEDIFF(SECOND, TrackInTime, TrackOutTime) AS FLOAT) / 86400.0, 2) 
                             <= ROUND((ST_SAP * TrackOutQuantity) / 1440.0, 2) THEN 'OnTime'
                        ELSE 'Delayed'
                    END
                ELSE 'NoStartTime'
            END
        
        -- 非0010工序逻辑
        ELSE
            CASE 
                WHEN EnterStepTime IS NOT NULL THEN 
                    CASE 
                        WHEN ROUND(CAST(DATEDIFF(SECOND, EnterStepTime, TrackOutTime) AS FLOAT) / 86400.0, 2) <= 0 THEN 'Abnormal'
                        WHEN ST_SAP IS NULL THEN 'NoStandard'
                        WHEN TrackOutQuantity IS NULL OR TrackOutQuantity <= 0 THEN 'NoQuantity'
                        WHEN ROUND(CAST(DATEDIFF(SECOND, EnterStepTime, TrackOutTime) AS FLOAT) / 86400.0, 2) 
                             <= ROUND((ST_SAP * TrackOutQuantity) / 1440.0, 2) THEN 'OnTime'
                        ELSE 'Delayed'
                    END
                WHEN TrackInTime IS NOT NULL THEN 
                    CASE 
                        WHEN ROUND(CAST(DATEDIFF(SECOND, TrackInTime, TrackOutTime) AS FLOAT) / 86400.0, 2) <= 0 THEN 'Abnormal'
                        WHEN ST_SAP IS NULL THEN 'NoStandard'
                        WHEN TrackOutQuantity IS NULL OR TrackOutQuantity <= 0 THEN 'NoQuantity'
                        WHEN ROUND(CAST(DATEDIFF(SECOND, TrackInTime, TrackOutTime) AS FLOAT) / 86400.0, 2) 
                             <= ROUND((ST_SAP * TrackOutQuantity) / 1440.0, 2) THEN 'OnTime'
                        ELSE 'Delayed'
                    END
                ELSE 'NoStartTime'
            END
    END AS CompletionStatus,
    
    -- 单件时间（分钟）
    ST_SAP AS unit_time
FROM mes_with_prev;
GO
