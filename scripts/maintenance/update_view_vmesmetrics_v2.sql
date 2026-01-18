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
        SUM(COALESCE(ScrapQty, 0)) AS ScrapQty
    FROM dbo.raw_sfc
    GROUP BY BatchNumber, LTRIM(RTRIM(Operation))
),

-- Routing 按业务键去重
sap_routing_dedup AS (
    SELECT *
    FROM (
        SELECT
            r.*, 
            ROW_NUMBER() OVER (
                PARTITION BY r.CFN, LTRIM(RTRIM(r.Operation)), r.[Group]
                ORDER BY COALESCE(r.updated_at, r.created_at) DESC
            ) AS rn
        FROM dbo.raw_sap_routing r
    ) t
    WHERE rn = 1
),

sap_routing_dedup_by_product AS (
    SELECT *
    FROM (
        SELECT
            r.*, 
            ROW_NUMBER() OVER (
                PARTITION BY r.ProductNumber, LTRIM(RTRIM(r.Operation)), r.[Group]
                ORDER BY COALESCE(r.updated_at, r.created_at) DESC
            ) AS rn
        FROM dbo.raw_sap_routing r
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
        m.VSM,
        m.source_file,
        m.created_at,
        m.updated_at,
        s.TrackInTime AS TrackIn_SFC,
        s.ScrapQty,
        COALESCE(r1.StandardTime, r2.StandardTime) AS ST_SAP,
        COALESCE(r1.EH_machine, r2.EH_machine) AS SAP_EH_machine,
        COALESCE(r1.EH_labor, r2.EH_labor) AS SAP_EH_labor,
        COALESCE(r1.Quantity, r2.Quantity) AS SAP_Quantity,
        COALESCE(r1.SetupTime, r2.SetupTime) AS SAP_SetupTime,
        COALESCE(r1.OEE, r2.OEE) AS SAP_OEE,
        LAG(m.TrackOutTime) OVER (
            PARTITION BY m.Machine 
            ORDER BY m.TrackOutTime
        ) AS PreviousBatchEndTime,
        CASE 
            WHEN LAG(m.CFN) OVER (PARTITION BY m.Machine ORDER BY m.TrackOutTime) IS NULL THEN 'Yes'
            WHEN m.CFN != LAG(m.CFN) OVER (PARTITION BY m.Machine ORDER BY m.TrackOutTime) THEN 'Yes'
            ELSE 'No'
        END AS IsSetup
    FROM dbo.raw_mes m
    LEFT JOIN sfc_agg s 
        ON m.BatchNumber = s.BatchNumber 
        AND LTRIM(RTRIM(m.Operation)) = LTRIM(RTRIM(s.Operation))
    LEFT JOIN sap_routing_dedup r1
        ON m.CFN = r1.CFN
        AND m.Operation = r1.Operation
        AND m.[Group] = r1.[Group]
    LEFT JOIN sap_routing_dedup_by_product r2
        ON r1.CFN IS NULL
        AND m.ProductNumber = r2.ProductNumber
        AND m.Operation = r2.Operation
        AND m.[Group] = r2.[Group]
),

-- 计算基础时间点 (Start Times)
metrics_time_basis AS (
    SELECT
        *,
        -- LT Start Time Rule
        CASE 
            WHEN LTRIM(RTRIM(Operation)) = '0010' OR LTRIM(RTRIM(Operation)) = '10' THEN
                COALESCE(TrackIn_SFC, EnterStepTime, TrackInTime)
            ELSE
                EnterStepTime
        END AS LT_StartTime,
        
        -- PT Start Time Rule
        CASE 
            WHEN EnterStepTime IS NOT NULL AND PreviousBatchEndTime IS NOT NULL 
                 AND CAST(EnterStepTime - PreviousBatchEndTime AS FLOAT) > 0 THEN
                -- Gap exists: Use TrackInTime if available
                COALESCE(TrackInTime, PreviousBatchEndTime)
            ELSE
                -- Continuous: Use PreviousBatchEndTime
                COALESCE(PreviousBatchEndTime, TrackInTime)
        END AS PT_StartTime
    FROM mes_with_prev
),

-- 计算非工作日扣除 (Non-Working Deduction)
metrics_calc_nw AS (
    SELECT
        m.*,
        
        -- 辅助字段: 日期部分
        CAST(m.LT_StartTime AS DATE) AS D_LT_Start,
        CAST(m.PT_StartTime AS DATE) AS D_PT_Start,
        CAST(m.TrackOutTime AS DATE) AS D_End,
        
        -- 关联日历信息
        clt.IsWorkday AS LT_Start_IsWork,
        clt.CumulativeNonWorkDays AS LT_Start_CumNW,
        
        cpt.IsWorkday AS PT_Start_IsWork,
        cpt.CumulativeNonWorkDays AS PT_Start_CumNW,
        
        cend.IsWorkday AS End_IsWork,
        cend.CumulativeNonWorkDays AS End_CumNW
        
    FROM metrics_time_basis m
    LEFT JOIN dbo.dim_calendar_cumulative clt ON CAST(m.LT_StartTime AS DATE) = clt.CalendarDate
    LEFT JOIN dbo.dim_calendar_cumulative cpt ON CAST(m.PT_StartTime AS DATE) = cpt.CalendarDate
    LEFT JOIN dbo.dim_calendar_cumulative cend ON CAST(m.TrackOutTime AS DATE) = cend.CalendarDate
),

metrics_final_calc AS (
    SELECT
        *,
        -- 计算 LNW (秒)
        CASE 
            WHEN LT_StartTime IS NULL OR TrackOutTime IS NULL OR D_LT_Start IS NULL OR D_End IS NULL THEN 0
            WHEN D_LT_Start = D_End THEN
                -- 同一天: 如果是NW，则全部时长为NW；否则0
                CASE WHEN LT_Start_IsWork = 0 THEN DATEDIFF(SECOND, LT_StartTime, TrackOutTime) ELSE 0 END
            ELSE
                -- 跨天: Start部分 + 中间整天 + End部分
                (CASE WHEN LT_Start_IsWork = 0 THEN (86400 - DATEDIFF(SECOND, CAST(LT_StartTime AS DATE), LT_StartTime)) ELSE 0 END)
                + (CASE WHEN End_IsWork = 0 THEN DATEDIFF(SECOND, CAST(TrackOutTime AS DATE), TrackOutTime) ELSE 0 END)
                + (
                    (COALESCE(End_CumNW, 0) - (CASE WHEN End_IsWork = 0 THEN 1 ELSE 0 END)) 
                    - COALESCE(LT_Start_CumNW, 0)
                  ) * 86400
        END AS LNW_Sec,
        
        -- 计算 PNW (秒)
        CASE 
            WHEN PT_StartTime IS NULL OR TrackOutTime IS NULL OR D_PT_Start IS NULL OR D_End IS NULL THEN 0
            WHEN D_PT_Start = D_End THEN
                CASE WHEN PT_Start_IsWork = 0 THEN DATEDIFF(SECOND, PT_StartTime, TrackOutTime) ELSE 0 END
            ELSE
                (CASE WHEN PT_Start_IsWork = 0 THEN (86400 - DATEDIFF(SECOND, CAST(PT_StartTime AS DATE), PT_StartTime)) ELSE 0 END)
                + (CASE WHEN End_IsWork = 0 THEN DATEDIFF(SECOND, CAST(TrackOutTime AS DATE), TrackOutTime) ELSE 0 END)
                + (
                    (COALESCE(End_CumNW, 0) - (CASE WHEN End_IsWork = 0 THEN 1 ELSE 0 END)) 
                    - COALESCE(PT_Start_CumNW, 0)
                  ) * 86400
        END AS PNW_Sec
    FROM metrics_calc_nw
),

metrics_days AS (
    SELECT 
        id, BatchNumber, Operation, Machine, EnterStepTime, TrackInTime, TrackOutTime,
        CAST(TrackOutTime AS DATE) AS TrackOutDate,
        Plant, Product_Desc, ProductNumber, CFN, ProductionOrder, OperationDesc, [Group],
        StepInQuantity, TrackOutQuantity, TrackOutOperator, IsSetup,
        SAP_SetupTime AS SetupTime, SAP_OEE AS OEE, SAP_EH_machine AS EH_machine, SAP_EH_labor AS EH_labor,
        factory_name, VSM, TrackIn_SFC, ScrapQty, ST_SAP, PreviousBatchEndTime,
        
        -- V2 新增字段
        ROUND(CAST(LNW_Sec AS FLOAT) / 86400.0, 4) AS [LNW(d)],
        ROUND(CAST(PNW_Sec AS FLOAT) / 86400.0, 4) AS [PNW(d)],
        
        -- V2 LT(d) = Gross LT - LNW
        CASE 
            WHEN TrackOutTime IS NULL OR LT_StartTime IS NULL THEN NULL
            ELSE ROUND(
                CASE WHEN CAST(TrackOutTime - LT_StartTime AS FLOAT) - (CAST(LNW_Sec AS FLOAT) / 86400.0) < 0 THEN 0
                     ELSE CAST(TrackOutTime - LT_StartTime AS FLOAT) - (CAST(LNW_Sec AS FLOAT) / 86400.0)
                END, 2
            )
        END AS [LT(d)],
        
        -- V2 PT(d) = Gross PT - PNW
        CASE 
            WHEN TrackOutTime IS NULL OR PT_StartTime IS NULL THEN NULL
            ELSE ROUND(
                CASE WHEN CAST(TrackOutTime - PT_StartTime AS FLOAT) - (CAST(PNW_Sec AS FLOAT) / 86400.0) < 0 THEN 0
                     ELSE CAST(TrackOutTime - PT_StartTime AS FLOAT) - (CAST(PNW_Sec AS FLOAT) / 86400.0)
                END, 2
            )
        END AS [PT(d)],
        
        -- ST(d) (保持原逻辑)
        CASE 
            WHEN SAP_EH_machine IS NULL AND SAP_EH_labor IS NULL THEN NULL
            ELSE ROUND(
                (
                    CASE WHEN IsSetup = 'Yes' AND SAP_SetupTime IS NOT NULL THEN SAP_SetupTime ELSE 0 END
                    + (COALESCE(TrackOutQuantity, 0) + COALESCE(ScrapQty, 0))
                        * (COALESCE(SAP_EH_machine, SAP_EH_labor) / COALESCE(NULLIF(SAP_Quantity, 0), 1))
                        / 3600.0 / COALESCE(NULLIF(SAP_OEE, 0), 0.77)
                    + 0.5
                ) / 24.0, 
                2
            )
        END AS [ST(d)],
        
        source_file, created_at, updated_at
    FROM metrics_final_calc
)

SELECT
    d.*,
    (8.0 / 24.0) AS [Tolerance(d)],
    CASE
        WHEN d.[PT(d)] IS NULL OR d.[ST(d)] IS NULL OR d.[ST(d)] <= 0 THEN NULL
        WHEN d.[PT(d)] > d.[ST(d)] + (8.0 / 24.0) THEN 'Overdue'
        ELSE 'OnTime'
    END AS CompletionStatus,
    CASE
        WHEN d.[PT(d)] IS NULL OR d.[ST(d)] IS NULL OR d.[ST(d)] <= 0 THEN NULL
        WHEN d.[PT(d)] > d.[ST(d)] + (8.0 / 24.0) THEN 1
        ELSE 0
    END AS IsOverdue,
    CASE
        WHEN d.[PT(d)] IS NULL OR d.[ST(d)] IS NULL OR d.[ST(d)] <= 0 THEN NULL
        WHEN d.[PT(d)] > d.[ST(d)] + (8.0 / 24.0) THEN 0
        ELSE 1
    END AS IsOnTime
FROM metrics_days d;
GO
