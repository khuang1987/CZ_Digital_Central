SET NOCOUNT ON;

-- INCREMENTAL REFRESH SCRIPT
-- Parameters required: {{TARGET_TABLE}}, {{MIN_ID}}

GO

-------------------------------------------------------------------------------
-- 1. Prepare New Data (Seed)
--    Normalize keys for new rows from raw_mes.
-------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#mes_seed_new') IS NOT NULL DROP TABLE #mes_seed_new;
CREATE TABLE #mes_seed_new (
    id INT NOT NULL,
    BatchNumber NVARCHAR(64) NULL,
    Operation NVARCHAR(32) NULL,
    OperationKey INT NULL,
    GroupKey BIGINT NULL,
    machine NVARCHAR(256) NULL,
    machine_code NVARCHAR(50) NULL,
    machine_key NVARCHAR(256) NULL,
    EnterStepTime DATETIME2 NULL,
    TrackInTime DATETIME2 NULL,
    TrackOutTime DATETIME2 NULL,
    TrackOutDate DATE NULL,
    Plant NVARCHAR(32) NULL,
    Product_Desc NVARCHAR(256) NULL,
    ProductNumber NVARCHAR(64) NULL,
    CFN NVARCHAR(64) NULL,
    ProductionOrder INT NULL,
    OperationDesc NVARCHAR(256) NULL,
    [Group] NVARCHAR(64) NULL,
    StepInQuantity FLOAT NULL,
    TrackOutQuantity FLOAT NULL,
    TrackOutOperator NVARCHAR(128) NULL,
    factory_name NVARCHAR(64) NULL,
    VSM NVARCHAR(64) NULL
);

INSERT INTO #mes_seed_new
SELECT
    m.id,
    CAST(m.BatchNumber AS NVARCHAR(64)),
    CAST(m.Operation AS NVARCHAR(32)),
    v.OpKey,
    v.GrpKey,
    -- Machine cleanup
    CAST(
        CASE
            WHEN mt._machine_trim IS NULL OR mt._machine_trim = '' THEN NULL
            ELSE UPPER(LEFT(mt._machine_trim, 1)) + SUBSTRING(mt._machine_trim, 2, 8000)
        END AS NVARCHAR(256)
    ),
    CAST(mc.machine_code AS NVARCHAR(50)),
    CAST(COALESCE(mc.machine_code, mt._machine_trim) AS NVARCHAR(256)),
    m.EnterStepTime,
    m.TrackInTime,
    m.TrackOutTime,
    CAST(m.TrackOutTime AS DATE),
    CAST(m.Plant AS NVARCHAR(32)),
    CAST(m.Product_Desc AS NVARCHAR(256)),
    CAST(m.ProductNumber AS NVARCHAR(64)),
    CAST(m.CFN AS NVARCHAR(64)),
    m.ProductionOrder,
    CAST(m.OperationDesc AS NVARCHAR(256)),
    CAST(m.[Group] AS NVARCHAR(64)),
    m.StepInQuantity,
    m.TrackOutQuantity,
    CAST(m.TrackOutOperator AS NVARCHAR(128)),
    CAST(m.factory_name AS NVARCHAR(64)),
    CAST(m.VSM AS NVARCHAR(64))
FROM dbo.raw_mes m
CROSS APPLY (
    SELECT
        TRY_CONVERT(int, TRY_CONVERT(float, m.Operation)) AS OpKey,
        TRY_CONVERT(bigint, TRY_CONVERT(float, m.[Group])) AS GrpKey
) v
CROSS APPLY (SELECT LTRIM(RTRIM(m.Machine)) AS _machine_trim) mt
CROSS APPLY (
    -- Extract potential machine code token (after first space)
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
    -- Validate machine token is numeric-ish
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
WHERE m.id >= {{MIN_ID}}
  AND (m.BatchNumber IS NULL OR m.BatchNumber NOT LIKE '%-0%');

-- Create index to speed up subsequent joins
CREATE INDEX IX_mes_seed_new_batch_op ON #mes_seed_new (BatchNumber, OperationKey);
CREATE INDEX IX_mes_seed_new_cfn_op_group ON #mes_seed_new (CFN, OperationKey, GroupKey);

GO

-------------------------------------------------------------------------------
-- 2. Prepare Boundary Rows
--    Fetch the latest row for each machine involved in the new batch
--    from the EXISTING snapshot table to support LAG calculations.
-------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#boundary_rows') IS NOT NULL DROP TABLE #boundary_rows;
CREATE TABLE #boundary_rows (
    id INT NOT NULL,
    BatchNumber NVARCHAR(64) NULL,
    Operation NVARCHAR(32) NULL,
    OperationKey INT NULL,
    GroupKey BIGINT NULL,
    machine NVARCHAR(256) NULL,
    machine_code NVARCHAR(50) NULL,
    machine_key NVARCHAR(256) NULL,
    EnterStepTime DATETIME2 NULL,
    TrackInTime DATETIME2 NULL,
    TrackOutTime DATETIME2 NULL,
    TrackOutDate DATE NULL,
    Plant NVARCHAR(32) NULL,
    Product_Desc NVARCHAR(256) NULL,
    ProductNumber NVARCHAR(64) NULL,
    CFN NVARCHAR(64) NULL,
    ProductionOrder INT NULL,
    OperationDesc NVARCHAR(256) NULL,
    [Group] NVARCHAR(64) NULL,
    StepInQuantity FLOAT NULL,
    TrackOutQuantity FLOAT NULL,
    TrackOutOperator NVARCHAR(128) NULL,
    factory_name NVARCHAR(64) NULL,
    VSM NVARCHAR(64) NULL
);

-- Identify machines in new data
SELECT DISTINCT machine_key INTO #new_machines FROM #mes_seed_new WHERE machine_key IS NOT NULL;

INSERT INTO #boundary_rows
SELECT
    t.id,
    t.BatchNumber,
    t.Operation,
    TRY_CONVERT(int, TRY_CONVERT(float, t.Operation)) AS OperationKey, -- Re-derive key if needed or trust existing? Snapshot doesn't store keys explicitly, need to re-derive to join #sfc_agg if we wanted to re-compute boundary (but we don't, we just need it for LAG). 
    -- Wait, we don't need to re-compute metrics for boundary rows. We just need their columns for LAG.
    -- However, we need them in the same structure as #mes_seed_new to UNION.
    TRY_CONVERT(bigint, TRY_CONVERT(float, t.[Group])) AS GroupKey,
    t.machine,
    NULL as machine_code, -- Not critical for LAG
    -- Re-construct machine_key from snapshot logic?
    -- Actually, snapshot doesn't store machine_key column explicitly? 
    -- Ah, the snapshot table structure DOES NOT have machine_key.
    -- It has `machine` and `[Machine(#)]`.
    -- We need to reconstruct `machine_key` to match #mes_seed_new logic so PARTITION BY works.
    -- Or simpler: use `machine` column if it was stored as the key?
    -- Checking `_compute_mes_metrics_materialized.sql`:
    --   machine_key is `COALESCE(machine_code, mt._machine_trim)`.
    --   The INSERT into target stores `machine` as `machine` (which is the cleaned name).
    --   It stores `[Machine(#)]` as the int code.
    --   The `machine_key` used for partitioning was transient.
    --   Re-deriving machine_key from stored `machine` + `[Machine(#)]` might be tricky if data was lost.
    --   However, for LAG, we just need to group by the "Same Machine".
    --   If `machine` column in snapshot is unique enough, we can use it.
    --   Let's check if `machine` in snapshot is `machine_trim`. Yes.
    --   And `machine_code` is extracted.
    --   If we can re-generate machine_key from the stored columns, that's best.
    --   Or we can use `machine` column as the partition key if that's what we want?
    --   The original script used `machine_key` which prioritized `machine_code` if present.
    --   Let's try to reconstruct it.
    CASE 
        WHEN t.[Machine(#)] IS NOT NULL THEN CAST(t.[Machine(#)] AS NVARCHAR(50))
        ELSE t.machine 
    END AS machine_key, -- Approximation
    t.EnterStepTime,
    t.TrackInTime,
    t.TrackOutTime,
    t.TrackOutDate,
    t.Plant,
    t.Product_Desc,
    t.ProductNumber,
    t.CFN,
    t.ProductionOrder,
    t.[Operation description],
    t.[Group],
    t.StepInQuantity,
    t.TrackOutQuantity,
    t.TrackOutOperator,
    t.factory_name,
    t.VSM
FROM {{TARGET_TABLE}} t
INNER JOIN (
    SELECT machine_key, MAX(id) as max_id
    FROM (
        SELECT 
             id,
             CASE 
                WHEN [Machine(#)] IS NOT NULL THEN CAST([Machine(#)] AS NVARCHAR(50))
                ELSE machine 
            END AS machine_key
        FROM {{TARGET_TABLE}}
    ) sub
    WHERE machine_key IN (SELECT machine_key FROM #new_machines)
    GROUP BY machine_key
) latest ON t.id = latest.max_id;

GO

-------------------------------------------------------------------------------
-- 3. Prepare SFC Aggregation (Filtered)
--    Only for batches in new rows.
-------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#sfc_agg') IS NOT NULL DROP TABLE #sfc_agg;
CREATE TABLE #sfc_agg (
    BatchNumber NVARCHAR(64) NOT NULL,
    OperationKey INT NOT NULL,
    Checkin_SFC DATETIME2 NULL,
    ScrapQty FLOAT NULL
);

INSERT INTO #sfc_agg (BatchNumber, OperationKey, Checkin_SFC, ScrapQty)
SELECT
    CAST(rs.BatchNumber AS NVARCHAR(64)),
    v.OpKey,
    MIN(rs.TrackInTime),
    SUM(ISNULL(rs.ScrapQty, 0))
FROM dbo.raw_sfc rs
CROSS APPLY (
    SELECT TRY_CONVERT(int, TRY_CONVERT(float, rs.Operation)) AS OpKey
) v
WHERE rs.BatchNumber IN (SELECT DISTINCT BatchNumber FROM #mes_seed_new)
  AND v.OpKey IS NOT NULL
GROUP BY
    CAST(rs.BatchNumber AS NVARCHAR(64)),
    v.OpKey;

CREATE INDEX IX_sfc_agg ON #sfc_agg (BatchNumber, OperationKey);

GO

-------------------------------------------------------------------------------
-- 4. Prepare Routing Latest (Filtered)
--    Only for CFNs in new rows.
-------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#routing_latest') IS NOT NULL DROP TABLE #routing_latest;
CREATE TABLE #routing_latest (
    CFN NVARCHAR(64) NOT NULL,
    OperationKey INT NOT NULL,
    GroupKey BIGINT NOT NULL,
    EH_machine FLOAT NULL,
    EH_labor FLOAT NULL,
    Quantity FLOAT NULL,
    SetupTime FLOAT NULL,
    OEE FLOAT NULL
);

WITH routing_calc AS (
    SELECT
        CAST(rr.CFN AS NVARCHAR(64)) AS CFN,
        v.OpKey,
        v.GrpKey,
        rr.EH_machine,
        rr.EH_labor,
        rr.Quantity,
        rr.SetupTime,
        rr.OEE,
        rr.updated_at,
        rr.created_at
    FROM dbo.raw_sap_routing rr
    CROSS APPLY (
        SELECT
            TRY_CONVERT(int, TRY_CONVERT(float, rr.Operation)) AS OpKey,
            TRY_CONVERT(bigint, TRY_CONVERT(float, rr.[Group])) AS GrpKey
    ) v
    WHERE rr.CFN IN (SELECT DISTINCT CFN FROM #mes_seed_new)
      AND v.OpKey IS NOT NULL
      AND v.GrpKey IS NOT NULL
),
routing_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY CFN, OpKey, GrpKey
            ORDER BY ISNULL(updated_at, created_at) DESC
        ) AS rn
    FROM routing_calc
)
INSERT INTO #routing_latest (
    CFN, OperationKey, GroupKey, EH_machine, EH_labor, Quantity, SetupTime, OEE
)
SELECT
    CFN, OpKey, GrpKey, EH_machine, EH_labor, Quantity, SetupTime, OEE
FROM routing_ranked
WHERE rn = 1;

CREATE INDEX IX_routing_latest ON #routing_latest (CFN, OperationKey, GroupKey);

GO

-------------------------------------------------------------------------------
-- 5. Calculate Metrics & Insert New Rows
-------------------------------------------------------------------------------
WITH combined_seed AS (
    SELECT * FROM #boundary_rows
    UNION ALL
    SELECT * FROM #mes_seed_new
),
mes_with_prev AS (
    SELECT
        m.*,
        s.Checkin_SFC,
        s.ScrapQty,
        r.EH_machine AS SAP_EH_machine,
        r.EH_labor AS SAP_EH_labor,
        r.SetupTime AS SAP_SetupTime,
        r.OEE AS SAP_OEE,
        -- Window functions for previous batch
        LAG(m.TrackOutTime) OVER (
            PARTITION BY m.machine_key
            ORDER BY m.TrackOutTime, m.id
        ) AS PreviousBatchEndTime,
        CASE
            WHEN LAG(m.CFN) OVER (PARTITION BY m.machine_key ORDER BY m.TrackOutTime, m.id) IS NULL THEN 'Yes'
            WHEN m.CFN != LAG(m.CFN) OVER (PARTITION BY m.machine_key ORDER BY m.TrackOutTime, m.id) THEN 'Yes'
            ELSE 'No'
        END AS Setup
    FROM combined_seed m
    LEFT JOIN #sfc_agg s
        ON s.BatchNumber = m.BatchNumber
        AND s.OperationKey = m.OperationKey
    LEFT JOIN #routing_latest r
        ON r.CFN = m.CFN
        AND r.OperationKey = m.OperationKey
        AND r.GroupKey = m.GroupKey
),
time_calc AS (
    SELECT
        m.*,
        
        -- LT Start Time Logic
        CASE
            WHEN m.OperationKey = 10 THEN
                COALESCE(m.Checkin_SFC, m.EnterStepTime, m.TrackInTime)
            ELSE
                COALESCE(m.EnterStepTime, m.TrackInTime)
        END AS LT_StartTime,
        
        -- PT Start Time Logic
        CASE
            WHEN m.TrackOutTime IS NULL THEN NULL
            WHEN m.PreviousBatchEndTime IS NULL THEN NULL
            WHEN m.EnterStepTime IS NOT NULL AND m.EnterStepTime > m.PreviousBatchEndTime THEN
                COALESCE(m.TrackInTime, m.PreviousBatchEndTime)
            ELSE
                m.PreviousBatchEndTime
        END AS PT_StartTime

    FROM mes_with_prev m
),
nw_calc AS (
    SELECT 
        tc.*,
        -- LNW Calculation
        CASE 
            WHEN tc.LT_StartTime IS NOT NULL AND tc.TrackOutTime IS NOT NULL THEN
                (cal_end.CumulativeNonWorkDays - COALESCE(cal_lt_start.CumulativeNonWorkDays, 0))
            ELSE 0 
        END AS LNW_Days,
        
        -- PNW Calculation
        CASE 
            WHEN tc.PT_StartTime IS NOT NULL AND tc.TrackOutTime IS NOT NULL THEN
                (cal_end.CumulativeNonWorkDays - COALESCE(cal_pt_start.CumulativeNonWorkDays, 0))
            ELSE 0
        END AS PNW_Days

    FROM time_calc tc
    -- Join for End Time (TrackOut)
    LEFT JOIN dbo.dim_calendar_cumulative cal_end 
        ON cal_end.CalendarDate = CAST(tc.TrackOutTime AS DATE)
        
    -- Join for LT Start
    LEFT JOIN dbo.dim_calendar_cumulative cal_lt_start
        ON cal_lt_start.CalendarDate = CAST(DATEADD(day, -1, CAST(tc.LT_StartTime AS DATE)) AS DATE)
        
    -- Join for PT Start
    LEFT JOIN dbo.dim_calendar_cumulative cal_pt_start
        ON cal_pt_start.CalendarDate = CAST(DATEADD(day, -1, CAST(tc.PT_StartTime AS DATE)) AS DATE)
)
INSERT INTO {{TARGET_TABLE}} (
    id,
    BatchNumber,
    Operation,
    machine,
    [Machine(#)],
    EnterStepTime,
    TrackInTime,
    TrackOutTime,
    TrackOutDate,
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
    [Setup Time (h)],
    OEE,
    EH_machine,
    EH_labor,
    factory_name,
    VSM,
    Checkin_SFC,
    ScrapQty,
    PreviousBatchEndTime,
    unit_time,
    [LT(d)],
    [PT(d)],
    [ST(d)],
    [PNW(d)],
    [LNW(d)],
    CompletionStatus
)
SELECT
    id,
    BatchNumber,
    Operation,
    machine,
    TRY_CONVERT(int,
        CASE
            WHEN machine_code IS NOT NULL AND machine_code NOT LIKE '%[^0-9]%' THEN machine_code
            ELSE NULL
        END
    ),
    EnterStepTime,
    TrackInTime,
    TrackOutTime,
    TrackOutDate,
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
    Setup,
    SAP_SetupTime,
    SAP_OEE,
    SAP_EH_machine,
    SAP_EH_labor,
    factory_name,
    VSM,
    Checkin_SFC,
    ScrapQty,
    PreviousBatchEndTime,
    CASE
        WHEN SAP_EH_machine IS NOT NULL AND SAP_EH_machine > 0 THEN SAP_EH_machine
        WHEN SAP_EH_labor IS NOT NULL AND SAP_EH_labor > 0 THEN SAP_EH_labor
        ELSE NULL
    END AS unit_time,
    -- LT(d) (Net)
    CASE
        WHEN LT_StartTime IS NOT NULL AND TrackOutTime IS NOT NULL THEN
            ROUND(
                CASE 
                    WHEN (CAST(DATEDIFF_BIG(SECOND, LT_StartTime, TrackOutTime) AS FLOAT) / 86400.0) - LNW_Days > 0 
                    THEN (CAST(DATEDIFF_BIG(SECOND, LT_StartTime, TrackOutTime) AS FLOAT) / 86400.0) - LNW_Days
                    ELSE 0 
                END, 
                4
            )
        ELSE NULL
    END AS [LT(d)],
    -- PT(d) (Net)
    CASE
        WHEN PT_StartTime IS NOT NULL AND TrackOutTime IS NOT NULL THEN
            ROUND(
                CASE 
                    WHEN (CAST(DATEDIFF_BIG(SECOND, PT_StartTime, TrackOutTime) AS FLOAT) / 86400.0) - PNW_Days > 0
                    THEN (CAST(DATEDIFF_BIG(SECOND, PT_StartTime, TrackOutTime) AS FLOAT) / 86400.0) - PNW_Days
                    ELSE 0
                END,
                4
            )
        ELSE NULL
    END AS [PT(d)],
    -- ST(d)
    CASE
        WHEN TrackOutQuantity IS NULL OR TrackOutQuantity <= 0 THEN NULL
        WHEN SAP_EH_machine IS NULL AND SAP_EH_labor IS NULL THEN NULL
        ELSE
            ROUND(
                (
                    CASE
                        WHEN Setup = 'Yes' AND SAP_SetupTime IS NOT NULL AND SAP_SetupTime > 0 THEN SAP_SetupTime
                        ELSE 0
                    END +
                    (
                        (TrackOutQuantity + ISNULL(ScrapQty, 0)) *
                        CASE
                            WHEN SAP_EH_machine IS NOT NULL AND SAP_EH_machine > 0 THEN SAP_EH_machine
                            ELSE ISNULL(SAP_EH_labor, 0)
                        END / 3600.0
                    ) / ISNULL(SAP_OEE, 0.77) +
                    0.5
                ) / 24.0,
                4
            )
    END AS [ST(d)],
    -- PNW(d)
    ROUND(PNW_Days, 4) AS [PNW(d)],
    -- LNW(d)
    ROUND(LNW_Days, 4) AS [LNW(d)],
    
    -- CompletionStatus
    CASE
        WHEN TrackOutTime IS NULL THEN 'Incomplete'
        WHEN PreviousBatchEndTime IS NULL THEN 'NoBaseline'
        WHEN SAP_EH_machine IS NULL AND SAP_EH_labor IS NULL THEN 'NoStandard'
        WHEN TrackOutQuantity IS NULL OR TrackOutQuantity <= 0 THEN 'NoQuantity'
        WHEN PT_StartTime IS NULL THEN 'Abnormal'
        
        -- Net PT <= ST + Tolerance
        WHEN ROUND(
                CASE 
                    WHEN (CAST(DATEDIFF_BIG(SECOND, PT_StartTime, TrackOutTime) AS FLOAT) / 86400.0) - PNW_Days > 0
                    THEN (CAST(DATEDIFF_BIG(SECOND, PT_StartTime, TrackOutTime) AS FLOAT) / 86400.0) - PNW_Days
                    ELSE 0
                END,
                4
             )
             <= ROUND(
                    (
                        CASE WHEN Setup = 'Yes' AND SAP_SetupTime IS NOT NULL AND SAP_SetupTime > 0 THEN SAP_SetupTime ELSE 0 END +
                        ((TrackOutQuantity + ISNULL(ScrapQty, 0)) *
                         CASE WHEN SAP_EH_machine IS NOT NULL AND SAP_EH_machine > 0 THEN SAP_EH_machine ELSE ISNULL(SAP_EH_labor, 0) END / 3600.0
                        ) / ISNULL(SAP_OEE, 0.77) +
                        0.5
                    ) / 24.0,
                    4
                ) + 0.33
             THEN 'OnTime'
        ELSE 'Overdue'
    END AS CompletionStatus
FROM nw_calc
WHERE id >= {{MIN_ID}};

GO
