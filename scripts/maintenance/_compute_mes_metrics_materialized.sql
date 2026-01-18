SET NOCOUNT ON;

GO

-------------------------------------------------------------------------------
-- 1. SFC Aggregation
--    Aggregates TrackInTime and ScrapQty by Batch + Operation.
--    Normalizes Operation to INT (handling '0010' vs '10') for joining.
-------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#sfc_agg') IS NOT NULL DROP TABLE #sfc_agg;
CREATE TABLE #sfc_agg (
    BatchNumber NVARCHAR(64) NOT NULL,
    OperationKey INT NOT NULL,
    Checkin_SFC DATETIME2 NULL,
    ScrapQty FLOAT NULL
);

GO

INSERT INTO #sfc_agg (BatchNumber, OperationKey, Checkin_SFC, ScrapQty)
SELECT
    CAST(rs.BatchNumber AS NVARCHAR(64)),
    v.OpKey,
    MIN(rs.TrackInTime),
    SUM(ISNULL(rs.ScrapQty, 0))
FROM dbo.raw_sfc rs
CROSS APPLY (
    -- Robust conversion: matches "0010" to 10. Filters out non-numeric ops.
    SELECT TRY_CONVERT(int, TRY_CONVERT(float, rs.Operation)) AS OpKey
) v
WHERE rs.BatchNumber IS NOT NULL
  AND v.OpKey IS NOT NULL
GROUP BY
    CAST(rs.BatchNumber AS NVARCHAR(64)),
    v.OpKey;

CREATE INDEX IX_sfc_agg ON #sfc_agg (BatchNumber, OperationKey);

GO

-------------------------------------------------------------------------------
-- 2. Routing Latest
--    Gets latest routing standards (EH, OEE, etc.) per CFN + Op + Group.
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

GO

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
    WHERE rr.CFN IS NOT NULL AND rr.CFN != ''
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
-- 3. Prepare Target
-------------------------------------------------------------------------------
TRUNCATE TABLE {{TARGET_TABLE}};

GO

-------------------------------------------------------------------------------
-- 4. MES Seed
--    Base MES transaction data, normalizing keys and cleaning machine names.
-------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#mes_seed') IS NOT NULL DROP TABLE #mes_seed;
CREATE TABLE #mes_seed (
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

GO

INSERT INTO #mes_seed (
    id, BatchNumber, Operation, OperationKey, GroupKey,
    machine, machine_code, machine_key,
    EnterStepTime, TrackInTime, TrackOutTime, TrackOutDate,
    Plant, Product_Desc, ProductNumber, CFN, ProductionOrder,
    OperationDesc, [Group], StepInQuantity, TrackOutQuantity,
    TrackOutOperator, factory_name, VSM
)
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
WHERE (m.BatchNumber IS NULL OR m.BatchNumber NOT LIKE '%-0%');

CREATE CLUSTERED INDEX IX_mes_seed_key_time ON #mes_seed (machine_key, TrackOutTime, id);
CREATE INDEX IX_mes_seed_batch_op ON #mes_seed (BatchNumber, OperationKey);
CREATE INDEX IX_mes_seed_cfn_op_group ON #mes_seed (CFN, OperationKey, GroupKey);

GO

-------------------------------------------------------------------------------
-- 5. Calculate Metrics & Insert
--    Joins Seed + SFC + Routing; computes LT, PT, ST, CompletionStatus.
-------------------------------------------------------------------------------
WITH mes_with_prev AS (
    SELECT
        m.*,
        s.Checkin_SFC,
        s.ScrapQty,
        r.EH_machine AS SAP_EH_machine,
        r.EH_labor AS SAP_EH_labor,
        r.SetupTime AS SAP_SetupTime,
        r.OEE AS SAP_OEE,
        -- Window functions for previous batch (setup/PT calculation)
        LAG(m.TrackOutTime) OVER (
            PARTITION BY m.machine_key
            ORDER BY m.TrackOutTime, m.id
        ) AS PreviousBatchEndTime,
        CASE
            WHEN LAG(m.CFN) OVER (PARTITION BY m.machine_key ORDER BY m.TrackOutTime, m.id) IS NULL THEN 'Yes'
            WHEN m.CFN != LAG(m.CFN) OVER (PARTITION BY m.machine_key ORDER BY m.TrackOutTime, m.id) THEN 'Yes'
            ELSE 'No'
        END AS Setup
    FROM #mes_seed m
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
    -- Try to convert machine_code to int for [Machine(#)]
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
    -- unit_time
    CASE
        WHEN SAP_EH_machine IS NOT NULL AND SAP_EH_machine > 0 THEN SAP_EH_machine
        WHEN SAP_EH_labor IS NOT NULL AND SAP_EH_labor > 0 THEN SAP_EH_labor
        ELSE NULL
    END,
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
    END,
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
    END,
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
    END,
    -- PNW(d)
    ROUND(PNW_Days, 4),
    -- LNW(d)
    ROUND(LNW_Days, 4),
    
    -- CompletionStatus
    -- Re-calculate Net PT and ST for comparison
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
    END
FROM nw_calc;


GO
