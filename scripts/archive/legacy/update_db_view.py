import sqlite3
from pathlib import Path
import time

DB_PATH = Path(r'C:\Users\huangk14\OneDrive - Medtronic PLC\Huangkai Files\B1_Project\250418_MDDAP_project\data_pipelines\database\mddap_v2.db')

SQL_DROP_VIEW = "DROP VIEW IF EXISTS v_mes_metrics;"

# Copying the definition from init_schema_v2.sql
SQL_CREATE_VIEW = """
CREATE VIEW v_mes_metrics AS
WITH
-- SFC 按 BatchNumber + Operation 聚合，避免 join 造成 raw_mes 行被重复放大
sfc_agg AS (
    SELECT
        BatchNumber,
        TRIM(Operation) AS Operation,
        MIN(TrackInTime) AS TrackInTime,
        SUM(COALESCE(ScrapQty, 0)) AS ScrapQty
    FROM raw_sfc
    GROUP BY BatchNumber, TRIM(Operation)
),

-- Routing 按业务键去重（保留最新一条）
sap_routing_dedup AS (
    SELECT *
    FROM (
        SELECT
            r.*, 
            ROW_NUMBER() OVER (
                PARTITION BY r.CFN, TRIM(r.Operation), r."Group"
                ORDER BY COALESCE(r.updated_at, r.created_at) DESC
            ) AS rn
        FROM raw_sap_routing r
    )
    WHERE rn = 1
),

sap_routing_dedup_by_product AS (
    SELECT *
    FROM (
        SELECT
            r.*, 
            ROW_NUMBER() OVER (
                PARTITION BY r.ProductNumber, TRIM(r.Operation), r."Group"
                ORDER BY COALESCE(r.updated_at, r.created_at) DESC
            ) AS rn
        FROM raw_sap_routing r
    )
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
        m."Group",
        m.StepInQuantity,
        m.TrackOutQuantity,
        m.TrackOutOperator,
        m.factory_name,
        m.VSM,
        m.source_file,
        m.created_at,
        m.updated_at,
        -- 关联 SFC TrackIn 时间
        s.TrackInTime AS TrackIn_SFC,
        s.ScrapQty,

        -- 关联 SAP 标准时间：优先 CFN 匹配，其次用 ProductNumber 匹配
        COALESCE(r1.StandardTime, r2.StandardTime) AS ST_SAP,
        COALESCE(r1.EH_machine, r2.EH_machine) AS SAP_EH_machine,
        COALESCE(r1.EH_labor, r2.EH_labor) AS SAP_EH_labor,
        COALESCE(r1.Quantity, r2.Quantity) AS SAP_Quantity,
        COALESCE(r1.SetupTime, r2.SetupTime) AS SAP_SetupTime,
        COALESCE(r1.OEE, r2.OEE) AS SAP_OEE,
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
    FROM raw_mes m
    LEFT JOIN sfc_agg s 
        ON m.BatchNumber = s.BatchNumber 
        AND TRIM(m.Operation) = TRIM(s.Operation)
    LEFT JOIN sap_routing_dedup r1
        ON m.CFN = r1.CFN
        AND m.Operation = r1.Operation
        AND m."Group" = r1."Group"
    LEFT JOIN sap_routing_dedup_by_product r2
        ON r1.CFN IS NULL
        AND m.ProductNumber = r2.ProductNumber
        AND m.Operation = r2.Operation
        AND m."Group" = r2."Group"
),
metrics AS (
    SELECT 
        id,
        BatchNumber,
        Operation,
        Machine,
        EnterStepTime,
        TrackInTime,
        TrackOutTime,
        DATE(TrackOutTime) AS TrackOutDate,
        Plant,
        Product_Desc,
        ProductNumber,
        CFN,
        ProductionOrder,
        OperationDesc,
        "Group",
        StepInQuantity,
        TrackOutQuantity,
        TrackOutOperator,
        IsSetup,
        SAP_SetupTime AS SetupTime,
        SAP_OEE AS OEE,
        SAP_EH_machine AS EH_machine,
        SAP_EH_labor AS EH_labor,
        factory_name,
        VSM,
        TrackIn_SFC,
        ScrapQty,
        ST_SAP,
        PreviousBatchEndTime,
        
        -- V1 LT(d) 计算逻辑：
        -- 0010工序：优先使用 TrackIn_SFC，否则使用 EnterStepTime，再否则使用 TrackInTime
        -- 非0010工序：使用 EnterStepTime
        CASE 
            WHEN TrackOutTime IS NULL THEN NULL
            WHEN TRIM(Operation) = '0010' OR TRIM(Operation) = '10' THEN
                CASE 
                    WHEN TrackIn_SFC IS NOT NULL THEN 
                        ROUND((julianday(TrackOutTime) - julianday(TrackIn_SFC)), 2)
                    WHEN EnterStepTime IS NOT NULL THEN 
                        ROUND((julianday(TrackOutTime) - julianday(EnterStepTime)), 2)
                    WHEN TrackInTime IS NOT NULL THEN 
                        ROUND((julianday(TrackOutTime) - julianday(TrackInTime)), 2)
                    ELSE NULL
                END
            ELSE
                CASE 
                    WHEN EnterStepTime IS NOT NULL THEN 
                        ROUND((julianday(TrackOutTime) - julianday(EnterStepTime)), 2)
                    ELSE NULL
                END
        END AS "LT(d)",
        
        -- V1 PT(d) 计算逻辑：
        -- 检测停产期：如果 EnterStepTime > PreviousBatchEndTime，说明中间有停产
        -- 有停产期：使用 TrackInTime
        -- 正常生产：使用 PreviousBatchEndTime，如果为空则使用 TrackInTime
        CASE 
            WHEN TrackOutTime IS NULL THEN NULL
            WHEN EnterStepTime IS NOT NULL AND PreviousBatchEndTime IS NOT NULL 
                 AND julianday(EnterStepTime) > julianday(PreviousBatchEndTime) THEN
                -- 有停产期：使用 TrackInTime
                CASE 
                    WHEN TrackInTime IS NOT NULL THEN 
                        ROUND((julianday(TrackOutTime) - julianday(TrackInTime)), 2)
                    ELSE 
                        ROUND((julianday(TrackOutTime) - julianday(PreviousBatchEndTime)), 2)
                END
            WHEN PreviousBatchEndTime IS NOT NULL THEN
                -- 正常生产：使用 PreviousBatchEndTime
                ROUND((julianday(TrackOutTime) - julianday(PreviousBatchEndTime)), 2)
            WHEN TrackInTime IS NOT NULL THEN
                -- PreviousBatchEndTime 为空，使用 TrackInTime
                ROUND((julianday(TrackOutTime) - julianday(TrackInTime)), 2)
            ELSE NULL
        END AS "PT(d)",
        
        -- V1 ST(d) 计算逻辑：
        -- ST = (调试时间 + (合格数量 + 报废数量) × 单件时间 / OEE + 0.5小时换批时间) / 24
        -- 单件时间 = SAP_EH_machine（秒）
        CASE 
            WHEN SAP_EH_machine IS NULL AND SAP_EH_labor IS NULL THEN NULL
            ELSE ROUND(
                (
                    CASE WHEN IsSetup = 'Yes' AND SAP_SetupTime IS NOT NULL THEN SAP_SetupTime ELSE 0 END
                    + (COALESCE(TrackOutQuantity, 0) + COALESCE(ScrapQty, 0))
                        * (COALESCE(SAP_EH_machine, SAP_EH_labor) / COALESCE(NULLIF(SAP_Quantity, 0), 1))
                        / 3600.0 / COALESCE(NULLIF(SAP_OEE, 0), 0.77)
                    + 0.5  -- 换批时间 0.5 小时
                ) / 24.0, 
                2
            )
        END AS "ST(d)",
        
        -- 元数据
        source_file,
        created_at,
        updated_at
    FROM mes_with_prev
)
SELECT
    metrics.*,
    (8.0 / 24.0) AS "Tolerance(d)",
    CASE
        WHEN metrics."PT(d)" IS NULL OR metrics."ST(d)" IS NULL OR metrics."ST(d)" <= 0 THEN NULL
        WHEN metrics."PT(d)" > metrics."ST(d)" + (8.0 / 24.0) THEN 'Overdue'
        ELSE 'OnTime'
    END AS CompletionStatus,
    CASE
        WHEN metrics."PT(d)" IS NULL OR metrics."ST(d)" IS NULL OR metrics."ST(d)" <= 0 THEN NULL
        WHEN metrics."PT(d)" > metrics."ST(d)" + (8.0 / 24.0) THEN 1
        ELSE 0
    END AS IsOverdue,
    CASE
        WHEN metrics."PT(d)" IS NULL OR metrics."ST(d)" IS NULL OR metrics."ST(d)" <= 0 THEN NULL
        WHEN metrics."PT(d)" > metrics."ST(d)" + (8.0 / 24.0) THEN 0
        ELSE 1
    END AS IsOnTime
FROM metrics;
"""

def update_view():
    print(f"Connecting to {DB_PATH}")
    if not DB_PATH.exists():
        print("Database file not found!")
        return

    max_retries = 5
    for attempt in range(1, max_retries + 1):
        conn = None
        try:
            # timeout: wait for locks; busy_timeout: SQLite-level wait
            conn = sqlite3.connect(DB_PATH, timeout=30)
            conn.execute('PRAGMA busy_timeout = 30000;')
            cursor = conn.cursor()

            print("Dropping old view...")
            cursor.execute(SQL_DROP_VIEW)

            print("Creating new view...")
            cursor.execute(SQL_CREATE_VIEW)

            conn.commit()
            print("View v_mes_metrics updated successfully.")
            return
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if 'database is locked' in msg and attempt < max_retries:
                wait_s = 2 * attempt
                print(f"Database is locked, retry {attempt}/{max_retries} after {wait_s}s...")
                try:
                    if conn is not None:
                        conn.rollback()
                except Exception:
                    pass
                time.sleep(wait_s)
                continue
            print(f"Error updating view: {e}")
            try:
                if conn is not None:
                    conn.rollback()
            except Exception:
                pass
            return
        except Exception as e:
            print(f"Error updating view: {e}")
            try:
                if conn is not None:
                    conn.rollback()
            except Exception:
                pass
            return
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

if __name__ == "__main__":
    update_view()
