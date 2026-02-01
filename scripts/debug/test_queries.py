
import pyodbc
import pandas as pd
import datetime

CONN_STR = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost\SQLEXPRESS;"
    r"DATABASE=mddap_v2;"
    r"Trusted_Connection=yes;"
)

def test_query(name, sql_text, params):
    conn = pyodbc.connect(CONN_STR)
    try:
        print(f"Testing {name}...")
        start_t = datetime.datetime.now()
        cursor = conn.cursor()
        cursor.execute(sql_text, params)
        # Fetch one row to ensure execution happens
        cursor.fetchmany(1)
        duration = datetime.datetime.now() - start_t
        print(f"PASS: {name} (Time: {duration.total_seconds():.3f}s)")
    except Exception as e:
        print(f"FAIL: {name}")
        print(e)
    finally:
        conn.close()

if __name__ == "__main__":
    plant = '9997'
    year = 'FY26'
    s = datetime.date(2025, 4, 1)
    e = datetime.date(2026, 3, 31)
    
    # Simulate areaFilter
    area_filter = " AND (om.area IS NULL OR om.area NOT IN (N'无区域 NA', N'外协 OS'))"

    # Heatmap Query
    heatmap_q = f"""
                SELECT 
                    COALESCE(om.display_name, l.OperationDesc) as op,
                    CAST(l.PostingDate AS DATE) as date,
                    SUM(l.EarnedLaborTime) as val
                FROM raw_sap_labor_hours l
                LEFT JOIN (SELECT operation_name, display_name, erp_code, MAX(area) as area FROM dim_operation_mapping WHERE erp_code = ? GROUP BY operation_name, display_name, erp_code) om 
                    ON l.OperationDesc = om.operation_name AND l.Plant = om.erp_code
                WHERE l.Plant = ?
                  AND CAST(l.PostingDate AS DATE) BETWEEN DATEADD(day, -20, ?) AND ?
                {area_filter}
                
                GROUP BY COALESCE(om.display_name, l.OperationDesc), CAST(l.PostingDate AS DATE)
                ORDER BY op, date
    """
    
    # Area Dist Query
    area_dist_q = f"""
                SELECT 
                  ISNULL(om.area, 'Unknown') as area,
                  ISNULL(SUM(l.EarnedLaborTime), 0) as earnedHours
                FROM raw_sap_labor_hours l
                LEFT JOIN (SELECT operation_name, erp_code, MAX(area) as area FROM dim_operation_mapping WHERE erp_code = ? GROUP BY operation_name, erp_code) om 
                    ON l.OperationDesc = om.operation_name AND l.Plant = om.erp_code
                WHERE TRY_CAST(l.PostingDate AS DATE) BETWEEN ? AND ? AND l.Plant = ? {area_filter}
                GROUP BY om.area
                ORDER BY earnedHours DESC
    """

    # Details Query (Suspect)
    # Details Query (Suspect) - Optimized
    details_q = f"""
                SELECT TOP 200 
                  TRY_CAST(l.PostingDate AS DATE) as PostingDate, 
                  l.OrderNumber, m.BatchNumber, l.Material, l.EarnedLaborTime as actualEH, l.ActualQuantity,
                  l.WorkCenter, l.Plant, l.ProductionScheduler as productLine,
                  l.Operation, l.OperationDesc as rawOpDesc, 
                  om.area, 
                  om.display_name as operationDesc
                FROM raw_sap_labor_hours l
                LEFT JOIN (SELECT operation_name, display_name, erp_code, MAX(area) as area FROM dim_operation_mapping WHERE erp_code = ? GROUP BY operation_name, display_name, erp_code) om 
                    ON l.OperationDesc = om.operation_name AND l.Plant = om.erp_code
                OUTER APPLY (
                    SELECT TOP 1 BatchNumber 
                    FROM raw_mes m
                    WHERE m.ProductionOrder = l.OrderNumber
                ) m
                WHERE TRY_CAST(l.PostingDate AS DATE) BETWEEN ? AND ? AND l.Plant = ? {area_filter}
                ORDER BY l.PostingDate DESC
    """

    test_query("Heatmap", heatmap_q, [plant, plant, e, e])
    test_query("AreaDist", area_dist_q, [plant, s, e, plant])
    test_query("Details", details_q, [plant, s, e, plant])
