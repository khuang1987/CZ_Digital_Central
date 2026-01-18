import pyodbc
import pandas as pd
import sys
import os

import warnings
warnings.filterwarnings('ignore')

# SQL Server connection
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost\SQLEXPRESS;"
    "DATABASE=mddap_v2;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
    "TrustServerCertificate=no;"
)

def validate_sa_calculation():
    print("Connecting to SQL Server...")
    try:
        conn = pyodbc.connect(conn_str)
    except Exception as e:
        print(f"Failed to connect: {e}")
        return

    # Query sample data: Overdue records and OnTime records
    # Force use of view to validate new logic
    table_name = "dbo.v_mes_metrics"
    
    # -------------------------------------------------------------------------
    # 2. Aggregate Statistics
    # -------------------------------------------------------------------------
    print(f"\nComputing Aggregate Statistics (Last 30 Days)...")
    
    agg_query = f"""
    SELECT 
        CompletionStatus,
        COUNT(*) as cnt
    FROM {table_name}
    WHERE TrackOutTime >= DATEADD(day, -30, GETDATE())
    GROUP BY CompletionStatus
    """
    
    try:
        cur = conn.cursor()
        cur.execute(agg_query)
        stats = {row[0]: row[1] for row in cur.fetchall()}
        
        total = sum(stats.values())
        on_time = stats.get('OnTime', 0)
        overdue = stats.get('Overdue', 0)
        valid_sa_count = on_time + overdue
        
        print(f"Total Batches: {total}")
        print("-" * 30)
        for status, count in stats.items():
            pct = (count / total * 100) if total > 0 else 0
            print(f"{status or 'NULL'}: {count} ({pct:.1f}%)")
        print("-" * 30)
        
        if valid_sa_count > 0:
            sa_rate = (on_time / valid_sa_count) * 100
            print(f"Schedule Attainment (SA) Rate: {sa_rate:.2f}% (OnTime / (OnTime + Overdue))")
        else:
            print("No valid SA samples (OnTime + Overdue = 0)")
            
    except Exception as e:
        print(f"Error computing aggregates: {e}")

    # PERFORMANCE OPTIMIZATION: Filter by recent data (last 30 days) 
    # to avoid full scan on computed view columns
    query = f"""
    SELECT TOP 10
        BatchNumber, Operation, machine as Machine, 
        TrackOutDate,
        "PT(d)", "ST(d)", "CompletionStatus",
        "PNW(d)", "LNW(d)", "LT(d)",
        
        -- Inputs for ST
        EH_machine, EH_labor, 
        TrackOutQuantity, ScrapQty,
        "Setup Time (h)" as SetupTime, "Setup" as IsSetup, OEE,
        
        -- Inputs for PT
        EnterStepTime, TrackInTime, TrackOutTime, PreviousBatchEndTime
        
    FROM {table_name}
    WHERE "ST(d)" IS NOT NULL 
      AND "PT(d)" IS NOT NULL
      AND CompletionStatus = 'Overdue'
      AND TrackOutTime >= DATEADD(day, -30, GETDATE())
    ORDER BY TrackOutTime DESC
    """
    
    print(f"\nFetching Overdue Samples (Low SA contributors) from {table_name} (Last 30 Days)...")
    try:
        df_overdue = pd.read_sql(query, conn)
        analyze_samples(df_overdue, "Overdue")
    except Exception as e:
        print(f"Error fetching overdue samples: {e}")

    query_ontime = query.replace("Overdue", "OnTime")
    print("\nFetching OnTime Samples (Good SA contributors) (Last 90 Days)...")
    try:
        df_ontime = pd.read_sql(query_ontime, conn)
        analyze_samples(df_ontime, "OnTime")
    except Exception as e:
        print(f"Error fetching ontime samples: {e}")
    
    conn.close()

def analyze_samples(df, label):
    if df.empty:
        print(f"No samples found for {label}.")
        return

    print(f"\n--- Analysis for {label} Samples ---")
    
    for i, row in df.iterrows():
        print(f"\nSample {i+1}: Batch={row['BatchNumber']}, Op={row['Operation']}, Machine={row['Machine']}")
        print(f"  DB Values: PT(d)={row['PT(d)']}, ST(d)={row['ST(d)']}, Status={row['CompletionStatus']}")
        print(f"  V2 Metrics: PNW(d)={row['PNW(d)']}, LNW(d)={row['LNW(d)']}, LT(d)={row['LT(d)']}")
        
        # --- Validate ST ---
        # Logic:
        # ( (IsSetup=='Yes' ? SetupTime : 0) + (Qty + Scrap) * (EH / BaseQty) / 3600 / OEE + 0.5 ) / 24
        
        # In snapshot, IsSetup might be 'Yes'/'No'
        is_setup = (str(row['IsSetup']).lower() == 'yes')
        setup_time = float(row['SetupTime']) if row['SetupTime'] else 0.0
        setup_component = setup_time if is_setup else 0.0
        
        qty = (float(row['TrackOutQuantity']) if row['TrackOutQuantity'] else 0) + (float(row['ScrapQty']) if row['ScrapQty'] else 0)
        
        eh_machine = float(row['EH_machine']) if row['EH_machine'] is not None else None
        eh_labor = float(row['EH_labor']) if row['EH_labor'] is not None else None
        eh = eh_machine if eh_machine is not None else eh_labor
        
        # Snapshot doesn't have SAP_Quantity, assume 1. 
        # In init_schema_v2.sql, it uses SAP_Quantity from raw_sap_routing. 
        # Here we only have what's in snapshot.
        base_qty = 1.0 
        oee = float(row['OEE']) if row['OEE'] and row['OEE'] != 0 else 0.77
        
        print("  ST Inputs:")
        print(f"    IsSetup={row['IsSetup']}, SetupTime={row['SetupTime']} -> SetupComp={setup_component}")
        print(f"    Qty={qty} (Out={row['TrackOutQuantity']}, Scrap={row['ScrapQty']})")
        print(f"    EH={eh} (Mach={row['EH_machine']}, Labor={row['EH_labor']})")
        print(f"    BaseQty={base_qty} (Assumed), OEE={oee}")
        
        st_calc = 0.0
        if eh is not None:
            # Formula breakdown
            # Part 1: Production Time (Hours)
            # (Qty * (EH / BaseQty)) / 3600 / OEE
            # EH is in seconds
            
            unit_time_sec = eh / base_qty
            total_time_sec = qty * unit_time_sec
            production_hours_raw = total_time_sec / 3600.0
            production_hours_oee = production_hours_raw / oee
            
            # Part 2: Changeover
            changeover = 0.5
            
            total_hours = setup_component + production_hours_oee + changeover
            st_calc = total_hours / 24.0
            
            print(f"  ST Calculation Trace:")
            print(f"    UnitTime(s) = {eh} / {base_qty} = {unit_time_sec}")
            print(f"    TotalProdTime(s) = {qty} * {unit_time_sec} = {total_time_sec}")
            print(f"    ProdHoursRaw = {total_time_sec} / 3600 = {production_hours_raw:.4f}")
            print(f"    ProdHoursOEE = {production_hours_raw} / {oee} = {production_hours_oee:.4f}")
            print(f"    TotalHours = {setup_component} (Setup) + {production_hours_oee:.4f} (Prod) + {changeover} (Changeover) = {total_hours:.4f}")
            print(f"    ST(d) = {total_hours} / 24 = {st_calc:.4f}")
            print(f"    DB ST(d) = {row['ST(d)']}")
            print(f"    Diff = {st_calc - float(row['ST(d)'] if row['ST(d)'] else 0):.6f}")
        else:
            print("    EH is None, cannot calculate ST.")

        # --- Validate PT ---
        print("  PT Inputs:")
        print(f"    EnterStepTime={row['EnterStepTime']}")
        print(f"    PreviousBatchEndTime={row['PreviousBatchEndTime']}")
        print(f"    TrackInTime={row['TrackInTime']}")
        print(f"    TrackOutTime={row['TrackOutTime']}")
        
        # Logic check
        # IF Enter > Prev: Gap exists -> Use TrackIn
        # ELSE: Continuous -> Use Prev
        
        pt_source = "Unknown"
        start_time_used = None
        
        track_out = pd.to_datetime(row['TrackOutTime'])
        enter_step = pd.to_datetime(row['EnterStepTime']) if pd.notna(row['EnterStepTime']) else None
        prev_end = pd.to_datetime(row['PreviousBatchEndTime']) if pd.notna(row['PreviousBatchEndTime']) else None
        track_in = pd.to_datetime(row['TrackInTime']) if pd.notna(row['TrackInTime']) else None
        
        if pd.isna(track_out):
            print("    TrackOutTime is None, PT is None.")
            continue
            
        if enter_step and prev_end and (enter_step > prev_end):
            pt_source = "Gap (Enter > Prev)"
            if track_in:
                start_time_used = track_in
                pt_source += " -> Used TrackIn"
            else:
                start_time_used = prev_end
                pt_source += " -> TrackIn Missing, Used Prev"
        elif prev_end:
            pt_source = "Continuous (Enter <= Prev or Enter Missing)"
            start_time_used = prev_end
            pt_source += " -> Used Prev"
        elif track_in:
             pt_source = "Prev Missing"
             start_time_used = track_in
             pt_source += " -> Used TrackIn"
             
        pt_calc = 0.0
        gross_pt = 0.0
        if start_time_used:
            duration = track_out - start_time_used
            gross_pt = duration.total_seconds() / 3600.0 / 24.0 # Days
            
            # V2 Logic: Net PT = Gross PT - PNW
            pnw_d = float(row['PNW(d)']) if row['PNW(d)'] is not None else 0.0
            pt_calc = max(0, gross_pt - pnw_d)
            
            print(f"  PT Calculation Trace:")
            print(f"    Logic Path: {pt_source}")
            print(f"    StartTimeUsed: {start_time_used}")
            print(f"    Gross Duration: {track_out} - {start_time_used} = {duration}")
            print(f"    Gross PT(d) = {gross_pt:.4f}")
            print(f"    PNW(d) = {pnw_d:.4f}")
            print(f"    Net PT(d) = {gross_pt:.4f} - {pnw_d:.4f} = {pt_calc:.4f}")
            print(f"    DB PT(d) = {row['PT(d)']}")
            
            if row['PT(d)'] is not None:
                 print(f"    Diff = {pt_calc - float(row['PT(d)']):.6f}")
        else:
            print("    Could not determine StartTime.")

        # --- Verdict ---
        tolerance_d = 8.0/24.0
        threshold = st_calc + tolerance_d
        is_overdue_calc = pt_calc > threshold
        print(f"  Verdict:")
        print(f"    Threshold(d) = ST({st_calc:.4f}) + Tol({tolerance_d:.4f}) = {threshold:.4f}")
        print(f"    PT({pt_calc:.4f}) > Threshold({threshold:.4f}) ? {is_overdue_calc}")
        print(f"    DB Status: {row['CompletionStatus']}")

if __name__ == "__main__":
    validate_sa_calculation()
