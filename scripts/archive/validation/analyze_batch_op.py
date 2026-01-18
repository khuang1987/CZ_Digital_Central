import argparse
import pandas as pd
import pyodbc
import sys
import warnings
from datetime import timedelta

# Suppress pandas UserWarning about SQLAlchemy
warnings.filterwarnings('ignore', category=UserWarning)

def connect_db():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        r"SERVER=localhost\SQLEXPRESS;"
        "DATABASE=mddap_v2;"
        "Trusted_Connection=yes;"
        "Encrypt=no;"
        "TrustServerCertificate=no;"
    )
    return pyodbc.connect(conn_str)

def get_batch_data(conn, batch_no, op_no=None):
    query = f"""
    SELECT 
        id, BatchNumber, Operation, [Group], CFN, Plant,
        TrackOutTime, TrackInTime, EnterStepTime, PreviousBatchEndTime,
        StepInQuantity, TrackOutQuantity, ScrapQty,
        [Setup], [Setup Time (h)], OEE, EH_machine, EH_labor,
        [PT(d)], [ST(d)], [PNW(d)], [LNW(d)], [LT(d)],
        CompletionStatus, 
        [Machine(#)], machine
    FROM dbo.v_mes_metrics
    WHERE BatchNumber = ?
    """
    params = [batch_no]
    if op_no:
        query += " AND Operation = ?"
        params.append(op_no)
    
    query += " ORDER BY TrackOutTime"
    
    return pd.read_sql(query, conn, params=params)

def check_calendar_range(conn, plant, start_dt, end_dt):
    if pd.isna(start_dt) or pd.isna(end_dt):
        return 0, []
        
    # NOTE: raw_calendar appears to be global (no factory_code)
    query = """
    SELECT CalendarDate 
    FROM dbo.raw_calendar
    WHERE IsWorkday = 0
      AND CalendarDate BETWEEN ? AND ?
    ORDER BY CalendarDate
    """
    
    try:
        df = pd.read_sql(query, conn, params=[start_dt, end_dt])
        return len(df), df['CalendarDate'].tolist()
    except Exception as e:
        print(f"Warning: Calendar check failed: {e}")
        return 0, []

def analyze_row(conn, row):
    print(f"\n{'='*60}")
    print(f"Batch: {row['BatchNumber']} | Op: {row['Operation']} | Group: {row['Group']}")
    print(f"{'='*60}")
    
    # 1. Routing / ST Analysis
    print("\n[1] Standard Time (ST) Analysis")
    print(f"  Source Inputs (DB):")
    print(f"    EH (Machine/Labor): {row['EH_machine']} / {row['EH_labor']}")
    print(f"    OEE: {row['OEE']}")
    print(f"    Setup Time: {row['Setup Time (h)']} h")
    print(f"    Is Setup Batch: {row['Setup']}")
    print(f"    Quantity: Out={row['TrackOutQuantity']}, Scrap={row['ScrapQty']}")
    
    # Recalculate ST
    st_calc = 0.0
    st_formula_trace = []
    
    # Logic matches SQL: 
    # UnitTime = EH / 3600 (Hours)
    # TotalProd = (Qty + Scrap) * UnitTime / OEE
    # TotalSetup = (SetupTime if IsSetup else 0)
    # Changeover = 0.5
    # ST(d) = (TotalProd + TotalSetup + Changeover) / 24.0
    
    eh = row['EH_machine'] if pd.notna(row['EH_machine']) and row['EH_machine'] > 0 else row['EH_labor']
    if pd.isna(eh) or eh == 0:
        print("  -> ERROR: Missing Effective Hours (EH). Cannot calculate ST.")
        print(f"     DB ST(d): {row['ST(d)']}")
    else:
        qty = (row['TrackOutQuantity'] or 0) + (row['ScrapQty'] or 0)
        oee = row['OEE'] if pd.notna(row['OEE']) and row['OEE'] > 0 else 0.77
        
        unit_time_hr = eh / 3600.0
        prod_time_hr = (qty * unit_time_hr) / oee
        
        is_setup = str(row['Setup']).lower() in ['yes', 'true', '1']
        setup_time_hr = row['Setup Time (h)'] if (is_setup and pd.notna(row['Setup Time (h)'])) else 0.0
        
        changeover_hr = 0.5
        
        total_hr = prod_time_hr + setup_time_hr + changeover_hr
        st_calc = total_hr / 24.0
        
        print(f"  Recalculation:")
        print(f"    Unit Time (hr) = {eh} / 3600 = {unit_time_hr:.4f}")
        print(f"    Prod Time (hr) = ({qty} * {unit_time_hr:.4f}) / {oee} = {prod_time_hr:.4f}")
        print(f"    Setup Time (hr)= {setup_time_hr} (IsSetup={is_setup})")
        print(f"    Total (hr)     = {prod_time_hr:.4f} + {setup_time_hr} + {changeover_hr} = {total_hr:.4f}")
        print(f"    ST (days)      = {total_hr:.4f} / 24 = {st_calc:.4f}")
        print(f"  DB Value: {row['ST(d)']}")
        
        diff = abs(st_calc - (row['ST(d)'] or 0))
        if diff < 0.001:
            print("  -> MATCH ✓")
        else:
            print(f"  -> MISMATCH ✗ (Diff: {diff:.6f})")

    # 2. Process Time (PT) Analysis
    print("\n[2] Process Time (PT) Analysis")
    print(f"  Timestamps:")
    print(f"    EnterStep:  {row['EnterStepTime']}")
    print(f"    PrevEnd:    {row['PreviousBatchEndTime']}")
    print(f"    TrackIn:    {row['TrackInTime']}")
    print(f"    TrackOut:   {row['TrackOutTime']}")
    
    # Determine Start Time
    start_time = None
    logic_path = "Unknown"
    
    enter = pd.to_datetime(row['EnterStepTime']) if pd.notna(row['EnterStepTime']) else None
    prev = pd.to_datetime(row['PreviousBatchEndTime']) if pd.notna(row['PreviousBatchEndTime']) else None
    track_in = pd.to_datetime(row['TrackInTime']) if pd.notna(row['TrackInTime']) else None
    track_out = pd.to_datetime(row['TrackOutTime'])
    
    if pd.isna(track_out):
        print("  -> ERROR: Missing TrackOutTime. PT is undefined.")
        return

    # Logic approximation
    if enter and prev and enter > prev:
        # Gap logic
        if track_in:
            start_time = track_in
            logic_path = "Gap (Enter > Prev) -> Start = TrackIn"
        else:
            start_time = prev
            logic_path = "Gap (Enter > Prev) -> TrackIn Missing -> Start = Prev"
    elif prev:
        start_time = prev
        logic_path = "Continuous (Enter <= Prev) -> Start = Prev"
    elif track_in:
        start_time = track_in
        logic_path = "First Batch / Prev Missing -> Start = TrackIn"
    else:
        # Fallback if no start time found?
        # Usually logic defaults to TrackOut (0 duration) or EnterStep?
        # SQL logic usually strictly requires a predecessor or trackin.
        logic_path = "No Start Time Found"
        
    print(f"  Logic Used: {logic_path}")
    print(f"  Start Time: {start_time}")
    
    if start_time:
        gross_duration = track_out - start_time
        gross_days = gross_duration.total_seconds() / 86400.0
        print(f"  Gross Duration: {gross_duration} ({gross_days:.4f} days)")
        
        # Calendar Check
        pnw_days_count, non_workdays = check_calendar_range(conn, str(row['Plant']), start_time, track_out)
        # Note: The SQL logic for PNW might sum fractions or whole days. 
        # Usually for 'raw_calendar' it's whole days.
        # But wait, if start/end are mid-day, does it exclude the whole day?
        # V2 logic usually subtracts full non-workdays that fall strictly within the range? 
        # Or does it intersect?
        # Assuming simple count of non-workdays fully covered or touched? 
        # Actually standard V2 logic: Sum(datediff(day, ...)) is rough.
        # Let's trust the DB PNW for now but show the calendar hits.
        
        print(f"  Calendar Check (Plant {row['Plant']}): Found {pnw_days_count} non-workdays in range.")
        if non_workdays:
            print(f"    Dates: {[d.strftime('%Y-%m-%d') for d in non_workdays]}")
            
        print(f"  DB PNW(d): {row['PNW(d)']}")
        
        # Recalculate Net PT
        net_pt_calc = max(0, gross_days - (row['PNW(d)'] or 0))
        print(f"  Recalculation:")
        print(f"    Net PT = Gross ({gross_days:.4f}) - PNW ({row['PNW(d)'] or 0}) = {net_pt_calc:.4f}")
        print(f"  DB PT(d): {row['PT(d)']}")
        
        diff_pt = abs(net_pt_calc - (row['PT(d)'] or 0))
        if diff_pt < 0.001:
             print("  -> MATCH ✓")
        else:
             print(f"  -> MISMATCH ✗ (Diff: {diff_pt:.6f})")
             
    # 3. Verdict
    print("\n[3] Verdict Analysis")
    if st_calc > 0:
        threshold = st_calc + (8.0/24.0) # 8 hours tolerance
        is_overdue = (row['PT(d)'] or 0) > threshold
        status_calc = "Overdue" if is_overdue else "OnTime"
        print(f"  Threshold = ST ({st_calc:.4f}) + Tolerance (0.3333) = {threshold:.4f}")
        print(f"  PT ({row['PT(d)']}) > Threshold ? {is_overdue}")
        print(f"  Calculated Status: {status_calc}")
        print(f"  DB Status:         {row['CompletionStatus']}")
        if status_calc == row['CompletionStatus']:
            print("  -> STATUS MATCH ✓")
        else:
            print("  -> STATUS MISMATCH ✗")
    else:
        print("  -> Cannot determine status (No ST)")
        print(f"  DB Status: {row['CompletionStatus']}")

def main():
    parser = argparse.ArgumentParser(description="Analyze Batch Calculation Trace")
    parser.add_argument("batch", help="Batch Number")
    parser.add_argument("--op", help="Operation Number (optional)", default=None)
    args = parser.parse_args()
    
    conn = connect_db()
    try:
        df = get_batch_data(conn, args.batch, args.op)
        if df.empty:
            print(f"No records found for Batch {args.batch}")
        else:
            for i, row in df.iterrows():
                analyze_row(conn, row)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
