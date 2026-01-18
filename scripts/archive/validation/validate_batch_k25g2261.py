import pandas as pd
import pyodbc
import sys

def validate_batch():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        r"SERVER=localhost\SQLEXPRESS;"
        "DATABASE=mddap_v2;"
        "Trusted_Connection=yes;"
        "Encrypt=no;"
        "TrustServerCertificate=no;"
    )
    
    try:
        conn = pyodbc.connect(conn_str)
    except Exception as e:
        print(f"Failed to connect: {e}")
        return

    batch_number = 'K25G2261'
    print(f"Analyzing Batch: {batch_number}")
    
    # 1. Get Batch Metrics
    query = f"""
    SELECT 
        BatchNumber, Operation, Plant,
        TrackInTime, TrackOutTime, PreviousBatchEndTime, EnterStepTime,
        [PT(d)], [PNW(d)], CompletionStatus
    FROM dbo.mes_metrics_snapshot_a
    WHERE BatchNumber = '{batch_number}'
    """
    
    try:
        df = pd.read_sql(query, conn)
    except Exception as e:
        print(f"Error querying batch: {e}")
        conn.close()
        return

    if df.empty:
        print("Batch not found in mes_metrics_snapshot_a")
        conn.close()
        return
        
    row = df.iloc[0]
    print("\n--- DB Values ---")
    print(df.transpose().to_string(header=False))
    
    # 2. Determine Time Range
    track_out = pd.to_datetime(row['TrackOutTime'])
    enter_step = pd.to_datetime(row['EnterStepTime']) if pd.notna(row['EnterStepTime']) else None
    prev_end = pd.to_datetime(row['PreviousBatchEndTime']) if pd.notna(row['PreviousBatchEndTime']) else None
    track_in = pd.to_datetime(row['TrackInTime']) if pd.notna(row['TrackInTime']) else None
    
    start_time_used = None
    logic_path = ""
    
    if enter_step and prev_end and (enter_step > prev_end):
        logic_path = "Gap (Enter > Prev)"
        if track_in:
            start_time_used = track_in
            logic_path += " -> Used TrackIn"
        else:
            start_time_used = prev_end
            logic_path += " -> TrackIn Missing, Used Prev"
    elif prev_end:
        logic_path = "Continuous (Enter <= Prev or Enter Missing)"
        start_time_used = prev_end
        logic_path += " -> Used Prev"
    elif track_in:
         logic_path = "Prev Missing"
         start_time_used = track_in
         logic_path += " -> Used TrackIn"
    
    print(f"\n--- Logic Trace ---")
    print(f"Logic Path: {logic_path}")
    print(f"Start Time: {start_time_used}")
    print(f"End Time:   {track_out}")
    
    if start_time_used:
        gross_duration = track_out - start_time_used
        gross_pt_days = gross_duration.total_seconds() / 86400.0
        print(f"Gross Duration: {gross_duration}")
        print(f"Gross PT (days): {gross_pt_days:.4f}")
        
        # 3. Check Calendar
        plant = row['Plant']
        if not plant:
            print("Plant is NULL, cannot check calendar.")
        else:
            print(f"\n--- Calendar Check (Plant: {plant}) ---")
            cal_query = f"""
            SELECT date_value, is_workday 
            FROM dbo.raw_calendar 
            WHERE factory_code = '{plant}' 
              AND date_value BETWEEN ? AND ?
            ORDER BY date_value
            """
            
            # Expand range slightly to be safe
            d_start = start_time_used.date()
            d_end = track_out.date()
            
            cal_df = pd.read_sql(cal_query, conn, params=[d_start, d_end])
            
            if cal_df.empty:
                print("No calendar entries found for this range.")
            else:
                non_workdays = cal_df[cal_df['is_workday'] == False]
                print(f"Calendar Days Found: {len(cal_df)}")
                print(f"Non-Workdays Found: {len(non_workdays)}")
                if not non_workdays.empty:
                    print(non_workdays)
                
                # Verify PNW calculation
                # (Simple approximation: count full non-workdays)
                # The actual SQL logic might be more complex (partial days?), but usually it's whole days for this granularity?
                # Actually SQL usually joins on date.
                
                print(f"\nDB PNW(d): {row['PNW(d)']}")
                
                calculated_net = max(0, gross_pt_days - (float(row['PNW(d)']) if row['PNW(d)'] else 0))
                print(f"Calculated Net PT = {gross_pt_days:.4f} - {row['PNW(d)']} = {calculated_net:.4f}")
                print(f"DB PT(d)          = {row['PT(d)']}")

    conn.close()

if __name__ == "__main__":
    validate_batch()
