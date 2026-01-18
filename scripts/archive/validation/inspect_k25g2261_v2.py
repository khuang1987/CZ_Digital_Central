import pandas as pd
import pyodbc

def analyze_batch():
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
    
    # Replicate logic to find start time
    if enter_step and prev_end and (enter_step > prev_end):
        if track_in:
            start_time_used = track_in
        else:
            start_time_used = prev_end
    elif prev_end:
        start_time_used = prev_end
    elif track_in:
         start_time_used = track_in
    
    if not start_time_used:
        print("Could not determine start time.")
        conn.close()
        return

    print(f"\nTime Range: {start_time_used} to {track_out}")
    
    # 3. Check Calendar (assuming raw_calendar for now, check if global)
    # The previous error said 'factory_code' invalid, so we omit it.
    cal_query = f"""
    SELECT CalendarDate, IsWorkday
    FROM dbo.raw_calendar 
    WHERE CalendarDate BETWEEN ? AND ?
    ORDER BY CalendarDate
    """
    
    d_start = start_time_used.date()
    d_end = track_out.date()
    
    try:
        cal_df = pd.read_sql(cal_query, conn, params=[d_start, d_end])
        print(f"\n--- Calendar Data ({d_start} to {d_end}) ---")
        if cal_df.empty:
            print("No calendar entries found.")
        else:
            print(cal_df)
            non_workdays = cal_df[cal_df['IsWorkday'] == False]
            print(f"Non-Workdays Count: {len(non_workdays)}")
    except Exception as e:
        print(f"Error querying calendar: {e}")

    conn.close()

if __name__ == "__main__":
    analyze_batch()
