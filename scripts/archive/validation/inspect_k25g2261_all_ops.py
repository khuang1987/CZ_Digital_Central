import pandas as pd
import pyodbc

def list_batch_operations():
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
    print(f"Listing Operations for Batch: {batch_number}")
    
    query = f"""
    SELECT 
        Operation, Plant,
        TrackInTime, TrackOutTime, PreviousBatchEndTime, EnterStepTime,
        [PT(d)], [PNW(d)], CompletionStatus
    FROM dbo.mes_metrics_snapshot_a
    WHERE BatchNumber = '{batch_number}'
    ORDER BY TrackOutTime
    """
    
    df = pd.read_sql(query, conn)
    
    if df.empty:
        print("No records found.")
    else:
        # Calculate Gross PT for comparison
        # Logic: Start Time is max(EnterStepTime, PreviousBatchEndTime) usually, or TrackInTime.
        # Let's just print the raw values first.
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        print(df)
        
        # Check calendar for any row where PT is large but PNW is small
        print("\n--- Detailed Analysis ---")
        for i, row in df.iterrows():
            print(f"\nOp: {row['Operation']}")
            
            # Determine start time (Simplified logic for quick check)
            enter = pd.to_datetime(row['EnterStepTime']) if pd.notna(row['EnterStepTime']) else None
            prev = pd.to_datetime(row['PreviousBatchEndTime']) if pd.notna(row['PreviousBatchEndTime']) else None
            track_in = pd.to_datetime(row['TrackInTime']) if pd.notna(row['TrackInTime']) else None
            track_out = pd.to_datetime(row['TrackOutTime'])
            
            start_time = None
            if enter and prev and enter > prev:
                start_time = track_in if track_in else prev # Gap logic
            elif prev:
                start_time = prev # Continuous logic
            elif track_in:
                start_time = track_in
                
            if start_time:
                gross_duration = track_out - start_time
                gross_days = gross_duration.total_seconds() / 86400.0
                pnw = float(row['PNW(d)']) if row['PNW(d)'] else 0.0
                pt = float(row['PT(d)']) if row['PT(d)'] else 0.0
                
                print(f"  Start: {start_time}")
                print(f"  End:   {track_out}")
                print(f"  Gross: {gross_days:.4f} d")
                print(f"  PNW:   {pnw:.4f} d")
                print(f"  NetPT: {pt:.4f} d")
                print(f"  Calc:  {gross_days:.4f} - {pnw:.4f} = {gross_days - pnw:.4f}")
                
                # Check for weekend if duration is long
                if gross_days > 1:
                    print("  -> Long duration, checking calendar...")
                    cal_query = f"""
                    SELECT CalendarDate, IsWorkday 
                    FROM dbo.raw_calendar 
                    WHERE CalendarDate BETWEEN ? AND ? 
                    """
                    try:
                        cal_df = pd.read_sql(cal_query, conn, params=[start_time.date(), track_out.date()])
                        non_work = cal_df[cal_df['IsWorkday'] == False]
                        print(f"  Calendar Non-Workdays found: {len(non_work)}")
                        if not non_work.empty:
                            print(f"  Dates: {non_work['CalendarDate'].dt.date.tolist()}")
                    except Exception as e:
                        print(f"  Calendar check failed: {e}")

    conn.close()

if __name__ == "__main__":
    list_batch_operations()
