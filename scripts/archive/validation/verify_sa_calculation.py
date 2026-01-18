import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Add project root to path to import modules if needed
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

DB_PATH = project_root / 'data_pipelines' / 'database' / 'mddap_v2.db'

def get_db_connection():
    return sqlite3.connect(DB_PATH)

def verify_calculations():
    print(f"Connecting to database: {DB_PATH}")
    conn = get_db_connection()
    
    # 1. Fetch data from v_mes_metrics
    # We fetch raw fields + calculated fields to compare
    query = """
    SELECT 
        BatchNumber, Operation, Machine,
        EnterStepTime, TrackInTime, TrackOutTime,
        TrackIn_SFC, PreviousBatchEndTime, IsSetup,
        TrackOutQuantity, ScrapQty,
        EH_machine, EH_labor, SetupTime, OEE,
        "LT(d)" as SQL_LT,
        "PT(d)" as SQL_PT,
        "ST(d)" as SQL_ST
    FROM v_mes_metrics
    WHERE TrackOutTime IS NOT NULL
    LIMIT 100
    """
    
    print("Fetching sample data (limit 100)...")
    try:
        df = pd.read_sql(query, conn)
    except Exception as e:
        print(f"Error reading data: {e}")
        return

    if df.empty:
        print("No data found in v_mes_metrics.")
        return

    print(f"Fetched {len(df)} records. Starting verification...")
    
    # Helper to parse dates
    def parse_date(x):
        return pd.to_datetime(x, errors='coerce')

    df['TrackOutTime_dt'] = df['TrackOutTime'].apply(parse_date)
    df['TrackInTime_dt'] = df['TrackInTime'].apply(parse_date)
    df['EnterStepTime_dt'] = df['EnterStepTime'].apply(parse_date)
    df['TrackIn_SFC_dt'] = df['TrackIn_SFC'].apply(parse_date)
    df['PreviousBatchEndTime_dt'] = df['PreviousBatchEndTime'].apply(parse_date)
    
    discrepancies = []

    for idx, row in df.iterrows():
        # --- Verify LT(d) ---
        # Logic matches SQL View:
        # If Op 0010/10: TrackIn_SFC -> EnterStepTime -> TrackInTime
        # Else: EnterStepTime
        start_time_lt = None
        op = str(row['Operation']).strip()
        
        if op in ['0010', '10']:
            if pd.notna(row['TrackIn_SFC_dt']):
                start_time_lt = row['TrackIn_SFC_dt']
            elif pd.notna(row['EnterStepTime_dt']):
                start_time_lt = row['EnterStepTime_dt']
            elif pd.notna(row['TrackInTime_dt']):
                start_time_lt = row['TrackInTime_dt']
        else:
            if pd.notna(row['EnterStepTime_dt']):
                start_time_lt = row['EnterStepTime_dt']
        
        calc_lt = None
        if start_time_lt and pd.notna(row['TrackOutTime_dt']):
            diff = row['TrackOutTime_dt'] - start_time_lt
            calc_lt = round(diff.total_seconds() / 86400.0, 2)
            
        # Compare LT
        # Allow small float diff
        sql_lt = row['SQL_LT']
        if pd.notna(calc_lt) and pd.notna(sql_lt):
            if abs(calc_lt - sql_lt) > 0.01:
                discrepancies.append({
                    'Batch': row['BatchNumber'], 'Op': row['Operation'],
                    'Type': 'LT', 'SQL': sql_lt, 'Python': calc_lt
                })
        elif pd.notna(calc_lt) != pd.notna(sql_lt):
             discrepancies.append({
                'Batch': row['BatchNumber'], 'Op': row['Operation'],
                'Type': 'LT_Existence', 'SQL': sql_lt, 'Python': calc_lt
            })

        # --- Verify PT(d) ---
        # Logic:
        # If EnterStepTime > PreviousBatchEndTime (Stop occurred): Use TrackInTime
        # Else If PreviousBatchEndTime exists: Use PreviousBatchEndTime
        # Else: Use TrackInTime
        start_time_pt = None
        
        has_stop = False
        if pd.notna(row['EnterStepTime_dt']) and pd.notna(row['PreviousBatchEndTime_dt']):
            if row['EnterStepTime_dt'] > row['PreviousBatchEndTime_dt']:
                has_stop = True
        
        if has_stop:
            start_time_pt = row['TrackInTime_dt']
        elif pd.notna(row['PreviousBatchEndTime_dt']):
            start_time_pt = row['PreviousBatchEndTime_dt']
        else:
            start_time_pt = row['TrackInTime_dt']
            
        calc_pt = None
        if pd.notna(start_time_pt) and pd.notna(row['TrackOutTime_dt']):
             diff = row['TrackOutTime_dt'] - start_time_pt
             calc_pt = round(diff.total_seconds() / 86400.0, 2)

        sql_pt = row['SQL_PT']
        if pd.notna(calc_pt) and pd.notna(sql_pt):
            if abs(calc_pt - sql_pt) > 0.01:
                discrepancies.append({
                    'Batch': row['BatchNumber'], 'Op': row['Operation'],
                    'Type': 'PT', 'SQL': sql_pt, 'Python': calc_pt,
                    'Debug': f"Start: {start_time_pt}, End: {row['TrackOutTime_dt']}, Prev: {row['PreviousBatchEndTime_dt']}, Enter: {row['EnterStepTime_dt']}"
                })

        # --- Verify ST(d) ---
        # Logic: (SetupTime + (Qty * UnitTime / OEE) + 0.5) / 24
        # SetupTime: row['SetupTime'] if IsSetup='Yes' else 0
        calc_st = None
        eh_machine = row['EH_machine']
        eh_labor = row['EH_labor']
        
        if pd.isna(eh_machine) and pd.isna(eh_labor):
            calc_st = None
        else:
            setup_val = row['SetupTime'] if row['IsSetup'] == 'Yes' and pd.notna(row['SetupTime']) else 0
            
            unit_time = eh_machine if pd.notna(eh_machine) else eh_labor
            if pd.isna(unit_time): unit_time = 0
            
            oee = row['OEE']
            if pd.isna(oee) or oee == 0: oee = 0.77
            
            qty = (row['TrackOutQuantity'] or 0) + (row['ScrapQty'] or 0)
            
            # Formula components
            # production_hours = (qty * unit_time_seconds) / 3600 / oee
            production_hours = (qty * unit_time) / 3600.0 / oee
            
            total_hours = setup_val + production_hours + 0.5 # 0.5 is changeover
            calc_st = round(total_hours / 24.0, 2)
            
        sql_st = row['SQL_ST']
        if pd.notna(calc_st) and pd.notna(sql_st):
             if abs(calc_st - sql_st) > 0.01:
                discrepancies.append({
                    'Batch': row['BatchNumber'], 'Op': row['Operation'],
                    'Type': 'ST', 'SQL': sql_st, 'Python': calc_st
                })


    print("\n=== Verification Results ===")
    if not discrepancies:
        print("✅ Success: All calculations match between SQL View and Python verification logic.")
    else:
        print(f"⚠️ Found {len(discrepancies)} discrepancies:")
        for d in discrepancies[:5]: # Show first 5
            print(d)
        if len(discrepancies) > 5:
            print(f"... and {len(discrepancies) - 5} more.")

if __name__ == "__main__":
    verify_calculations()
