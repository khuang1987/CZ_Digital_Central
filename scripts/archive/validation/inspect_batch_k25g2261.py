import pandas as pd
import pyodbc
from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

def inspect_batch():
    conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost\\SQLEXPRESS;DATABASE=mddap_v2;Trusted_Connection=yes;"
    conn = pyodbc.connect(conn_str)
    
    query = """
    SELECT 
        BatchNumber, Operation, Plant, 
        TrackInTime, TrackOutTime, PreviousBatchEndTime, EnterStepTime, 
        [PT(d)], [PNW(d)], CompletionStatus
    FROM dbo.mes_metrics_snapshot_a 
    WHERE BatchNumber = 'K25G2261'
    """
    
    df = pd.read_sql(query, conn)
    print(df.to_string())
    
    if not df.empty:
        plant = df.iloc[0]['Plant']
        start_time = df.iloc[0]['PreviousBatchEndTime']  # Assuming continuous logic usually, but check
        end_time = df.iloc[0]['TrackOutTime']
        print(f"\nChecking calendar for Plant {plant} between {start_time} and {end_time}")
        
        cal_query = f"""
        SELECT date_value, is_workday 
        FROM dbo.raw_calendar 
        WHERE factory_code = '{plant}' 
          AND date_value BETWEEN '{start_time}' AND '{end_time}'
        ORDER BY date_value
        """
        cal_df = pd.read_sql(cal_query, conn)
        print(cal_df)

if __name__ == "__main__":
    inspect_batch()
