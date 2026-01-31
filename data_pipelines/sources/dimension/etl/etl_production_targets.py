"""
Production Target (Planned EH) ETL Script
Source: Excel file from OneDrive
Logic: Monthly update, skip if no change, filter by date, calculate workday based on null values.
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd

# Setup paths
PROJECT_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(PROJECT_ROOT))

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

# Configure logging
LOG_DIR = os.path.join(PROJECT_ROOT, "shared_infrastructure", "logs", "production")
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, f"etl_production_targets_{datetime.now().strftime('%Y%m%d')}.log"), encoding='utf-8'),
        logging.StreamHandler()
    ]
)

EXCEL_PATH = r"C:\Users\huangk14\OneDrive - Medtronic PLC\General - CZ OPS生产每日产出登记\01-常州园区每日计划工时.xlsx"
ETL_NAME = "production_targets"
TABLE_NAME = "dim_production_targets"

def get_db_manager() -> SQLServerOnlyManager:
    return SQLServerOnlyManager()

def main():
    logging.info("Starting Production Targets ETL...")
    db = get_db_manager()
    
    # 1. Check for changes
    if not os.path.exists(EXCEL_PATH):
        logging.error(f"Excel file not found: {EXCEL_PATH}")
        return

    changed_files = db.filter_changed_files(ETL_NAME, [EXCEL_PATH])
    if not changed_files:
        logging.info("Excel file has not changed. Skipping ETL.")
        return

    # 2. Read and Clean Data
    try:
        # Load Sheet1
        df = pd.read_excel(EXCEL_PATH, sheet_name='Sheet1')
        
        # Verify columns
        required_cols = ['日期', '康辉运营工时', '常州运营工时']
        for col in required_cols:
            if col not in df.columns:
                logging.error(f"Missing required column: {col}")
                return

        # Select and rename
        df_clean = df[required_cols].copy()
        df_clean.columns = ['Date', 'target_eh_9997', 'target_eh_1303']
        
        # Convert Date
        df_clean['Date'] = pd.to_datetime(df_clean['Date'], errors='coerce')
        df_clean = df_clean.dropna(subset=['Date'])
        
        # Calculate is_workday
        # Logic: If both target hours are null/NaN, it's not a workday
        df_clean['is_workday'] = df_clean.apply(
            lambda x: 0 if pd.isna(x['target_eh_9997']) and pd.isna(x['target_eh_1303']) else 1,
            axis=1
        )
        
        # Handle EH values
        df_clean['target_eh_9997'] = pd.to_numeric(df_clean['target_eh_9997'], errors='coerce')
        df_clean['target_eh_1303'] = pd.to_numeric(df_clean['target_eh_1303'], errors='coerce')
        
        logging.info(f"Read {len(df_clean)} rows from Excel.")

        # 3. Save to Database
        # Note: We use merge/update logic based on Date PK
        # Using staging table approach or simple delete/insert for targets
        with db.get_connection() as conn:
            cursor = conn.cursor()
            
            # Simple approach: Since the file is small (monthly/yearly targets), 
            # we can truncate and refill or do selective updates.
            # User wants "like routing", routing uses record_hash.
            # Here Date is the PK.
            
            # Generate record_hash for consistency with Manager methods if needed, 
            # but Date is fine as key.
            # We'll use a direct upsert logic.
            
            # Prepare data with native types (convert NaN to None)
            inserted = 0
            for _, row in df_clean.iterrows():
                # Extract values and convert to native Python types
                dt = row['Date'].to_pydatetime().date()
                target_9997 = float(row['target_eh_9997']) if pd.notna(row['target_eh_9997']) else None
                target_1303 = float(row['target_eh_1303']) if pd.notna(row['target_eh_1303']) else None
                workday = int(row['is_workday'])

                try:
                    cursor.execute("""
                        MERGE INTO dim_production_targets AS target
                        USING (SELECT ? AS Date, ? AS target_eh_9997, ? AS target_eh_1303, ? AS is_workday) AS source
                        ON target.Date = source.Date
                        WHEN MATCHED THEN
                            UPDATE SET target_eh_9997 = source.target_eh_9997, 
                                       target_eh_1303 = source.target_eh_1303,
                                       is_workday = source.is_workday
                        WHEN NOT MATCHED THEN
                            INSERT (Date, target_eh_9997, target_eh_1303, is_workday)
                            VALUES (source.Date, source.target_eh_9997, source.target_eh_1303, source.is_workday);
                    """, (dt, target_9997, target_1303, workday))
                    inserted += 1
                except Exception as row_e:
                    logging.error(f"Failed to upsert row {dt}: {row_e}")
                    logging.error(f"Values: target_9997={target_9997}, target_1303={target_1303}, workday={workday}")
                    raise
            
            conn.commit()
            logging.info(f"Upserted {inserted} records into {TABLE_NAME}.")

        # 4. Mark processed
        db.mark_file_processed(ETL_NAME, EXCEL_PATH)
        logging.info("ETL completed successfully.")

    except Exception as e:
        logging.exception(f"ETL failed: {e}")

if __name__ == "__main__":
    main()
