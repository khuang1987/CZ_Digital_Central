import sys
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime

# Add project root needed for shared modules
PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager
from shared_infrastructure.export_utils import export_partitioned_table
from data_pipelines.sources.indirect_material.download.transaction_exporter import fetch_transactions

logger = logging.getLogger(__name__)

TABLE_NAME = "raw_indirect_material_transactions"
EXPORT_DATASET_NAME = "indirect_material"

def ensure_table(db: SQLServerOnlyManager):
    """Ensure the raw table exists."""
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            IF OBJECT_ID('dbo.{TABLE_NAME}', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.{TABLE_NAME} (
                    StartTime DATETIME2 NULL,
                    EndTime DATETIME2 NULL,
                    ItemNumber NVARCHAR(255) NULL,
                    ItemDescription NVARCHAR(MAX) NULL,
                    ItemGroup NVARCHAR(255) NULL,
                    JobNumber NVARCHAR(255) NULL,
                    BatchNumber NVARCHAR(255) NULL,
                    OperationNumber NVARCHAR(255) NULL,
                    MachineNumber NVARCHAR(255) NULL,
                    EmployeeName NVARCHAR(255) NULL,
                    Quantity FLOAT NULL,
                    VendingMachine NVARCHAR(255) NULL,
                    Location NVARCHAR(255) NULL,
                    Area NVARCHAR(255) NULL,
                    record_hash NVARCHAR(64) NULL,
                    inserted_at DATETIME2 DEFAULT GETDATE(),
                    updated_at DATETIME2 DEFAULT GETDATE()
                );
                CREATE INDEX IDX_{TABLE_NAME}_EndTime ON dbo.{TABLE_NAME}(EndTime);
            END
        """)
        conn.commit()

def calculate_hash(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate record hash for deduplication."""
    # Use key columns consistent with other ETLs if possible, or all columns
    # For logs, usually the timestamp + identifiers define unicity
    # Here we use all columns except metadata
    cols = ['StartTime', 'EndTime', 'ItemNumber', 'JobNumber', 'BatchNumber', 'EmployeeName', 'Quantity']
    
    # Helper for hashing
    import hashlib
    def _hash_row(row):
        s = "".join(str(x) for x in row.values)
        return hashlib.sha256(s.encode('utf-8')).hexdigest()

    df['record_hash'] = df[cols].apply(_hash_row, axis=1)
    return df

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize data types."""
    # Ensure datetimes
    df['StartTime'] = pd.to_datetime(df['StartTime'])
    df['EndTime'] = pd.to_datetime(df['EndTime'])
    
    # Clean string columns
    str_cols = ['ItemNumber', 'ItemDescription', 'ItemGroup', 'JobNumber', 'BatchNumber', 
                'OperationNumber', 'MachineNumber', 'EmployeeName', 'VendingMachine', 'Location', 'Area']
    for c in str_cols:
        if c in df.columns:
            df[c] = df[c].astype(str).replace('nan', '').replace('None', '')
            
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0)
    
    return calculate_hash(df)

def etl_indirect_material_transactions(headless: bool = True):
    """Main ETL function."""
    logger.info(">>> Starting Indirect Material ETL...")
    
    db = SQLServerOnlyManager()
    ensure_table(db)
    
    current_year = datetime.now().year
    # --- OPTIMIZATION: Incremental Download Strategy ---
    start_year = current_year - 4 # Default: Last 5 years if empty
    
    try:
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT MAX(EndTime) FROM dbo.{TABLE_NAME}")
            row = cursor.fetchone()
            if row and row[0]:
                last_date = row[0]
                # If we have data, start from the year of the last data
                # We also include the previous year just in case of late adjustments/sync issues
                # Logic: max(last_data_year, current_year - 1)
                # Why current_year - 1? Because sometimes late data for previous year arrives in Jan/Feb.
                db_year = last_date.year
                start_year = max(db_year, current_year - 1)
                logger.info(f"Incremental Strategy: Found data up to {last_date}. Starting download from {start_year}.")
            else:
                logger.info("Incremental Strategy: No existing data found. Starting full 5-year download.")
    except Exception as e:
        logger.warning(f"Failed to determine max date: {e}. Falling back to full download.")

    years = range(start_year, current_year + 1)
    
    # Track months for export
    affected_months = set()

    for year in years:
        try:
            # 1. Fetch from SPS
            df = fetch_transactions(year)
            
            if df.empty:
                logger.info(f"No transactions found for {year}")
                continue
                
            # 2. Clean
            df_cleaned = clean_data(df)
            
            # 3. Insert into SQL (Merge/Upsert)
            # Since transaction logs are usually immutable, we can use merge_insert_by_hash
            inserted = db.merge_insert_by_hash(df_cleaned, TABLE_NAME, hash_column='record_hash')
            logger.info(f"Year {year}: Upserted {inserted} / {len(df_cleaned)} rows")
            
            # Collect unique YYYY-MM for export
            # Assume EndTime is the business date
            if 'EndTime' in df_cleaned:
                months = df_cleaned['EndTime'].dt.to_period('M').astype(str).unique()
                affected_months.update(months)
                
        except Exception as e:
            logger.error(f"Failed to process year {year}: {e}")
            # Continue to next year? or execute what we have? 
            # Usually better to try all years.
            
    # 4. Export Parquet
    if affected_months:
        sorted_months = sorted(list(affected_months))
        logger.info(f"Exporting Parquet for months: {sorted_months}")
        
        from shared_infrastructure.export_utils import get_default_output_dir
        output_dir = get_default_output_dir()
        
        with db.get_connection() as conn:
            export_partitioned_table(
                conn, 
                dataset=EXPORT_DATASET_NAME,
                table_name=TABLE_NAME,
                date_col="EndTime",
                months=sorted_months,
                output_dir=output_dir,
                reconcile=True # Incremental check
            )
            
    logger.info(">>> Indirect Material ETL Completed.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    etl_indirect_material_transactions()
