import os
import sys
import yaml
import pandas as pd
import pyodbc
import logging
from datetime import datetime

# ============================================================
# Configuration & Setup
# ============================================================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CONFIG_PATH = os.path.join(PROJECT_ROOT, "shared_infrastructure", "config", "config_unified.yaml")
PATHS_PATH = os.path.join(PROJECT_ROOT, "shared_infrastructure", "config", "paths.yaml")

def setup_logging():
    log_dir = os.path.join(PROJECT_ROOT, "shared_infrastructure", "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "validation_gate.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def load_configs():
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    with open(PATHS_PATH, 'r', encoding='utf-8') as f:
        paths = yaml.safe_load(f)
    return config, paths

# ============================================================
# Validation Logic
# ============================================================
def get_sql_count(table_name, conn):
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    return cursor.fetchone()[0]

def validate_outputs():
    setup_logging()
    logging.info("Starting Parquet Output Validation Gate...")
    
    try:
        config, paths = load_configs()
    except Exception as e:
        logging.error(f"Failed to load configuration: {e}")
        return False

    # DB Connection
    server = os.getenv("MDDAP_SQL_SERVER", paths['environment']['dev']['sql_server'])
    database = os.getenv("MDDAP_SQL_DATABASE", "mddap_v2")
    driver = "ODBC Driver 17 for SQL Server"
    
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )
    
    threshold = config.get('validation', {}).get('consistency_error_threshold', 0.005)
    
    # We validate specific core tables for this phase
    # In a full implementation, this could be dynamic based on config
    targets = [
        {"table": "dbo.fct_mes_batch_output_raw", "parquet": "data_pipelines/sources/mes/publish/mes_batch_output_raw.parquet"},
        {"table": "dbo.fct_sfc_batch_output_raw", "parquet": "data_pipelines/sources/sfc/publish/sfc_batch_output_raw.parquet"},
    ]
    
    success = True
    
    try:
        with pyodbc.connect(conn_str) as conn:
            for target in targets:
                table_name = target['table']
                parquet_rel_path = target['parquet']
                parquet_path = os.path.join(PROJECT_ROOT, parquet_rel_path)
                
                if not os.path.exists(parquet_path):
                    logging.warning(f"Parquet file not found, skipping: {parquet_rel_path}")
                    continue
                
                # Get Counts
                sql_count = get_sql_count(table_name, conn)
                df_parquet = pd.read_parquet(parquet_path)
                parquet_count = len(df_parquet)
                
                # Compare
                diff = abs(sql_count - parquet_count)
                variance = diff / sql_count if sql_count > 0 else 0
                
                status = "PASS" if variance <= threshold else "FAIL"
                logging.info(f"Checking {table_name}:")
                logging.info(f"  - SQL Count:     {sql_count}")
                logging.info(f"  - Parquet Count: {parquet_count}")
                logging.info(f"  - Variance:      {variance:.4%} (Threshold: {threshold:.4%})")
                logging.info(f"  - Result:        {status}")
                
                if status == "FAIL":
                    success = False

    except Exception as e:
        logging.error(f"Validation process failed with error: {e}")
        return False

    if success:
        logging.info("Validation Gate: ALL PASSED")
    else:
        logging.error("Validation Gate: FAILED - Data variance exceeds threshold")
        
    return success

if __name__ == "__main__":
    is_valid = validate_outputs()
    sys.exit(0 if is_valid else 1)
