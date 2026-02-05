import os
import sys
import yaml
import pandas as pd
import pyodbc
import logging
from datetime import datetime
from pathlib import Path

# ============================================================
# Configuration & Setup
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared_infrastructure.export_utils import get_default_output_dir, _df_to_parquet_with_schema

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
    # Use standard paths (fallback to defaults if file missing)
    config_path = PROJECT_ROOT / "shared_infrastructure" / "config" / "config_unified.yaml"
    paths_path = PROJECT_ROOT / "shared_infrastructure" / "config" / "paths.yaml"
    
    config = {}
    if config_path.exists():
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f) or {}
            
    paths = {}
    if paths_path.exists():
        with open(paths_path, 'r', encoding='utf-8') as f:
            paths = yaml.safe_load(f) or {}
            
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
    logging.info("Starting Output Validation Gate (A1 Direct)...")
    
    config, paths = load_configs()
    output_dir = get_default_output_dir()
    logging.info(f"Using Output Directory: {output_dir}")

    # DB Connection
    server = os.getenv("MDDAP_SQL_SERVER", r"localhost\SQLEXPRESS")
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
    
    # Map SQL tables to their parquet exports in A1 (relative to output_dir)
    # Using specific files that exist in 01_CURATED_STATIC and 03_METADATA
    targets = [
        {"table": "dbo.raw_sap_routing", "parquet": "01_CURATED_STATIC/sap_routing.parquet"},
        {"table": "dbo.planner_task_labels", "parquet": "01_CURATED_STATIC/planner_task_labels.parquet"},
        {"table": "dbo.dim_operation_mapping", "parquet": "01_CURATED_STATIC/dim_operation_mapping.parquet"},
        {"table": "dbo.TriggerCaseRegistry", "parquet": "03_METADATA/core_tables/TriggerCaseRegistry.parquet"},
        {"table": "dbo.KPI_Data", "parquet": "03_METADATA/core_tables/KPI_Data.parquet"},
    ]
    
    results = []
    success = True
    
    try:
        with pyodbc.connect(conn_str) as conn:
            for target in targets:
                table_name = target['table']
                parquet_rel_path = target['parquet']
                parquet_path = output_dir / parquet_rel_path
                
                if not parquet_path.exists():
                    logging.warning(f"Parquet file not found: {parquet_rel_path}")
                    continue
                
                # Get Counts
                sql_count = get_sql_count(table_name, conn)
                df_parquet = pd.read_parquet(parquet_path)
                parquet_count = len(df_parquet)
                
                # Compare
                diff = abs(sql_count - parquet_count)
                variance = diff / sql_count if sql_count > 0 else (1.0 if parquet_count > 0 else 0)
                score = max(0.0, 100.0 * (1.0 - variance))
                
                status = "PASS" if variance <= threshold else "FAIL"
                logging.info(f"Checking {table_name}:")
                logging.info(f"  - SQL Count:     {sql_count}")
                logging.info(f"  - Parquet Count: {parquet_count}")
                logging.info(f"  - Variance:      {variance:.4%} (Score: {score:.1f}/100)")
                logging.info(f"  - Result:        {status}")
                
                results.append({
                    "table_name": table_name,
                    "sql_count": sql_count,
                    "parquet_count": parquet_count,
                    "variance": variance,
                    "score": score,
                    "status": status,
                    "validated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

                if status == "FAIL":
                    success = False

        # Save results to metadata
        if results:
            df_results = pd.DataFrame(results)
            scores_path = output_dir / "03_METADATA" / "monitoring" / "validation_scores.parquet"
            # In a real app we might append, here we overwrite for latest status
            _df_to_parquet_with_schema(df_results, scores_path, None)
            logging.info(f"Validation summary saved to: {scores_path}")

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
