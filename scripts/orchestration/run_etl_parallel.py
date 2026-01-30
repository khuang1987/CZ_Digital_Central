import os
import sys
import time
import logging
import subprocess
import concurrent.futures
import multiprocessing
import pyodbc 
from typing import List, Dict, Any, Tuple
from datetime import datetime

# ============================================================
# Configuration
# ============================================================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOG_DIR = os.path.join(PROJECT_ROOT, "shared_infrastructure", "logs")
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

def cleanup_logs(keep_days: int = 7):
    """
    Clean up log files older than keep_days in the log directory and subdirectories.
    """
    now = time.time()
    cutoff = now - (keep_days * 86400)
    
    deleted_count = 0
    
    for root, dirs, files in os.walk(LOG_DIR):
        for file in files:
            if file.endswith(".log"):
                file_path = os.path.join(root, file)
                try:
                    mtime = os.path.getmtime(file_path)
                    if mtime < cutoff:
                        os.remove(file_path)
                        deleted_count += 1
                        # print(f"Deleted old log: {file_path}") # Optional verbose
                except Exception as e:
                    print(f"Failed to delete old log {file_path}: {e}")
    
    if deleted_count > 0:
        print(f"Cleaned up {deleted_count} log files older than {keep_days} days.")

# Perform cleanup on startup
cleanup_logs(keep_days=7)

# Generate single log filename for this run
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = os.path.join(LOG_DIR, f"orchestrator_{TIMESTAMP}.log")

# ============================================================
# Task Definitions (Stages)
# ============================================================
STAGES = [
    {
        "name": "0. Data Collection",
        "tasks": [
            # Runs unified downloader for Planner, CMES/MES, and Labor Formatting
            # Warning: Browsers are heavy, running in parallel might be risky if resources are low, 
            # but orchestrator runs stages sequentially so it's fine.
            # Within this stage, we only have one task 'Data Collection (All)' to keep it simple.
            {"name": "Data Collection (All)", "script": "scripts/orchestration/run_data_collection.py", "args": ["all"]},
        ]
    },
    {
        "name": "1. Raw Data & Dimensions",
        "tasks": [
            {"name": "SAP Routing Raw",       "script": "data_pipelines/sources/sap/etl/etl_sap_routing_raw.py"},
            {"name": "SFC Batch Raw",         "script": "data_pipelines/sources/sfc/etl/etl_sfc_batch_output_raw.py", "args": ["--max-new-files", "22"]},
            {"name": "SFC Inspection Raw",    "script": "data_pipelines/sources/sfc/etl/etl_sfc_inspection_raw.py", "args": ["--max-new-files", "22"]},
            {"name": "MES Batch Raw",         "script": "data_pipelines/sources/mes/etl/etl_mes_batch_output_raw.py"},
            {"name": "Planner Tasks",         "script": "data_pipelines/sources/planner/etl/etl_planner_tasks_raw.py"},
            {"name": "Calendar Dim",          "script": "data_pipelines/sources/dimension/etl/etl_calendar.py"},
            {"name": "Operation Mapping",     "script": "data_pipelines/sources/dimension/etl/etl_operation_mapping.py"},
            {"name": "SAP Labor Hours",       "script": "data_pipelines/sources/sap/etl/etl_sap_labor_hours.py", "args": ["--ypp"]},
            {"name": "SAP 9997 GI",           "script": "data_pipelines/sources/sap/etl/etl_sap_gi_9997.py"},
        ]
    },
    {
        "name": "2. WIP & Calculations",
        "tasks": [
            {"name": "SFC WIP CZM",           "script": "data_pipelines/sources/sfc/etl/etl_sfc_wip_czm.py", "args": ["--mode", "latest"]},
            {"name": "CMES WIP",              "script": "data_pipelines/sources/mes/etl/etl_mes_wip_cmes.py", "args": ["--days", "7"]},
            {"name": "SFC Repair",            "script": "data_pipelines/sources/sfc/etl/etl_sfc_repair.py", "args": ["--mode", "latest"]},
        ]
    },
    {
        "name": "3. Materialized Views",
        "tasks": [
            {"name": "MES Metrics Materialized", "script": "scripts/maintenance/_refresh_mes_metrics_materialized.py"},
        ]
    },
    {
        "name": "4. Export & Validation",
        "tasks": [
            {"name": "Export to A1",          "script": "scripts/orchestration/export_core_to_a1.py", "args": ["--mode", "partitioned", "--reconcile", "--reconcile-last-n", "2", "--meta-store", "sql"]},
            {"name": "Validation Postcheck",  "script": "scripts/orchestration/sqlserver_postcheck.py"},
            {"name": "Meta Summary",          "script": "data_pipelines/monitoring/etl/etl_meta_table_health.py"},
        ]
    },
    {
        "name": "5. Dashboard & Reports",
        "tasks": [
            # 1. PowerBI Refresh (New migration)
            # Use 'run_data_collection.py refresh'
            {"name": "PowerBI Refresh",            "script": "scripts/orchestration/run_data_collection.py", "args": ["refresh"]},
            
            # 2. Start Dashboard Service (Non-blocking launch)
            {"name": "Start Dashboard Service",    "script": "scripts/orchestration/launch_services.py"},
        ]
    }
]

# ============================================================
# Logging Setup (Queue-based)
# ============================================================
# ============================================================
# Logging Setup (Buffered)
# ============================================================
def setup_logging():
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    
    # File Handler
    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    
    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    root.addHandler(file_handler)
    root.addHandler(console_handler)

# ============================================================
# Execution Logic
# ============================================================
def run_task(task: Dict[str, Any]) -> Dict[str, Any]:
    """Runs a single task in a subprocess, capturing output."""
    name = task["name"]
    script_rel_path = task["script"]
    args = task.get("args", [])
    stream_output = task.get("stream_output", False)
    
    # Buffer for logs
    log_buffer = []

    def log(msg):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        full_msg = f"[{timestamp}] [{name}] {msg}"
        log_buffer.append(full_msg)
        if stream_output:
            print(full_msg)

    log(f"STARTING...")
    start_time = time.time()
    
    script_path = os.path.join(PROJECT_ROOT, script_rel_path)
    if not os.path.exists(script_path):
        log(f"ERROR: Script not found: {script_path}")
        return {
            'name': name, 
            'success': False, 
            'duration': 0, 
            'error': 'Script not found',
            'output': "\n".join(log_buffer),
            'streamed': stream_output
        }

    # Use the same python interpreter as the orchestrator
    cmd = [sys.executable, script_path] + args
    
    # Set environment variables for encoding
    env = os.environ.copy()
    current_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{PROJECT_ROOT}{os.pathsep}{current_pythonpath}"
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"

    try:
        # Run and capture output
        if stream_output:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=PROJECT_ROOT,
                env=env,
                text=True,
                encoding='utf-8',
                errors='replace',
                bufsize=1  # Line buffered
            )
            
            # Read stdout line by line
            for line in process.stdout:
                line_stripped = line.strip()
                if line_stripped:
                    print(line_stripped)  # Real-time print
                    log_buffer.append(line_stripped)
            
            process.wait()
            process_returncode = process.returncode
            success = process_returncode == 0
            
        else:
            # Traditional capture
            process = subprocess.run(
                cmd,
                capture_output=True,
                cwd=PROJECT_ROOT,
                env=env,
                text=True,
                encoding='utf-8',
                errors='replace'
            )
            
            if process.stdout:
                log_buffer.append(process.stdout.strip())
            if process.stderr:
                log_buffer.append(process.stderr.strip())
                
            process_returncode = process.returncode
            success = process.returncode == 0

        duration = time.time() - start_time
        status = "SUCCESS" if success else "FAILED"
        
        if not success:
            log(f"Task failed with return code {process_returncode}")
        
        log(f"FINISHED - {status} ({duration:.2f}s)")
        
        return {
            'name': name,
            'success': success,
            'returncode': process_returncode,
            'duration': duration,
            'output': "\n".join(log_buffer),
            'streamed': stream_output
        }

    except Exception as e:
        log(f"Exception launching task: {e}")
        return {
            'name': name,
            'success': False,
            'duration': time.time() - start_time,
            'error': str(e),
            'output': "\n".join(log_buffer),
            'streamed': stream_output
        }

def run_stage(stage: Dict[str, Any], pool: concurrent.futures.ProcessPoolExecutor) -> Tuple[bool, List[Dict]]:
    stage_name = stage["name"]
    tasks = stage["tasks"]
    logging.info(f"=== STAGE: {stage_name} ===")
    
    stage_results = []
    failed = False
    
    # Inject streaming flag for Stage 0 (Data Collection)
    is_streaming_stage = stage_name.startswith("0.")
    if is_streaming_stage:
        for t in tasks:
            t["stream_output"] = True
    
    # Submit all tasks in this stage
    futures = {pool.submit(run_task, task): task for task in tasks}
    
    for future in concurrent.futures.as_completed(futures):
        result = future.result()
        stage_results.append(result)
        
        # Determine if we should print the log block
        was_streamed = result.get('streamed', False)
        
        if not was_streamed:
            # Print buffered logs (Existing behavior for non-streamed tasks)
            logging.info("")
            logging.info(f"==================================================================")
            logging.info(f"LOGS for TASK: {result['name']}")
            logging.info(f"==================================================================")
            if result.get('output'):
                for line in result['output'].splitlines():
                    logging.info(f"  {line}")
            else:
                logging.info("  (No output captured)")
            logging.info(f"==================================================================")
            logging.info("")
        else:
            # For streamed tasks, just print a small marker since logs are already visible
            logging.info("")
            logging.info(f">>> Task Completed: {result['name']} (Output above)")
            logging.info("")

        if not result['success']:
            failed = True
            
    logging.info(f"=== STAGE {stage_name} COMPLETED. Success: {not failed} ===")
    return not failed, stage_results

def get_db_stats() -> List[Dict]:
    """Fetches the latest table statistics from the database."""
    server = os.getenv("MDDAP_SQL_SERVER", r"localhost\SQLEXPRESS")
    database = os.getenv("MDDAP_SQL_DATABASE", "mddap_v2")
    driver = os.getenv("MDDAP_SQL_DRIVER", "ODBC Driver 17 for SQL Server")
    
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )
    
    try:
        with pyodbc.connect(conn_str, timeout=5) as conn:
            cursor = conn.cursor()
            # Get latest stats for today (snapshot_date is in local time usually)
            sql = """
            SELECT table_name, today_inserted, today_updated, row_count, last_updated_at
            FROM dbo.meta_table_stats_daily
            WHERE snapshot_date = CAST(GETDATE() AS DATE)
            ORDER BY row_count DESC
            """
            cursor.execute(sql)
            columns = [column[0] for column in cursor.description]
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            return results
    except Exception as e:
        # Don't fail the orchestrator just because stats collection failed
        return []

def print_execution_summary(all_results: List[Dict], total_duration: float):
    """
    Prints a beautiful summary table of the execution and data stats.
    """
    logging.info("")
    logging.info("==========================================================================================")
    logging.info("              MDDAP DATA PLATFORM - EXECUTION SUMMARY REPORT")
    logging.info("==========================================================================================")
    
    # 1. Script Execution Status
    logging.info(f"{'Task Name':<35} | {'Status':<10} | {'Time(s)':>10}")
    logging.info("-" * 62)
    
    success_count = 0
    failed_tasks = []
    
    # Sort results by execution order (approximate) or name
    all_results.sort(key=lambda x: x['name'])

    for res in all_results:
        status = "SUCCESS" if res['success'] else "FAILED"
        duration = res.get('duration', 0)
        if res['success']: 
            success_count += 1
        else:
            failed_tasks.append(res['name'])
        
        # Color-coded feel in logs (though plain text)
        logging.info(f"{res['name']:<35} | {status:<10} | {duration:>10.2f}")

    logging.info("-" * 62)
    logging.info(f"TASKS: {len(all_results)} processed, {success_count} succeeded, {len(all_results)-success_count} failed")
    logging.info(f"TOTAL REFRESH TIME: {total_duration:.2f}s")
    
    if failed_tasks:
        logging.info(f"FAILED TASKS: {', '.join(failed_tasks)}")
    
    # 2. Data Record Statistics (Fetched from DB)
    stats = get_db_stats()
    if stats:
        logging.info("")
        logging.info("==========================================================================================")
        logging.info("              DATA RECORD STATISTICS (Today's Activity)")
        logging.info("==========================================================================================")
        logging.info(f"{'Table Name':<35} | {'Inserted':<10} | {'Updated':<10} | {'Total Rows':>12}")
        logging.info("-" * 75)
        for s in stats:
            ins = s.get('today_inserted', 0) or 0
            upd = s.get('today_updated', 0) or 0
            total = s.get('row_count', 0) or 0
            logging.info(f"{s['table_name']:<35} | {ins:<10,} | {upd:<10,} | {total:>12,}")
        logging.info("-" * 75)
    else:
        logging.info("")
        logging.info("NOTE: Detailed record statistics not available yet (meta_table_health might have failed).")

    logging.info("==========================================================================================")
    logging.info("")


# ============================================================
# Main Entry Point
# ============================================================
def main():
    import argparse
    parser = argparse.ArgumentParser(description="MDDAP ETL Orchestrator")
    parser.add_argument("--only-collection", action="store_true", help="Only run Stage 0 (Data Collection), skip SQL/DB stages.")
    args = parser.parse_args()

    setup_logging()
    
    logging.info("MDDAP ETL Orchestrator Started")
    if args.only_collection:
        logging.info(">>> MODE: ONLY COLLECTION (System logic will skip SQL/DB stages)")
    
    logging.info(f"Project Root: {PROJECT_ROOT}")
    logging.info(f"Log File: {LOG_FILE}")
    
    start_total = time.time()
    all_results = []
    workflow_success = True

    # Use ProcessPoolExecutor
    with concurrent.futures.ProcessPoolExecutor(max_workers=5) as pool:
        for stage in STAGES:
            # Skip non-collection stages if requested
            if args.only_collection and not stage["name"].startswith("0."):
                logging.info(f"--- Skipping Stage: {stage['name']} (Collection-only mode) ---")
                continue

            success, stage_res = run_stage(stage, pool)
            all_results.extend(stage_res)
            if not success:
                logging.error(f"Stage {stage['name']} failed. Stopping workflow.")
                workflow_success = False
                break
    
    total_duration = time.time() - start_total
    print_execution_summary(all_results, total_duration)
    
    logging.info("Orchestration workflow completed.")
    
    sys.exit(0 if workflow_success else 1)

if __name__ == "__main__":
    # Windows support for multiprocessing
    multiprocessing.freeze_support()
    main()
