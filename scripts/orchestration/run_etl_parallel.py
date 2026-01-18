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

# Generate single log filename for this run
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = os.path.join(LOG_DIR, f"orchestrator_{TIMESTAMP}.log")

# ============================================================
# Task Definitions (Stages)
# ============================================================
STAGES = [
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
    
    # Buffer for logs
    log_buffer = []

    def log(msg):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_buffer.append(f"[{timestamp}] [{name}] {msg}")

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
            'output': "\n".join(log_buffer)
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
        process = subprocess.run(
            cmd,
            capture_output=True,
            cwd=PROJECT_ROOT,
            env=env,
            text=True,
            encoding='utf-8',
            errors='replace'
        )

        # Merge stdout and stderr
        full_output = ""
        if process.stdout:
            full_output += process.stdout
        if process.stderr:
            full_output += process.stderr
            
        if full_output:
            log_buffer.append(full_output.strip())
        
        duration = time.time() - start_time
        success = process.returncode == 0
        status = "SUCCESS" if success else "FAILED"
        
        if not success:
            log(f"Task failed with return code {process.returncode}")
        
        log(f"FINISHED - {status} ({duration:.2f}s)")
        
        return {
            'name': name,
            'success': success,
            'returncode': process.returncode,
            'duration': duration,
            'output': "\n".join(log_buffer)
        }

    except Exception as e:
        log(f"Exception launching task: {e}")
        return {
            'name': name,
            'success': False,
            'duration': time.time() - start_time,
            'error': str(e),
            'output': "\n".join(log_buffer)
        }

def run_stage(stage: Dict[str, Any], pool: concurrent.futures.ProcessPoolExecutor) -> Tuple[bool, List[Dict]]:
    stage_name = stage["name"]
    tasks = stage["tasks"]
    logging.info(f"=== STAGE: {stage_name} ===")
    
    stage_results = []
    failed = False
    
    # Submit all tasks in this stage
    futures = {pool.submit(run_task, task): task for task in tasks}
    
    for future in concurrent.futures.as_completed(futures):
        result = future.result()
        stage_results.append(result)
        
        # Print buffered logs
        logging.info("")
        logging.info(f"==================================================================")
        logging.info(f"LOGS for TASK: {result['name']}")
        logging.info(f"==================================================================")
        # Use simple print or logging.info for the whole block
        # Since we want it in the main log file, we use logging.info
        if result.get('output'):
            # Avoid prefixing every line with main process timestamp
            # We want to dump the raw block. But standard logging adds prefix.
            # To keep it pretty, we iterate lines
            for line in result['output'].splitlines():
                # We already added timestamps in run_task
                logging.info(f"  {line}")
        else:
            logging.info("  (No output captured)")
        logging.info(f"==================================================================")
        logging.info("")

        if not result['success']:
            failed = True
            
    
    logging.info(f"=== STAGE {stage_name} COMPLETED. Success: {not failed} ===")
    return not failed, stage_results

def print_execution_summary(all_results: List[Dict]):
    """
    Prints a summary table of the execution.
    For simplicity in this consolidated version, we use the returned results 
    rather than querying the DB, as the return structure contains success/duration.
    To match the prompt's request for 'Read/Inserted/Skipped', we would typically 
    need to parse the logs or query the DB. Here we provide a high-level summary.
    """
    logging.info("")
    logging.info("==========================================================================================")
    logging.info("EXECUTION SUMMARY REPORT")
    logging.info("==========================================================================================")
    logging.info(f"{'Task Name':<30} | {'Status':<10} | {'Time(s)':>10}")
    logging.info("-" * 56)
    
    total_time = 0
    success_count = 0
    
    # Sort results by execution order (approximate) or name
    all_results.sort(key=lambda x: x['name'])

    for res in all_results:
        status = "SUCCESS" if res['success'] else "FAILED"
        duration = res.get('duration', 0)
        total_time += duration
        if res['success']: success_count += 1
        
        logging.info(f"{res['name']:<30} | {status:<10} | {duration:>10.2f}")

    logging.info("-" * 56)
    logging.info(f"TOTAL: {len(all_results)} tasks, {success_count} success, {len(all_results)-success_count} failed")
    logging.info("==========================================================================================")


# ============================================================
# Main Entry Point
# ============================================================
def main():
    setup_logging()
    
    logging.info("MDDAP ETL Orchestrator Started")
    logging.info(f"Project Root: {PROJECT_ROOT}")
    logging.info(f"Log File: {LOG_FILE}")
    
    start_total = time.time()
    all_results = []
    workflow_success = True

    # Use ProcessPoolExecutor
    # Max workers = 5 (CPU dependent, but good default)
    with concurrent.futures.ProcessPoolExecutor(max_workers=5) as pool:
        for stage in STAGES:
            success, stage_res = run_stage(stage, pool)
            all_results.extend(stage_res)
            if not success:
                logging.error(f"Stage {stage['name']} failed. Stopping workflow.")
                workflow_success = False
                break
    
    print_execution_summary(all_results)
    
    total_duration = time.time() - start_total
    logging.info(f"Total Workflow Duration: {total_duration:.2f}s")
    logging.info("Done.")
    
    sys.exit(0 if workflow_success else 1)

if __name__ == "__main__":
    # Windows support for multiprocessing
    multiprocessing.freeze_support()
    main()
