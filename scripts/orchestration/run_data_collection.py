"""
Unified Data Collection Pipeline Runner

Usage:
    python scripts/run_data_collection.py [planner|cmes|labor|all] [--headless]

Options:
    --headless    Run browsers in headless mode (default: True, use --no-headless to disable)
"""

import os
import sys
import argparse
import logging
import datetime
import time
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Ensure environment is loaded from env_utils
try:
    from shared_infrastructure.env_utils import PROJECT_ROOT as _ROOT, ENV_FILE
except ImportError:
    pass

# Setup logging
# Setup logging
log_format = '%(asctime)s - %(levelname)s - %(message)s'
if os.getenv("MDDAP_ORCHESTRATOR_RUN"):
    # Orchestrator handles timestamping, so we use a concise format
    log_format = '%(levelname)s - %(message)s'

logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("DataCollectionPipeline")

# ==============================================================================
# Smart Auth Check Logic
# ==============================================================================


# ==============================================================================
# Helpers
# ==============================================================================

def _get_db_manager_for_state():
    from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager
    return SQLServerOnlyManager()

def check_run_allowed(component_name: str, force: bool = False) -> bool:
    """
    Check if run is allowed based on last run time (throttling 1 hour).
    Returns True if allowed, False otherwise.
    Now uses SQL Server based state.
    """
    if force:
        logger.info(f"[{component_name}] Force run enabled.")
        return True

    try:
        db = _get_db_manager_for_state()
        last_run_epoch = db.get_last_run_time(component_name)
        
        if last_run_epoch:
            elapsed = time.time() - last_run_epoch
            if elapsed < 3600: # 1 hour
                logger.info(f"[{component_name}] Skipped: Last run was {elapsed/60:.1f} mins ago (< 60 mins). Use --force to override.")
                return False
    except Exception as e:
        logger.warning(f"[{component_name}] Failed to check run allowed, defaulting to True: {e}")

    return True

def update_last_run(component_name: str):
    """Update last run timestamp in SQL Server."""
    try:
        db = _get_db_manager_for_state()
        db.update_run_state(component_name)
    except Exception as e:
        logger.error(f"[{component_name}] Failed to update run state: {e}")


def run_planner(headless=True, force=False):
    if not check_run_allowed("planner", force): return

    logger.info(">>> Starting Planner Data Collection...")
    try:
        from data_pipelines.sources.planner.download.planner_downloader import export_planner_data
        success = export_planner_data(headless=headless)
        
        # Immediate ETL
        if success:
            logger.info(">>> Planner Download Complete. Triggering ETL & Export...")
            try:
                # 假设 ETL 脚本暴露一个 run_etl(force_export=True) 方法
                # 目前由于 etl_planner_tasks_raw.py 可以在 main 运行，我们直接调用其 main 逻辑
                from data_pipelines.sources.planner.etl.etl_planner_tasks_raw import main as etl_main
                etl_main(force=True) 
                logger.info(">>> Planner ETL & Export Completed.")
            except Exception as etl_err:
                 logger.error(f">>> Planner ETL Failed: {etl_err}")

            update_last_run("planner")
            logger.info(">>> Planner Pipeline Completed Successfully.")
        else:
            logger.error(">>> Planner Collection Failed.")
    except Exception as e:
        logger.exception(f">>> Planner Collection Error: {e}")

def run_cmes(headless=True, force=False):
    if not check_run_allowed("cmes", force): return

    logger.info(">>> Starting CMES/MES Data Collection...")
    try:
        from data_pipelines.sources.mes.download.cmes_downloader import collect_cmes_data
        collect_cmes_data(headless=headless)
        
        # Immediate ETL
        logger.info(">>> CMES Download Complete. Triggering ETL & Export...")
        try:
             # etl_mes_wip_cmes.py
             from data_pipelines.sources.mes.etl.etl_mes_wip_cmes import main as etl_main
             etl_main([])
             logger.info(">>> CMES ETL & Export Completed.")
        except Exception as etl_err:
             logger.error(f">>> CMES ETL Failed: {etl_err}")

        update_last_run("cmes")
        logger.info(">>> CMES/MES Collection Completed.")
    except Exception as e:
        logger.exception(f">>> CMES/MES Collection Error: {e}")

def run_labor(headless=True, force=False):
    if not check_run_allowed("labor", force): return

    # Headless arg is ignored for labor formatter as it uses COM
    logger.info(">>> Starting Labor Hour/SAP Formatting...")
    try:
        from data_pipelines.sources.sap.download.labor_hour_formatter import format_labor_hour
        # labor_hour_formatter integrated export internally already
        success = format_labor_hour(force_refresh=force)
        
        if success:
            update_last_run("labor")
            logger.info(">>> Labor Hour/SAP Formatting Completed Successfully.")
        else:
            logger.error(">>> Labor Hour/SAP Formatting Failed.")
    except Exception as e:
        logger.exception(f">>> Labor Hour/SAP Formatting Error: {e}")

def run_indirect_material(headless=True, force=False):
    if not check_run_allowed("indirect_material", force): return

    logger.info(">>> Starting Indirect Material (Tooling etc.) ETC...")
    try:
        from data_pipelines.sources.indirect_material.etl.etl_indirect_transactions import etl_indirect_material_transactions
        etl_indirect_material_transactions(headless=headless)
        update_last_run("indirect_material")
        logger.info(">>> Indirect Material ETL Completed Successfully.")
    except Exception as e:
        logger.exception(f">>> Indirect Material ETL Error: {e}")

def run_sap_9997(headless=True, force=False):
    if not check_run_allowed("sap_9997", force): return
    
    logger.info(">>> Starting SAP 9997 GI Export...")
    try:
        # Directly run ETL which does file read + sql + export
        from data_pipelines.sources.sap.etl.etl_sap_gi_9997 import main as etl_main
        # Rebuild=False, Force=force
        ret = etl_main(['--force'] if force else [])
        update_last_run("sap_9997")
        logger.info(">>> SAP 9997 Export Completed.")
    except Exception as e:
        logger.exception(f">>> SAP 9997 Error: {e}")

def run_refresh(headless=True):
    logger.info(">>> Starting PowerBI Refresh...")
    try:
        from scripts.orchestration.trigger_pbi_refresh import trigger_refresh_all
        trigger_refresh_all(headless=headless)
        logger.info(">>> PowerBI Refresh Completed.")
    except Exception as e:
        logger.exception(f">>> PowerBI Refresh Error: {e}")

def main():
    parser = argparse.ArgumentParser(description="Run Data Collection Pipelines")
    # Updated choices to include 'indirect_material' and 'refresh'
    parser.add_argument("target", choices=["planner", "cmes", "labor", "indirect_material", "tooling", "sap9997", "refresh", "all"], nargs='?', default='all', help="Target pipeline")
    parser.add_argument("--headless", action="store_true", default=False, help="Run in headless mode")
    parser.add_argument("--no-headless", action="store_false", dest="headless", help="Show browser UI")
    parser.add_argument("--force", action="store_true", default=False, help="Force execution ignoring 1h limit")
    
    args = parser.parse_args()
    
    # Pre-flight check for .env variables
    ms_user = os.getenv("MDDAP_MS_USER")
    if not ms_user:
        logger.warning("="*50)
        logger.warning("⚠️  WARNING: MDDAP_MS_USER not found in environment!")
        logger.warning(f"Expected .env at: {PROJECT_ROOT / '.env'}")
        logger.warning("Please ensure .env exists and is correctly synced to the server.")
        logger.warning("="*50)
    
    final_headless = args.headless
    
    if args.target in ["planner", "cmes", "tooling", "indirect_material", "refresh", "all"]:
         mode_str = "HEADLESS" if final_headless else "HEADED (Visible Browser)"
         logger.info(f">>> Mode: {mode_str}")
    
    # Execution Sequence
    if args.target == "refresh":
        run_refresh(headless=final_headless)
        return # Refresh is usually standalone

    if args.target == "planner" or args.target == "all":
        run_planner(headless=final_headless, force=args.force)
        
    if args.target == "cmes" or args.target == "all":
        run_cmes(headless=final_headless, force=args.force)
        
    if args.target == "labor" or args.target == "all":
        run_labor(headless=final_headless, force=args.force)

    if args.target == "sap9997" or args.target == "all":
        run_sap_9997(headless=final_headless, force=args.force)
        
    if args.target == "indirect_material" or args.target == "tooling" or args.target == "all":
        run_indirect_material(headless=final_headless, force=args.force)

if __name__ == "__main__":
    main()
