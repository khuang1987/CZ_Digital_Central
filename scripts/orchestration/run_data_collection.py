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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("DataCollectionPipeline")

# ==============================================================================
# Smart Auth Check Logic
# ==============================================================================

# Smart Auth Check Logic Removed per user request 2026-01-27


def run_planner(headless=True):
    logger.info(">>> Starting Planner Data Collection...")
    try:
        from data_pipelines.sources.planner.download.planner_downloader import export_planner_data
        success = export_planner_data(headless=headless)
        if success:
            logger.info(">>> Planner Collection Completed Successfully.")
        else:
            logger.error(">>> Planner Collection Failed.")
    except Exception as e:
        logger.exception(f">>> Planner Collection Error: {e}")

def run_cmes(headless=True):
    logger.info(">>> Starting CMES/MES Data Collection...")
    try:
        from data_pipelines.sources.mes.download.cmes_downloader import collect_cmes_data
        collect_cmes_data(headless=headless)
        logger.info(">>> CMES/MES Collection Completed.")
    except Exception as e:
        logger.exception(f">>> CMES/MES Collection Error: {e}")

def run_labor(headless=True):
    # Headless arg is ignored for labor formatter as it uses COM
    logger.info(">>> Starting Labor Hour/SAP Formatting...")
    try:
        from data_pipelines.sources.sap.download.labor_hour_formatter import format_labor_hour
        success = format_labor_hour()
        if success:
            logger.info(">>> Labor Hour/SAP Formatting Completed Successfully.")
        else:
            logger.error(">>> Labor Hour/SAP Formatting Failed.")
    except Exception as e:
        logger.exception(f">>> Labor Hour/SAP Formatting Error: {e}")

def run_tooling(headless=True):
    logger.info(">>> Starting Tooling Data Export...")
    try:
        from data_pipelines.sources.tooling.download.transaction_exporter import export_tooling_data
        success = export_tooling_data()
        if success:
            logger.info(">>> Tooling Export Completed Successfully.")
        else:
            logger.error(">>> Tooling Export Failed.")
    except Exception as e:
        logger.exception(f">>> Tooling Export Error: {e}")

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
    # Updated choices to include 'tooling' and 'refresh'
    parser.add_argument("target", choices=["planner", "cmes", "labor", "tooling", "refresh", "all"], nargs='?', default='all', help="Target pipeline to run (default: all=planner+cmes+labor+tooling)")
    parser.add_argument("--headless", action="store_true", default=False, help="Run in headless mode (default: False for debugging)")
    parser.add_argument("--no-headless", action="store_false", dest="headless", help="Show browser UI (redundant if default is False)")
    
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
    
    if args.target in ["planner", "cmes", "tooling", "refresh", "all"]:
         mode_str = "HEADLESS" if final_headless else "HEADED (Visible Browser)"
         logger.info(f">>> Mode: {mode_str}")
    
    # Execution Sequence
    if args.target == "refresh":
        run_refresh(headless=final_headless)
        return # Refresh is usually standalone

    if args.target == "planner" or args.target == "all":
        run_planner(headless=final_headless)
        
    if args.target == "cmes" or args.target == "all":
        run_cmes(headless=final_headless)
        
    if args.target == "labor" or args.target == "all":
        run_labor(headless=final_headless)
        
    if args.target == "tooling" or args.target == "all":
        run_tooling(headless=final_headless)

if __name__ == "__main__":
    main()
