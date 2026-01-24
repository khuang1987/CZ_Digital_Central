"""
Unified Data Collection Pipeline Runner

Usage:
    python scripts/run_data_collection.py [planner|cmes|labor|all] [--headless]

Options:
    --headless    Run browsers in headless mode (default: True, use --no-headless to disable)
"""

import sys
import argparse
import logging
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("DataCollectionPipeline")

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

def main():
    parser = argparse.ArgumentParser(description="Run Data Collection Pipelines")
    parser.add_argument("target", choices=["planner", "cmes", "labor", "all"], help="Target pipeline to run")
    parser.add_argument("--headless", action="store_true", default=True, help="Run in headless mode")
    parser.add_argument("--no-headless", action="store_false", dest="headless", help="Show browser UI")
    
    args = parser.parse_args()
    
    if args.target == "planner" or args.target == "all":
        run_planner(headless=args.headless)
        
    if args.target == "cmes" or args.target == "all":
        run_cmes(headless=args.headless)
        
    if args.target == "labor" or args.target == "all":
        run_labor(headless=args.headless)

if __name__ == "__main__":
    main()
