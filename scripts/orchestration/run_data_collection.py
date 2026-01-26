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
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("DataCollectionPipeline")

# ==============================================================================
# Smart Auth Check Logic
# ==============================================================================

def get_test_urls():
    """Retrieves one Planner URL and one CMES URL for testing authentication."""
    planner_url = None
    cmes_url = None
    
    # Get Planner URL
    try:
        from data_pipelines.sources.planner.download.planner_downloader import PROJECT_ROOT as DL_ROOT
        planner_config = DL_ROOT / "data_pipelines" / "sources" / "planner" / "config" / "planner_urls.csv"
        if planner_config.exists():
            import pandas as pd
            df = pd.read_csv(planner_config)
            if not df.empty and 'URL' in df.columns:
                planner_url = df['URL'].iloc[0]
    except Exception as e:
        logger.warning(f"Failed to get test Planner URL: {e}")

    # Get CMES URL
    try:
        from data_pipelines.sources.mes.download.cmes_downloader import get_cmes_config
        cmes_configs = get_cmes_config()
        if cmes_configs:
            cmes_url = cmes_configs[0]['url']
    except Exception as e:
        logger.warning(f"Failed to get test CMES URL: {e}")
        
    return planner_url, cmes_url

def check_login_required() -> bool:
    """
    Checks if login is required by visiting test URLs in headless mode.
    Returns True if login is required (login page detected), False otherwise.
    """
    logger.info(">>> Performing Smart Auth Check (Headless)...")
    
    planner_url, cmes_url = get_test_urls()
    if not planner_url and not cmes_url:
        logger.warning(">>> No URLs found for auth check. Skipping check.")
        return False # Assume logged in or can't check
    
    login_detected = False
    manager = None
    try:
        from shared_infrastructure.automation.playwright_manager import PlaywrightManager
        manager = PlaywrightManager(headless=True, use_user_profile=True)
        manager.start()
        page = manager.new_page()
        
        urls_to_check = []
        if planner_url: urls_to_check.append(("Planner", planner_url))
        if cmes_url: urls_to_check.append(("CMES", cmes_url))
        
        for name, url in urls_to_check:
            logger.info(f">>> Checking {name} access...")
            try:
                page.goto(url, timeout=30000, wait_until="domcontentloaded")
                # Wait a bit for redirects
                page.wait_for_timeout(3000)
                
                # Check URL for login domains
                current_url = page.url.lower()
                if any(x in current_url for x in ["login.microsoftonline.com", "login.live.com", "signin", "f5.medtronic.com"]):
                    logger.warning(f"!!! Login prompt detected for {name} ({current_url})")
                    login_detected = True
                    break
                
                # Check specific elements (optional, URL check is usually enough for redirects)
                # Planner: "计划选项" button
                # CMES: "visual-tableEx"
                
                logger.info(f">>> {name} seems accessible.")
                
            except Exception as e:
                logger.warning(f">>> Error checking {name}: {e}")
                # If timeout or error, might be network or login block. 
                # Conservatively assume we might need to see it if it failed strangely? 
                # Or assume it works? Let's assume valid unless explicit login url.
                pass
                
    except Exception as e:
        logger.error(f"Auth check failed: {e}")
    finally:
        if manager:
            manager.close()
            
    return login_detected


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
    
    # ---------------------------------------------------------
    # Smart Auth Logic
    # ---------------------------------------------------------
    # If user explicitly asked for headless (default), we try to be smart.
    # If user explicitly asked for --no-headless, we respect that and don't check.
    
    final_headless = args.headless
    
    if args.headless and (args.target in ["planner", "cmes", "all"]):
        logger.info(">>> Mode: Headless requested. Initiating Logic: 'Smart Auth Check'")
        
        if check_login_required():
            logger.warning("==================================================================")
            logger.warning("!!! AUTHENTICATION REQUIRED !!!")
            logger.warning("!!! A login prompt was detected in headless mode.")
            logger.warning("!!! Switching to HEADED mode (Visible Browser).")
            logger.warning("!!! Please manually complete the login/MFA in the browser window.")
            logger.warning("==================================================================")
            final_headless = False
        else:
            logger.info(">>> Auth Check Passed: You seem to be logged in. Proceeding in headless mode.")
    
    if args.target == "planner" or args.target == "all":
        run_planner(headless=final_headless)
        
    if args.target == "cmes" or args.target == "all":
        run_cmes(headless=final_headless)
        
    if args.target == "labor" or args.target == "all":
        # Labor always runs in its own way (COM), headless arg ignored usually
        run_labor(headless=final_headless)

if __name__ == "__main__":
    main()
