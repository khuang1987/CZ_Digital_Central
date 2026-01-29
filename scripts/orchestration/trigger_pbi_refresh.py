"""
PowerBI Refresh Trigger
Function: Automate 'Refresh' button click on PowerBI Report pages.
"""

import sys
import os
import csv
import time
import logging
from pathlib import Path
from typing import List, Dict

# Add project root needed for shared modules
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Load Env
try:
    from dotenv import load_dotenv
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    pass

from shared_infrastructure.automation.playwright_manager import PlaywrightManager

logger = logging.getLogger(__name__)

def get_config_path() -> Path:
    return PROJECT_ROOT / "scripts" / "orchestration" / "config" / "powerbi_urls.csv"

def get_reports() -> List[Dict]:
    path = get_config_path()
    if not path.exists():
        logger.error(f"Config not found: {path}")
        return []
        
    reports = []
    try:
        with open(path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            reports = list(reader)
    except Exception as e:
        logger.error(f"Error reading config: {e}")
        
    return reports

def refresh_single_report(page, report_name, report_url):
    logger.info(f"Processing: {report_name}")
    try:
        # Goto with login auto-check implicit in flow (manager handles cookies)
        page.goto(report_url, timeout=60000, wait_until="domcontentloaded")
        
        # 1. Click 'Refresh' (Header menu)
        # PowerBI UI changes often, using specific selectors from old script
        refresh_btn = 'button.mat-mdc-menu-trigger[title="刷新"]'
        
        try:
             page.wait_for_selector(refresh_btn, state="visible", timeout=30000)
             page.click(refresh_btn)
        except:
             # Fallback 1: Try finding by text
             page.get_by_text("刷新", exact=True).first.click()
             
        # 2. Click 'Refresh now' (Dropdown item)
        refresh_now_btn = 'button.mat-mdc-menu-item[title="立即刷新"]'
        
        try:
            page.wait_for_selector(refresh_now_btn, state="visible", timeout=5000)
            page.click(refresh_now_btn)
            logger.info(f"✅ Triggered refresh for {report_name}")
            return True
        except:
            logger.warning(f"Could not find 'Refresh now' button for {report_name}")
            return False
            
    except Exception as e:
        logger.error(f"Error refreshing {report_name}: {e}")
        return False

def trigger_refresh_all(headless=False):
    reports = get_reports()
    if not reports:
        logger.warning("No reports to refresh.")
        return

    logger.info(f"Starting Refresh for {len(reports)} reports...")

    manager = PlaywrightManager(
        headless=headless,
        use_user_profile=True,
        browser_type="chrome"
    )
    
    try:
        manager.start()
        
        # Auto Login Attempt
        ms_user = os.getenv("MDDAP_MS_USER")
        ms_pass = os.getenv("MDDAP_MS_PASSWORD")
        if ms_user and ms_pass:
            manager.login_microsoft(ms_user, ms_pass)
            
        page = manager.new_page()
        
        for report in reports:
            refresh_single_report(page, report['name'], report['url'])
            time.sleep(2) # Brief pause
            
    except Exception as e:
        logger.error(f"Global refresh error: {e}")
    finally:
        manager.close()
        logger.info("Refresh session finished.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    trigger_refresh_all(headless=False)
