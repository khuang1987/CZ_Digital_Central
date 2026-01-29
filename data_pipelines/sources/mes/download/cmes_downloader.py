"""
CMES 数据采集器 (Headless)

从 Power BI 报表导出 CMES MES 产出数据。
支持从 CSV 配置文件读取多个 URL，逐个下载并保存到指定位置。
"""

import sys
import os
import shutil
import logging
import time
import csv
from datetime import datetime, timedelta
from calendar import monthrange
from pathlib import Path
from typing import Optional, Callable, List, Dict

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import shared Playwright Manager
from shared_infrastructure.automation.playwright_manager import PlaywrightManager

# Load environment variables
try:
    from dotenv import load_dotenv
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    pass

logger = logging.getLogger(__name__)

# ============================================================
# 配置
# ============================================================

# 默认超时设置（秒）
PAGE_LOAD_TIMEOUT = 60
ELEMENT_TIMEOUT = 30
DOWNLOAD_TIMEOUT = 600  # 10分钟，年度数据量大需要更长时间

# 元素选择器
SELECTORS = {
    # 日期筛选器 - 开始日期
    "date_start_input": "input[aria-label^='开始日期']",
    # 日期筛选器 - 结束日期
    "date_end_input": "input[aria-label^='结束日期']",
    # 数据表容器
    "table_visual": ".visual-tableEx",
    # 更多选项按钮
    "table_menu_button": "button[data-testid='visual-more-options-btn']",
    # 导出数据菜单项
    "export_data_menu": "button[data-testid='pbimenu-item.导出数据']",
    # 导出确认按钮
    "export_confirm_button": "button[data-testid='export-btn']",
}

# ============================================================
# 工具函数
# ============================================================

def get_base_path() -> Path:
    return PROJECT_ROOT

def get_config_path() -> Path:
    path = get_base_path() / "data_pipelines" / "sources" / "mes" / "config" / "cmes_urls.csv"
    if not path.exists():
        # Fallback to old config name if needed or raise error
        pass 
    return path

from shared_infrastructure.env_utils import resolve_path

def get_download_base_dir() -> Path:
    # 30-MES导出数据 (Uses resolve_path to handle dynamic OneDrive root)
    # The string below is legacy-compatible; resolve_path will rewrite 'huangk14' to current user
    raw_path = r"C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\30-MES导出数据"
    path = resolve_path(raw_path)
    
    if not path.exists():
        logger.warning(f"External path not found: {path}, falling back to local")
        path = PROJECT_ROOT / "data" / "raw" / "cmes"
        path.mkdir(parents=True, exist_ok=True)
    return path

def get_cmes_config() -> List[Dict]:
    """从配置文件读取 CMES 报表配置"""
    config_path = get_config_path()
    
    if not config_path.exists():
        raise FileNotFoundError(f"CMES 配置文件不存在: {config_path}")
    
    base_folder = get_download_base_dir()
    
    configs = []
    try:
        with open(config_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Basic validation
                if not row.get('url'): continue

                report_name = row.get('name', 'Unknown').strip()
                filename_format = row.get('filename_format', '').strip()
                skip_date = row.get('skip_date_filter', 'false').lower() == 'true'

                # --- 路径逻辑优化 ---
                # 规则 1: WIP 数据 -> .../CMES_WIP
                if "WIP" in report_name.upper():
                     target_folder = str(base_folder / "CMES_WIP")
                
                # 规则 2: Output 数据 (CZM/CKH) -> .../CMES_Product_Output/{Factory}
                elif report_name.upper() in ["CZM", "CKH"]:
                     target_folder = str(base_folder / "CMES_Product_Output" / report_name)
                
                # 规则 3: 其他默认逻辑 (按原样或放在根目录)
                else:
                    # 尝试从文件名推断
                    if "Product_Output" in filename_format:
                         # 假设 report_name 就是工厂名
                         target_folder = str(base_folder / "CMES_Product_Output" / report_name)
                    else:
                        target_folder = str(base_folder)

                configs.append({
                    'name': report_name,
                    'url': row.get('url', ''),
                    'filename_format': filename_format,
                    'target_folder': target_folder,
                    'skip_date_filter': skip_date
                })
    except Exception as e:
        logger.error(f"Error reading config: {e}")
        return []
    
    return configs

def get_current_month_range() -> tuple[str, str]:
    today = datetime.now()
    first_day = today.replace(day=1)
    last_day = today.replace(day=monthrange(today.year, today.month)[1])
    return first_day.strftime("%Y/%m/%d"), last_day.strftime("%Y/%m/%d")

def get_current_quarter_range() -> tuple[str, str]:
    """Get start and end date of the current quarter"""
    today = datetime.now()
    quarter = (today.month - 1) // 3 + 1
    
    # Start date: 1st of first month of quarter
    start_month = (quarter - 1) * 3 + 1
    start_date = datetime(today.year, start_month, 1)
    
    # End date: last day of last month of quarter
    end_month = start_month + 2
    # monthrange returns (weekday, days_in_month)
    days_in_end_month = monthrange(today.year, end_month)[1]
    end_date = datetime(today.year, end_month, days_in_end_month)
    
    return start_date.strftime("%Y/%m/%d"), end_date.strftime("%Y/%m/%d")

def get_quarter_str() -> str:
    """Get current quarter string, e.g., '2026Q1'"""
    today = datetime.now()
    quarter = (today.month - 1) // 3 + 1
    return f"{today.year}Q{quarter}"

def get_output_filename(filename_format: str, name: str, period: str = None) -> str:
    if period is None:
        period = datetime.now().strftime("%Y%m")
    # Support both {month} and {period} placeholders for flexibility
    return filename_format.format(month=period, period=period, name=name)

def move_and_rename_file(source: Path, target_dir: str, new_name: str) -> Path:
    target_path = Path(target_dir) / new_name
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists():
        target_path.unlink()
    shutil.move(str(source), str(target_path))
    return target_path

# ============================================================
# CMES 数据采集器类
# ============================================================

class CMESDataCollector:
    def __init__(self, config: Dict, headless: bool = False, browser_type: str = "chrome", page=None):
        self.config = config
        self.headless = headless
        self.browser_type = browser_type
        self.download_dir = Path(os.path.expanduser("~/Downloads")) # Temp download location
        self.page = page
        
    def _log(self, message: str):
        logger.info(message)
    
    def collect(self, start_date: str = None, end_date: str = None, output_period: str = None) -> bool:
        skip_date_filter = self.config.get('skip_date_filter', False)
        report_name = self.config.get('name', '')
        
        # Logic to determine report type
        is_quarterly = "Product_Output" in self.config.get('filename_format', '') or report_name in ['CZM', 'CKH']
        is_wip = "WIP" in report_name.upper()

        if not skip_date_filter and (start_date is None or end_date is None):
            if is_quarterly:
                start_date, end_date = get_current_quarter_range()
                if output_period is None:
                    output_period = get_quarter_str()
            else:
                # Default to monthly for non-quarterly (could be refined for WIP specific ranges if needed)
                start_date, end_date = get_current_month_range()
                if output_period is None:
                    # WIP defaults to YYYYMMDD, others to YYYYMM
                    if is_wip:
                        output_period = datetime.now().strftime("%Y%m%d")
                    else:
                        output_period = datetime.now().strftime("%Y%m")
        
        # Allow override even if logic set it above, but if still None (e.g. skip_date=True), set default name
        if output_period is None:
             if is_quarterly:
                 output_period = get_quarter_str()
             elif is_wip:
                 output_period = datetime.now().strftime("%Y%m%d")
             else:
                 output_period = datetime.now().strftime("%Y%m")
        
        self._log(f"开始采集: {self.config['name']}")
        if not skip_date_filter:
            self._log(f"日期范围: {start_date} ~ {end_date}")
        else:
            self._log(f"日期筛选: 跳过 (使用默认或WIP逻辑)")
        
        manager = None
        current_page = None
        
        try:
            if self.page:
                current_page = self.page
            else:
                manager = PlaywrightManager(
                    headless=self.headless,
                    use_user_profile=True,
                    callback=None, # Use internal logger
                    browser_type=self.browser_type
                )
                manager.start()
                current_page = manager.new_page()

            # Ensure we have a page object
            page = current_page
            page.set_default_timeout(ELEMENT_TIMEOUT * 1000)
            
            report_url = self.config['url']
            try:
                page.goto(report_url, timeout=PAGE_LOAD_TIMEOUT * 1000, wait_until="domcontentloaded")
            except Exception as e:
                self._log(f"⚠️ 页面加载超时，尝试继续: {str(e)}")
            
            if not self._check_page_loaded(page):
                self._log("❌ 页面加载失败或需要登录")
                return False
            
            if not skip_date_filter:
                if not self._set_date_filter(page, start_date, end_date):
                    self._log("❌ 设置日期筛选器失败")
                    return False
                time.sleep(2)
            
            downloaded_file = self._export_data(page)
            if not downloaded_file:
                self._log("❌ 导出数据失败")
                return False
            
            new_filename = get_output_filename(
                self.config['filename_format'],
                self.config['name'],
                output_period
            )
            target_dir = self.config['target_folder']
            
            final_path = move_and_rename_file(downloaded_file, target_dir, new_filename)
            self._log(f"✅ 导出成功: {final_path}")
            return True
            
        except Exception as e:
            self._log(f"❌ 采集错误: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            # Only close if we created the manager
            if manager:
                manager.close()

    def _check_page_loaded(self, page) -> bool:
        # Simplified load check logic
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                # Check login
                if self._try_auto_login(page):
                    self._log("✅ 自动登录成功，等待跳转...")
                    time.sleep(5)
                
                load_indicators = [".visual-tableEx", "div[class*='visualContainer']"]
                for indicator in load_indicators:
                    try:
                        page.wait_for_selector(indicator, timeout=10000)
                        return True
                    except:
                        continue
                time.sleep(2)
            except Exception as e:
                self._log(f"页面检测异常: {e}")
        return False

    def _try_auto_login(self, page) -> bool:
        # Re-use logic from Planner downloader (could be shared, but copying for isolation)
        try:
            if "login.microsoftonline.com" not in page.url.lower(): return False
            
            pick_account_selector = "div[aria-label*='@medtronic.com'], div[role='button']:has-text('@medtronic.com')"
            try:
                if page.wait_for_selector(pick_account_selector, timeout=2000):
                    page.click(pick_account_selector)
                    return True
            except: pass
            
            # Click standard login buttons if present
            login_btn = page.locator("input[type='submit'][value='Next'], input[type='submit'][value='Sign in'], input#idSIButton9").first
            if login_btn.is_visible(timeout=1000):
                login_btn.click()
                return True
            return False
        except: return False

    def _set_date_filter(self, page, start_date: str, end_date: str) -> bool:
        try:
            # JS injection is most reliable for PowerBI inputs
            def _apply(val, selector):
                page.locator(selector).fill(val)
                page.locator(selector).evaluate("el => { el.dispatchEvent(new Event('input', {bubbles: true})); el.dispatchEvent(new Event('change', {bubbles: true})); el.blur(); }")
                time.sleep(0.5)

            _apply(start_date, SELECTORS["date_start_input"])
            _apply(end_date, SELECTORS["date_end_input"])
            return True
        except Exception as e:
            self._log(f"日期设置失败: {e}")
            return False

    def _export_data(self, page) -> Optional[Path]:
        try:
            time.sleep(1)
            # 1. Hover table
            page.locator(SELECTORS["table_visual"]).first.hover()
            time.sleep(0.5)
            
            # 2. Click 'More options' (...)
            # PowerBI DOM is tricky, rely on visibility or JS click
            # Try finding the button near the visual
            try:
                page.locator(SELECTORS["table_menu_button"]).click(timeout=3000)
            except:
                # Fallback: force show header via JS then click
                page.evaluate("document.querySelectorAll('.visual-header').forEach(el => el.style.display='block')")
                page.locator(SELECTORS["table_menu_button"]).click()
            
            # 3. Click 'Export data' menuItem
            page.locator(SELECTORS["export_data_menu"]).click()
            
            # 4. Wait for dialog and click Export confirm
            page.wait_for_selector(SELECTORS["export_confirm_button"], timeout=5000)
            
            with page.expect_download(timeout=60000) as download_info:
                page.click(SELECTORS["export_confirm_button"])
                
            download = download_info.value
            save_path = self.download_dir / download.suggested_filename
            download.save_as(str(save_path))
            return save_path
            
        except Exception as e:
            self._log(f"导出操作失败: {e}")
            # debugging screenshot
            # page.screenshot(path="export_error.png")
            return None

def collect_cmes_data(headless=True):
    configs = get_cmes_config()
    if not configs:
        logger.warning("没有加载到 CMES 配置")
        return
    
    # Initialize global browser session to reuse across tasks
    manager = None
    try:
        manager = PlaywrightManager(
            headless=headless,
            use_user_profile=True,
            callback=None,
            browser_type="chrome"
        )
        manager.start()

        # 尝试自动登录
        if os.getenv("MDDAP_MS_USER") and os.getenv("MDDAP_MS_PASSWORD"):
            manager.login_microsoft(os.getenv("MDDAP_MS_USER"), os.getenv("MDDAP_MS_PASSWORD"))

        main_page = manager.new_page()
        
        # Loop with the same page
        for cfg in configs:
            # Pass page to collector
            collector = CMESDataCollector(cfg, headless=headless, page=main_page)
            collector.collect()
            
    except Exception as e:
        logger.error(f"Global collection error: {e}")
    finally:
        if manager:
            logger.info("[INFO] Closing shared browser session")
            manager.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
    collect_cmes_data(headless=False)
