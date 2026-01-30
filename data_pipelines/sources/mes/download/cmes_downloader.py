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

# Ensure environment is loaded from shared utils
try:
    from shared_infrastructure.env_utils import PROJECT_ROOT as _ROOT, ENV_FILE
except ImportError:
    pass

logger = logging.getLogger(__name__)

# ============================================================
# 配置
# ============================================================

# 默认超时设置（秒）
PAGE_LOAD_TIMEOUT = 60
ELEMENT_TIMEOUT = 30
DOWNLOAD_TIMEOUT = 900  # 15分钟，Resource 数据量特别大需要更长时间

# 元素选择器
SELECTORS = {
    # 日期筛选器 - 开始日期 (支持中文和英文)
    "date_start_input": "input[aria-label^='开始日期'], input[aria-label^='Start date']",
    # 日期筛选器 - 结束日期
    "date_end_input": "input[aria-label^='结束日期'], input[aria-label^='End date']",
    # 数据表容器
    "table_visual": ".visual-tableEx",
    # 更多选项按钮
    "table_menu_button": "button[data-testid='visual-more-options-btn']",
    # 导出数据菜单项
    "export_data_menu": "button[data-testid='pbimenu-item.导出数据'], button[data-testid*='Export']",
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
                     
                # 规则 3: Scrap Detail (如 CKH_Scrap) -> .../CMES_Scrap_Detail/{Factory}
                elif "SCRAP" in report_name.upper():
                    # 提取工厂名 (e.g. CKH_Scrap -> CKH)
                    factory = report_name.split('_')[0]
                    target_folder = str(base_folder / "CMES_Scrap_Detail" / factory)
                    
                # 规则 4: Resource Detail (如 CKH_Resource) -> .../CMES_Resource_Detail/{Factory}
                elif "RESOURCE" in report_name.upper():
                    factory = report_name.split('_')[0]
                    target_folder = str(base_folder / "CMES_Resource_Detail" / factory)
                
                # 规则 5: 其他默认逻辑 (按原样或放在根目录)
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
        is_quarterly = (
            "Product_Output" in self.config.get('filename_format', '') 
            or report_name in ['CZM', 'CKH'] 
            or "SCRAP" in report_name.upper() 
            or "RESOURCE" in report_name.upper()
        )
        
        # 历史数据补全逻辑 (仅针对 Quarterly 报表且未指定特定日期时)
        # WIP 报表永远只下载最新，不需要补全历史
        is_wip = "WIP" in report_name.upper()
        if is_quarterly and not skip_date_filter and start_date is None and not is_wip:
            # Generate a sample filename to log what we are looking for
            sample_period = "2024Q1"
            if "RESOURCE" in report_name.upper(): sample_period = "2024M01"
            sample_filename = get_output_filename(self.config['filename_format'], self.config['name'], period=sample_period)
            
            self._log(f"[{report_name}] 启动历史数据检查 (2023年至今)...")
            self._log(f"[{report_name}] 启动历史数据检查 (2023年至今)...")
            # self._log(f"[{report_name}] 目标文件示例: {sample_filename}")
            
            browser_initialized = False
            manager = None
            
            try:
                # 判断是否为 Resource 报表（按月下载）
                is_resource = "RESOURCE" in report_name.upper()
                
                if is_resource:
                    # Resource 报表：生成从 2023年1月 到当前月的所有月份
                    periods_to_check = []
                    today = datetime.now()
                    start_year, start_month = 2023, 1
                    
                    current_year, current_month = today.year, today.month
                    
                    # 从2023年1月开始，逐月生成
                    year, month = start_year, start_month
                    while (year < current_year) or (year == current_year and month <= current_month):
                        period_str = f"{year}M{month:02d}"
                        periods_to_check.append((year, month, period_str, 'month'))
                        
                        # 递增月份
                        month += 1
                        if month > 12:
                            month = 1
                            year += 1
                else:
                    # 其他报表（Output, Scrap）：生成从 2023Q1 到当前季度
                    periods_to_check = []
                    today = datetime.now()
                    current_quarter = (today.month - 1) // 3 + 1
                    current_year = today.year
                    
                    start_year, start_quarter = 2023, 1
                    
                    # 从2023Q1开始，逐季度生成
                    year, quarter = start_year, start_quarter
                    while (year < current_year) or (year == current_year and quarter <= current_quarter):
                        period_str = f"{year}Q{quarter}"
                        periods_to_check.append((year, quarter, period_str, 'quarter'))
                        
                        # 递增季度
                        quarter += 1
                        if quarter > 4:
                            quarter = 1
                            year += 1
                
                # 优先下载最近的数据：反转列表
                periods_to_check.reverse()

                # 打印一下要检查的周期总数，辅助调试
                # self._log(f"[{report_name}] 需检查 {len(periods_to_check)} 个周期: {[x[2] for x in periods_to_check]}")
                
                # 逐个检查本地文件是否存在
                today = datetime.now()
                if is_resource:
                     current_period_str = f"{today.year}M{today.month:02d}"
                else:
                     curr_q = (today.month - 1) // 3 + 1
                     current_period_str = f"{today.year}Q{curr_q}"

                missing_periods = []
                for *period_info, period_str, period_type in periods_to_check:
                    # 预测文件名
                    filename = get_output_filename(
                        self.config['filename_format'],
                        self.config['name'],
                        period=period_str
                    )
                    target_path = Path(self.config['target_folder']) / filename
                    
                    # 强制更新当前周期 (即便文件已存在，也要覆盖以获取最新数据)
                    is_current = (period_str == current_period_str)
                    
                    if not target_path.exists() or is_current:
                        if is_current:
                             pass # self._log(f"[{report_name}] 🔄 检测到当前周期 ({period_str})，强制加入更新队列。")
                        missing_periods.append((*period_info, period_str, period_type))
                    # else:
                    #    self._log(f"[{report_name}] 文件已存在: {filename}")
                
                if not missing_periods:
                    period_type_name = "月度" if is_resource else "季度"
                    self._log(f"✅ [{report_name}] 所有历史{period_type_name}数据已完整 ({len(periods_to_check)} 个周期)，无需下载。")
                    return True
                    
                period_type_name = "月" if is_resource else "个季度"
                self._log(f"⚠️ [{report_name}] 发现 {len(missing_periods)} {period_type_name}数据缺失，准备补全: {[x[2] for x in missing_periods]}")
                
                # 初始化浏览器 (如果还没初始化)
                if self.page:
                     page = self.page
                else:
                    manager = PlaywrightManager(headless=self.headless, use_user_profile=True, browser_type=self.browser_type)
                    manager.start()
                    # 尝试自动登录
                    if os.getenv("MDDAP_MS_USER"):
                         manager.login_microsoft(os.getenv("MDDAP_MS_USER"), os.getenv("MDDAP_MS_PASSWORD"))
                    page = manager.new_page()
                    browser_initialized = True

                # 导航到页面 (只需一次)
                self._ensure_page_ready(page)
                
                # 循环下载缺失周期
                for *period_info, period_str, period_type in missing_periods:
                    self._log(f"\n{'='*20} 开始下载: {report_name} - {period_str} {'='*20}")
                    # self._log(f">>> 开始补全: {period_str}")
                    
                    if period_type == 'month':
                        # 月度：直接使用该月的起止日期
                        year, month = period_info
                        s_date = datetime(year, month, 1).strftime("%Y/%m/%d")
                        last_day = monthrange(year, month)[1]
                        e_date = datetime(year, month, last_day).strftime("%Y/%m/%d")
                    else:
                        # 季度：计算季度的起止日期
                        year, quarter = period_info
                        q_start_month = (quarter - 1) * 3 + 1
                        q_end_month = q_start_month + 2
                        
                        s_date = datetime(year, q_start_month, 1).strftime("%Y/%m/%d")
                        last_day = monthrange(year, q_end_month)[1]
                        e_date = datetime(year, q_end_month, last_day).strftime("%Y/%m/%d")
                    
                    # 执行下载逻辑
                    if self._perform_single_download(page, s_date, e_date, period_str):
                        self._log(f"✅ {period_str} 补全成功")
                    else:
                        self._log(f"❌ {period_str} 补全失败")
                
                return True
                
            except Exception as e:
                self._log(f"历史补全过程出错: {e}")
                return False
            finally:
                if browser_initialized and manager:
                    manager.close()
            
            return True # 这里的 Return 代表历史检查逻辑结束
            
        # ==========================================
        # 原有的单次下载逻辑 (WIP 或 指定日期)
        # ==========================================
        
        # 智能日期计算：优先从 output_period 解析，否则使用当前月
        if start_date is None or end_date is None:
            if output_period and ('M' in output_period or 'Q' in output_period):
                # 从 output_period 解析日期范围
                try:
                    if 'M' in output_period:
                        # 月度格式: 2024M01
                        year, month = output_period.split('M')
                        year, month = int(year), int(month)
                        start_date = datetime(year, month, 1).strftime("%Y/%m/%d")
                        last_day = monthrange(year, month)[1]
                        end_date = datetime(year, month, last_day).strftime("%Y/%m/%d")
                    elif 'Q' in output_period:
                        # 季度格式: 2024Q1
                        year, quarter = output_period.split('Q')
                        year, quarter = int(year), int(quarter)
                        q_start_month = (quarter - 1) * 3 + 1
                        q_end_month = q_start_month + 2
                        start_date = datetime(year, q_start_month, 1).strftime("%Y/%m/%d")
                        last_day = monthrange(year, q_end_month)[1]
                        end_date = datetime(year, q_end_month, last_day).strftime("%Y/%m/%d")
                except:
                    # 解析失败，回退到当前月
                    start_date, end_date = get_current_month_range()
                    if output_period is None:
                        output_period = datetime.now().strftime("%Y%m%d")
            else:
                # 没有周期格式，使用当前月
                start_date, end_date = get_current_month_range()
                if output_period is None:
                    output_period = datetime.now().strftime("%Y%m%d")  # WIP format default
        
        self._log(f"开始单次采集: {self.config['name']} ({output_period})")
        
        # ... (Reuse _perform_single_download logic or generic manager setup)
        # Refactoring to avoid code duplication is ideal, but for now lets wrap the core download
        
        manager = None
        try:
            if self.page:
                page = self.page
            else:
                manager = PlaywrightManager(headless=self.headless, use_user_profile=True, browser_type=self.browser_type)
                manager.start()
                page = manager.new_page()
            
            self._ensure_page_ready(page)
            if self._perform_single_download(page, start_date, end_date, output_period):
                 return True
            return False
            
        except Exception as e:
            self._log(f"采集错误: {e}")
            return False
        finally:
            if manager:
                manager.close()

    def _ensure_page_ready(self, page):
        """Helper to navigate and wait for load"""
        if page.url != self.config['url']:
            try:
                page.goto(self.config['url'], timeout=PAGE_LOAD_TIMEOUT * 1000, wait_until="domcontentloaded")
            except: pass
        
        if not self._check_page_loaded(page):
            raise Exception("页面加载失败")

    def _perform_single_download(self, page, start_date, end_date, output_period) -> bool:
        """Core download execution for a given date range"""
        skip_date_filter = self.config.get('skip_date_filter', False)
        
        self._log(f"  日期范围: {start_date} ~ {end_date}")
        
        if not skip_date_filter:
            if not self._set_date_filter(page, start_date, end_date):
                 self._log("❌ 设置日期筛选器失败")
                 return False
            time.sleep(2)
        
        downloaded_file = self._export_data(page)
        if not downloaded_file:
            return False
        
        new_filename = get_output_filename(
            self.config['filename_format'],
            self.config['name'],
            output_period
        )
        target_dir = self.config['target_folder']
        final_path = move_and_rename_file(downloaded_file, target_dir, new_filename)
        self._log(f"✅ 文件保存: {final_path}")
        return True

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
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Reverting to explicit JS event dispatching - The most efficient method
                # Method Name: JS Injection with Event Dispatching (直接DOM操作+事件触发)
                def _apply(val, selector):
                    # 1. Fill standard way (updates 'value' attribute)
                    try: page.locator(selector).fill(val)
                    except: pass
                    
                    # 2. Force events using JS - critical for React/Angular to detect change
                    page.locator(selector).evaluate("el => { el.dispatchEvent(new Event('input', {bubbles: true})); el.dispatchEvent(new Event('change', {bubbles: true})); el.blur(); }")
                    time.sleep(0.5)

                # Use "Sandwich" Strategy (Start -> End -> Start) to handle both Forward and Backward time shifts robustly.
                # Forward: Start(fail) -> End(ok) -> Start(ok)
                # Backward: Start(ok) -> End(ok) -> Start(ok)
                
                if attempt == 0:
                     self._log(f"  正在设置日期: {start_date} ~ {end_date}")
                else:
                     self._log(f"  正在设置日期 (重试 {attempt}): {start_date} ~ {end_date}")

                _apply(start_date, SELECTORS["date_start_input"])
                time.sleep(0.3)
                _apply(end_date, SELECTORS["date_end_input"])
                time.sleep(0.3)
                # Redundant set to ensure Start didn't fail if we expanded range from End
                _apply(start_date, SELECTORS["date_start_input"])
                
                # Wait for UI to react
                time.sleep(1.0)
                
                # Validation Step: Check if the value was actually set
                try:
                    act_start = page.locator(SELECTORS["date_start_input"]).input_value()
                    act_end = page.locator(SELECTORS["date_end_input"]).input_value()
                    
                    # Robust Date Comparison (Intelligent Parsing)
                    def parse_date_loose(s):
                        # Convert "2025/09/01" or "2025-9-1" to tuple (2025, 9, 1)
                        parts = s.replace('-', '/').replace('.', '/').split('/')
                        if len(parts) == 3:
                            return (int(parts[0]), int(parts[1]), int(parts[2]))
                        return None
                    
                    target_s = parse_date_loose(start_date)
                    target_e = parse_date_loose(end_date)
                    actual_s = parse_date_loose(act_start)
                    actual_e = parse_date_loose(act_end)
                    
                    if target_s and actual_s and target_e and actual_e:
                        if target_s == actual_s and target_e == actual_e:
                            return True
                    
                    self._log(f"⚠️ 日期校验差异: 期望 {start_date} / {end_date}, 实际 {act_start} / {act_end}")
                    
                except Exception as e:
                    # self._log(f"⚠️ 日期校验过程出错: {e}")
                    pass
            
            except Exception as e:
                self._log(f"日期设置异常: {e}")
            
            time.sleep(1) # Interval before retry
            
        self._log("❌ 日期设置最终失败")
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
            
            # Dynamic timeout: Resource reports need more time due to large data volume
            is_resource = "RESOURCE" in self.config.get('name', '').upper()
            download_timeout_ms = 900000 if is_resource else 600000  # 15min for Resource, 10min for others
            
            with page.expect_download(timeout=download_timeout_ms) as download_info:
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

        main_page = manager.new_page()

        # 优化流程：先访问第一个目标 URL，触发跳转登录页，然后再执行登录
        # 这样避免了在 about:blank 页面尝试登录的问题
        if configs and configs[0].get('url'):
            first_url = configs[0]['url']
            logger.info(f"正在导航至首个目标页面以触发登录: {first_url}")
            try:
                main_page.goto(first_url, timeout=60000, wait_until="domcontentloaded")
            except: 
                pass # 忽略可能的超时，继续处理登录

        # 尝试自动登录
        if os.getenv("MDDAP_MS_USER") and os.getenv("MDDAP_MS_PASSWORD"):
            manager.login_microsoft(os.getenv("MDDAP_MS_USER"), os.getenv("MDDAP_MS_PASSWORD"))
        
        
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
