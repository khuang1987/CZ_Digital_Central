"""
Planner 任务数据下载器 (Headless)

功能：自动登录 Microsoft Planner，下载所有配置的 Plan Excel 文件。
"""

import sys
import time
import os
import glob
import shutil
import logging
import yaml
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional, Callable, Dict, List

# Add project root to sys.path to allow imports
PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import shared Playwright Manager
from shared_infrastructure.automation.playwright_manager import PlaywrightManager

# Configure Logging
logger = logging.getLogger(__name__)

# Load environment variables
try:
    from dotenv import load_dotenv
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        logger.warning(f".env file not found at {env_path}")
except ImportError:
    logger.warning("python-dotenv not installed, environment variables might not be loaded.")

# Debug Env Loading
try:
    user_val = os.getenv("MDDAP_MS_USER")
    pass_val = os.getenv("MDDAP_MS_PASSWORD")
    logger.info(f"DEBUG: .env path used: {PROJECT_ROOT / '.env'}")
    logger.info(f"DEBUG: .env exists? {(PROJECT_ROOT / '.env').exists()}")
    logger.info(f"DEBUG: MDDAP_MS_USER loaded? {'Yes: ' + user_val if user_val else 'No'}")
    logger.info(f"DEBUG: MDDAP_MS_PASSWORD loaded? {'Yes (Masked)' if pass_val else 'No'}")
except Exception as e:
    logger.error(f"DEBUG Error: {e}")

# Constants
DEFAULT_DOWNLOAD_TIMEOUT = 120

def get_base_path() -> Path:
    return PROJECT_ROOT

def get_etl_config_path() -> Path:
    return PROJECT_ROOT / "data_pipelines" / "sources" / "planner" / "config" / "config_planner_tasks.yaml"

from shared_infrastructure.env_utils import load_yaml_with_env

def get_download_path() -> Path:
    """Returns the path where raw Planner files should be saved."""
    # Try reading from ETL config first
    try:
        config_path = get_etl_config_path()
        if config_path.exists():
            config = load_yaml_with_env(config_path)
            if config and 'source' in config and 'planner_path' in config['source']:
                custom_path = Path(config['source']['planner_path'])
                # Ensure it exists or try to create it
                custom_path.mkdir(parents=True, exist_ok=True)
                logger.info(f"Using download path from config: {custom_path}")
                return custom_path
    except Exception as e:
        logger.warning(f"Failed to read ETL config: {e}")
    except Exception as e:
        logger.warning(f"Failed to read ETL config: {e}")

    # Fallback to default
    path = PROJECT_ROOT / "data" / "raw" / "planner"
    path.mkdir(parents=True, exist_ok=True)
    return path



def _wait_for_planner_load(page, manager, log_callback) -> bool:
    """循环检查页面加载，处理重定向和自动登录"""
    max_attempts = 10
    dropdown_selector = '//button[contains(@aria-label, "计划选项") and contains(@class, "linkedBadgeDropdown")]'
    
    # 获取凭证
    ms_user = os.getenv("MDDAP_MS_USER")
    ms_pass = os.getenv("MDDAP_MS_PASSWORD")
    
    for attempt in range(max_attempts):
        try:
            # 1. 检查当前 URL 状态 (检测登录页)
            current_url = page.url.lower()
            is_login_page = any(domain in current_url for domain in [
                'login.microsoftonline.com', 'login.live.com', 'login.windows.net', 'microsoftonline.com'
            ])

            if is_login_page:
                if ms_user and ms_pass:
                    log_callback(f"探测到登录页面，尝试通过 PlaywrightManager 自动登录...")
                    # 调用统一的登录逻辑
                    if manager.login_microsoft(ms_user, ms_pass):
                        log_callback("✅ 自动登录操作执行完毕，等待跳转...")
                        time.sleep(5)
                        continue
                else:
                    log_callback("探测到登录页面，但未配置 .env 凭证 (MDDAP_MS_USER/PASSWORD)，等待手动登录...")
                    time.sleep(5)
            
            # 2. 检查是否已经加载到 Planner 主页面
            try:
                # 缩短等待时间以便快速响应
                element = page.wait_for_selector(f"xpath={dropdown_selector}", state="visible", timeout=5000)
                if element:
                    log_callback("✅ Planner 页面加载完成")
                    return True
            except:
                pass
            
            log_callback(f"等待 Planner 内容加载 ({attempt + 1}/{max_attempts})...")
            time.sleep(3)
                
        except Exception as e:
            log_callback(f"⚠️ 加载检测异常: {str(e)}")
            time.sleep(2)
            
    return False

def move_downloaded_file(downloads_path, index, total_urls, callback):
    recent_files = sorted(glob.glob(os.path.join(downloads_path, "*.xlsx")), key=os.path.getmtime)
    if not recent_files:
        raise Exception("未找到下载的文件")
        
    source_path = recent_files[-1]
    file_name = os.path.basename(source_path)
    
    # Save to data/raw/planner
    save_path = get_download_path()
    destination_path = os.path.join(save_path, file_name)
    
    # Remove existing if any
    if os.path.exists(destination_path):
        os.remove(destination_path)
    
    shutil.move(source_path, destination_path)
    
    if callback:
        message = f"文件已保存[{index+1}/{total_urls}]: {file_name} -> {save_path}"
        callback(message)
    
    return file_name

def export_planner_data(headless=True, browser_type="chrome") -> bool:
    """导出Planner数据（使用 Playwright）"""
    
    # User requested to remove retry loop and trigger only one round.
    # Logic simplified: run once. If it fails, it fails (logs will show).
    
    # Simple logger callback
    def log_callback(message):
        logger.info(message)
    
    log_callback("开始导出Planner数据...")
    
    manager = None
    
    try:
        # 创建 Playwright 管理器
        manager = PlaywrightManager(
            headless=headless,
            use_user_profile=True,
            callback=log_callback,
            browser_type=browser_type
        )
        manager.start()
        page = manager.new_page()
            
        # 读取配置文件
        config_file = PROJECT_ROOT / "data_pipelines" / "sources" / "planner" / "config" / "planner_urls.csv"
        
        if not config_file.exists():
             raise FileNotFoundError(f"配置文件未找到: {config_file}")

        try:
            df = pd.read_csv(config_file, encoding='utf-8-sig')
        except Exception as e:
             # Try without BOM
             df = pd.read_csv(config_file, encoding='utf-8')

        if df.empty:
            raise ValueError("配置文件为空")
        
        # Check columns
        if 'URL' not in df.columns:
             # Try first column
             df['URL'] = df.iloc[:, 0]

        urls = df['URL'].dropna().tolist()
        areas = df['区域'].dropna().tolist() if '区域' in df.columns else [f"Area {i}" for i in range(len(urls))]
        
        if not urls:
            raise ValueError("未找到有效的URL配置")
            
        log_callback(f"开始处理 {len(urls)} 个Planner URL...")
        
        success_count = 0
        
        # 处理所有URL
        for index, url in enumerate(urls):
            try:
                area = areas[index] if index < len(areas) else f"区域{index+1}"
                log_callback(f"开始处理 {area} [{index+1}/{len(urls)}]...")
                
                # 访问URL
                try:
                    page.goto(url, timeout=60000, wait_until="domcontentloaded")
                except Exception as e:
                    log_callback(f"⚠️ 页面初次加载超时: {str(e)}")
                
                # 等待加载
                if not _wait_for_planner_load(page, manager, log_callback):
                    log_callback(f"❌ {area} 页面加载超时，跳过此区域")
                    continue
                
                # 定位计划选项按钮
                dropdown_selector = '//button[contains(@aria-label, "计划选项") and contains(@class, "linkedBadgeDropdown")]'
                
                try:
                    page.wait_for_selector(f"xpath={dropdown_selector}", state="visible", timeout=30000)
                    page.locator(f"xpath={dropdown_selector}").wait_for(state="visible", timeout=5000)
                except Exception as e:
                    log_callback(f"⚠️ 等待计划选项按钮超时: {str(e)}")
                    # Retry logic could be added here
                    continue # Skip if cant find button
                
                # 点击下接
                page.click(f"xpath={dropdown_selector}")
                
                # 等待导出按钮
                export_selector = "//button[@aria-label='将计划导出到 Excel']"
                page.wait_for_selector(f"xpath={export_selector}", state="visible", timeout=10000)
                
                # 等待下载
                downloads_path = os.path.expanduser("~/Downloads")
                
                with page.expect_download(timeout=60000) as download_info:
                    page.click(f"xpath={export_selector}")
                
                download = download_info.value
                download_path = Path(downloads_path) / download.suggested_filename
                download.save_as(str(download_path))
                
                # 移动文件
                file_name = move_downloaded_file(downloads_path, index, len(urls), log_callback)
                log_callback(f"✅ {area} 导出成功")
                success_count += 1
                print()
                
            except Exception as e:
                error_message = f"处理{area}时出错: {str(e)}"
                log_callback(error_message)
                continue
        
        log_callback(f"🎉 Planner导出完成: {success_count}/{len(urls)} 个区域")
        return True
    
    except Exception as e:
        error_msg = f"处理过程中出错: {str(e)}"
        log_callback(error_msg)
        return False
    
    finally:
        if manager:
            try:
                manager.close()
            except Exception as e:
                pass
    
    return False

if __name__ == "__main__":
    # Setup basic logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    export_planner_data(headless=False) # Default to visible browser for manual testing if run directly
