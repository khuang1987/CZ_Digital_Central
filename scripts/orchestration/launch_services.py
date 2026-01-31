import os
import sys
import subprocess
import socket
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def is_port_in_use(port: int) -> bool:
    """Check if a port is in use on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

def launch_dashboard(port=3000):
    project_root = Path(__file__).resolve().parents[2]
    dashboard_dir = project_root / "apps" / "web_dashboard"
    
    logger.info(f"Checking Next.js Dashboard Status on port {port}...")
    
    if is_port_in_use(port):
        logger.info(f"✅ Dashboard Service is already running on port {port}. Skipping launch.")
        return

    logger.info(f"Launching Next.js Dashboard from: {dashboard_dir}")
    
    # Use npm run dev for Next.js
    if os.name == 'nt':
        # Windows: start cmd to run npm
        cmd = f'start "MDDAP Dashboard" cmd /k "npm run dev"'
    else:
        # Linux/Mac
        cmd = "npm run dev &"
    
    # Use shell=True and OS-specific flags for true detachment
    # On Windows, 'start' via shell=True within Popen is usually enough, 
    # but we want to ensure the parent script (orchestrator) finishes immediately.
    
    # Use 'start' to launch new window, and redirects to break pipe inheritance
    # Do not use DETACHED_PROCESS or CREATE_NEW_CONSOLE with shell=True on Windows 
    # as it can conflict with 'start' or hide the intermediate shell.
    try:
        subprocess.Popen(
            cmd, 
            shell=True, 
            cwd=dashboard_dir,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        logger.info("✅ Dashboard launch command executed.")
    except Exception as e:
        logger.error(f"Failed to launch dashboard: {e}")

if __name__ == "__main__":
    launch_dashboard()
