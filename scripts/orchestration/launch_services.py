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

def launch_dashboard(port=8501):
    project_root = Path(__file__).resolve().parents[2]
    dashboard_dir = project_root / "dashboard"
    
    logger.info(f"Checking Dashboard Service Status on port {port}...")
    
    if is_port_in_use(port):
        logger.info(f"✅ Dashboard Service is already running on port {port}. Skipping launch.")
        return

    logger.info(f"Launching Dashboard from: {dashboard_dir}")
    
    # Command for new window launch
    # Determine streamlit executable path based on current python
    # This ensures we use the same venv that the orchestrator is running in
    if os.name == 'nt':
        # Windows: <venv>/Scripts/streamlit.exe
        streamlit_exe = os.path.join(sys.prefix, 'Scripts', 'streamlit.exe')
    else:
        # Linux/Mac: <venv>/bin/streamlit
        streamlit_exe = os.path.join(sys.prefix, 'bin', 'streamlit')
        
    # Fallback if not found (e.g. global install)
    if not os.path.exists(streamlit_exe):
        streamlit_exe = "streamlit"
    
    cmd = f'start "MDDAP Dashboard" cmd /k "{streamlit_exe} run app.py --server.port {port}"'
    
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
