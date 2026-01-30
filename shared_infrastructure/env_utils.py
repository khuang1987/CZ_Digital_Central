import os
import re
import yaml
from pathlib import Path
from typing import Any, Dict

# Try to load dotenv
# Load environment variables (Robust Method)
def _manual_load_dotenv(env_path: Path):
    """Fallback manual parser for .env files if python-dotenv is missing"""
    if not env_path.exists():
        return
    try:
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                k, v = line.split('=', 1)
                os.environ[k.strip()] = v.strip().strip('"').strip("'")
    except Exception:
        pass

import getpass
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Prioritize .env_<USERNAME> (e.g. .env_czxmfg) or fall back to .env
username = getpass.getuser()
ENV_FILE_USER = PROJECT_ROOT / f".env_{username}"
ENV_FILE_DEFAULT = PROJECT_ROOT / ".env"

ENV_FILE = ENV_FILE_USER if ENV_FILE_USER.exists() else ENV_FILE_DEFAULT

try:
    from dotenv import load_dotenv
    if ENV_FILE.exists():
        load_dotenv(ENV_FILE)
    
    # Also load default .env if we used a user-specific one (to provide base defaults)
    if ENV_FILE == ENV_FILE_USER and ENV_FILE_DEFAULT.exists():
        load_dotenv(ENV_FILE_DEFAULT, override=False) # Only fill in missing gaps
    
    if not ENV_FILE.exists() and not ENV_FILE_DEFAULT.exists():
        # Last resort fallback if no files found
        pass
except ImportError:
    if ENV_FILE.exists():
        _manual_load_dotenv(ENV_FILE)
    if ENV_FILE == ENV_FILE_USER and ENV_FILE_DEFAULT.exists():
        _manual_load_dotenv(ENV_FILE_DEFAULT) # Manual fallback doesn't easy support override=False, but it's okay for now

def get_onedrive_root() -> Path:
    """
    Get the OneDrive root directory from environment variable or default.
    """
    env_root = os.getenv("MDDAP_ONEDRIVE_ROOT")
    if env_root:
        path = Path(env_root)
        if path.exists():
            return path
    
    # Fallback: Try to detect based on current user home
    # This works for both 'huangk14' and 'czxmfg' without .env
    user_home = Path(os.path.expanduser("~"))
    possible_root = user_home / "OneDrive - Medtronic PLC"
    
    if possible_root.exists():
        return possible_root
        
    # Final fallback if folder structure is totally different
    return Path(r"C:\Users\huangk14\OneDrive - Medtronic PLC")

def resolve_path(path_str: str) -> Path:
    """
    Resolve a path string that might containing placeholders like ${MDDAP_ONEDRIVE_ROOT}.
    """
    if not path_str:
        return Path(".")
        
    on_drive_root = str(get_onedrive_root()).replace("\\", "/")
    
    # Simple replace
    resolved = path_str.replace("${MDDAP_ONEDRIVE_ROOT}", on_drive_root)
    
    # Also handle the hardcoded legacy string widely used in config
    legacy_root = "C:/Users/huangk14/OneDrive - Medtronic PLC"
    legacy_root_win = r"C:\Users\huangk14\OneDrive - Medtronic PLC"
    
    if path_str.startswith(legacy_root):
        resolved = path_str.replace(legacy_root, on_drive_root)
    elif path_str.startswith(legacy_root_win):
        resolved = path_str.replace(legacy_root_win, on_drive_root)
        
    return Path(resolved)

def load_yaml_with_env(file_path: Path) -> Dict[str, Any]:
    """
    Load a YAML file and substitute environment variables in values.
    Also handles the specific ${MDDAP_ONEDRIVE_ROOT} placeholder.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
    
    # regex sub for general env vars: ${VAR_NAME}
    # This is a simple implementation
    def replacer(match):
        var_name = match.group(1)
        if var_name == "MDDAP_ONEDRIVE_ROOT":
            return str(get_onedrive_root()).replace("\\", "/")
        return os.getenv(var_name, match.group(0))
        
    content_expanded = re.sub(r'\$\{(\w+)\}', replacer, content)
    
    return yaml.safe_load(content_expanded)
