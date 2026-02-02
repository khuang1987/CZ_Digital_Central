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
    
    # Use denormalize logic for consistent result
    resolved = denormalize_path(path_str)
    return Path(resolved)

def normalize_path(path_str: str) -> str:
    """
    Convert an absolute OneDrive path into a placeholder-based path.
    Example: C:/Users/huangk14/OneDrive... -> ${MDDAP_ONEDRIVE_ROOT}/...
    """
    if not path_str:
        return path_str
        
    on_drive_root = str(get_onedrive_root()).replace("\\", "/")
    path_str = path_str.replace("\\", "/")
    
    # 1. Check current user's root
    if path_str.startswith(on_drive_root):
        return path_str.replace(on_drive_root, "${MDDAP_ONEDRIVE_ROOT}")
        
    # 2. Check hardcoded legacy roots (for migration)
    legacy_roots = [
        "C:/Users/huangk14/OneDrive - Medtronic PLC",
        "C:/Users/czxmfg/OneDrive - Medtronic PLC"
    ]
    
    for legacy in legacy_roots:
        if path_str.startswith(legacy):
            return path_str.replace(legacy, "${MDDAP_ONEDRIVE_ROOT}")
            
    return path_str

def denormalize_path(path_str: str) -> str:
    """
    Convert a placeholder-based path back to an absolute path for the current user.
    """
    if not path_str:
        return path_str
        
    on_drive_root = str(get_onedrive_root()).replace("\\", "/")
    
    # Replace placeholder
    resolved = path_str.replace("${MDDAP_ONEDRIVE_ROOT}", on_drive_root)
    
    # Also handle legacy hardcoded strings in case they are still in config
    legacy_roots = [
        "C:/Users/huangk14/OneDrive - Medtronic PLC",
        "C:/Users/czxmfg/OneDrive - Medtronic PLC"
    ]
    
    for legacy in legacy_roots:
        if resolved.startswith(legacy):
            resolved = resolved.replace(legacy, on_drive_root)
            
    return resolved.replace("\\", "/")

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
