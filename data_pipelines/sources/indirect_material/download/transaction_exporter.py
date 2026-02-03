"""
Tooling Transaction Exporter
Function: Exports TransactionLog from SPS SQL Server.
"""

import os
import sys
import logging
import pandas as pd
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
import contextlib

# Add project root needed for shared modules
PROJECT_ROOT = Path(__file__).resolve().parents[4]
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

from shared_infrastructure.env_utils import resolve_path

logger = logging.getLogger(__name__)

# ============================================================
# Core Logic
# ============================================================

def create_db_engine(server, database, username, password):
    # SPS uses standard SQL Server port 1433 typically
    connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=SQL+Server'
    return create_engine(
        connection_string,
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800
    )

@contextlib.contextmanager
def get_db_connection(engine):
    connection = engine.connect()
    try:
        yield connection
    finally:
        connection.close()

def check_wifi_ssid(target_ssid="mdtmobile"):
    """Check if connected to specific Wi-Fi (SPS requirement)."""
    try:
        # Use netsh
        result = subprocess.check_output(
            ["netsh", "wlan", "show", "interfaces"], 
            stderr=subprocess.DEVNULL, 
            timeout=3,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
        ).decode("utf-8", errors='ignore')
        
        for line in result.splitlines():
            if "SSID" in line and "BSSID" not in line:
                current_ssid = line.split(":", 1)[1].strip()
                if current_ssid.lower() == target_ssid.lower():
                    return True
                else:
                    logger.warning(f"Current Wi-Fi: {current_ssid}, Required: {target_ssid}")
                    return False
        return False
    except Exception as e:
        logger.warning(f"Wi-Fi check failed: {e}")
        return False

def get_output_dir():
    # User requested specific path: 
    # "C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\20-GoodsMovement"
    # resolving it dynamically for portability
    raw_path = r"C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\20-GoodsMovement"
    path = resolve_path(raw_path)
    
    if not path.exists():
        logger.warning(f"Target path not found: {path}, falling back to local workspace.")
        path = PROJECT_ROOT / "data" / "raw" / "tooling"
        
    path.mkdir(parents=True, exist_ok=True)
    return path

def fetch_transactions(year: int) -> pd.DataFrame:
    """
    Fetch transactions for a specific year from SPS.
    Returns DataFrame.
    """
    logger.debug(f"Fetching indirect material transactions for {year}...")
    
    # 1. Wi-Fi Check
    if not check_wifi_ssid("mdtmobile"):
        if os.getenv("MDDAP_SKIP_WIFI_CHECK", "false").lower() != "true":
            logger.error("❌ Must be connected to 'mdtmobile' Wi-Fi for SPS access.")
            raise ConnectionError("Incorrect Wi-Fi network")
        else:
            logger.warning("⚠️ Skipping Wi-Fi check (MDDAP_SKIP_WIFI_CHECK=true)")

    # 2. Database Connection
    server = os.getenv("MDDAP_SPS_SERVER", "192.168.103.1")
    database = os.getenv("MDDAP_SPS_DATABASE", "sps")
    username = os.getenv("MDDAP_SPS_USER", "sa")
    password = os.getenv("MDDAP_SPS_PASSWORD", "sps")

    if not all([server, database, username, password]):
        logger.error("❌ SPS Database credentials missing in .env (MDDAP_SPS_*)")
        raise ValueError("Missing SPS credentials")

    engine = None
    try:
        engine = create_db_engine(server, database, username, password)
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise

    # 3. Query
    start_time = datetime(year, 1, 1)
    end_time = datetime(year + 1, 1, 1) if year != datetime.now().year else datetime.now()
    
    query = """
        SELECT 
            T.TRANSTARTDATETIME AS StartTime,
            T.TRANENDDATETIME AS EndTime,
            T.ITEMNUMBER AS ItemNumber,
            I.DESCR AS ItemDescription,
            I.ITEMGROUP AS ItemGroup,
            T.JOBNUMBER AS JobNumber,
            T.AUX1 AS BatchNumber,
            T.AUX2 AS OperationNumber,
            T.MACHINENUMBER AS MachineNumber,
            U.DESCR AS EmployeeName,
            T.QTY AS Quantity,
            V.DESCR AS VendingMachine,
            T.LOCATIONTEXT AS Location,
            T.USERGROUP01 AS Area
        FROM dbo.TransactionLog T
        JOIN dbo.Users U ON T.USERNUMBER = U.USERNUMBER
        JOIN dbo.VendingMachines V ON T.VMID = V.VMID
        JOIN dbo.Items I ON T.ITEMNUMBER = I.ITEMNUMBER
        WHERE T.TRANENDDATETIME >= ? 
          AND T.TRANENDDATETIME < ?
        ORDER BY T.TRANSTARTDATETIME
    """
    
    try:
         with get_db_connection(engine) as conn:
            # chunksize used internally by read_sql, but we want full DF here
            df = pd.read_sql(query, conn, params=(start_time, end_time))
            logger.info(f"✅ Fetched {year}: {len(df)} rows")
            return df
                
    except Exception as e:
        logger.error(f"❌ Failed to fetch {year}: {e}")
        raise

if __name__ == "__main__":
    # Test run
    logging.basicConfig(level=logging.INFO)
    try:
        df = fetch_transactions(datetime.now().year)
        print(df.head())
    except Exception as e:
        print(e)
