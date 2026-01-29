import sys
import os
import logging
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Setup simple logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def test_connection():
    print("=" * 60)
    print("  MDDAP Database Connection Test Utility")
    print("=" * 60)
    
    # 1. Load Environment Variables
    try:
        from shared_infrastructure.env_utils import load_dotenv
        env_path = PROJECT_ROOT / ".env"
        if env_path.exists():
            print(f"[INFO] Loading configuration from: {env_path}")
            load_dotenv(env_path)
        else:
            print(f"[WARN] No .env file found at: {env_path}")
            print("       Using system environment variables or defaults.")
    except Exception as e:
        print(f"[WARN] Failed to load .env: {e}")

    # 2. Print Configuration (Masking Password)
    server = os.getenv("MDDAP_SQL_SERVER", "localhost\SQLEXPRESS")
    db = os.getenv("MDDAP_SQL_DATABASE", "mddap_v2")
    user = os.getenv("MDDAP_SQL_USER", "")
    
    print("-" * 60)
    print(f"Target Server:   {server}")
    print(f"Target Database: {db}")
    print(f"User:            {user if user else '(Windows Auth)'}")
    print("-" * 60)

    # 3. Attempt Connection
    try:
        from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager
        
        print("Connecting...", end=" ", flush=True)
        db_manager = SQLServerOnlyManager()
        
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # 4. Run Simple Query
            cursor.execute("SELECT @@VERSION")
            row = cursor.fetchone()
            version_info = row[0] if row else "Unknown"
            
            # Simple line clean up for version string
            version_short = version_info.split('\n')[0].strip()
            
            print("SUCCESS! ✅")
            print(f"\n[Server Version]: {version_short}")
            
            # 5. Check Database Access
            cursor.execute(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
            table_count = cursor.fetchone()[0]
            print(f"[Database Access]: OK. Found {table_count} tables in '{db}'.")
            
    except Exception as e:
        print("FAILED! ❌")
        print("\n" + "=" * 20 + " ERROR DETAILS " + "=" * 20)
        print(e)
        print("=" * 55)
        print("\nTroubleshooting Tips:")
        print("1. Check if the IP address and Port (1433) are correct in .env")
        print("2. Check if the SQL User and Password are correct.")
        print("3. Ensure the Server Firewall allows Inbound Traffic on Port 1433.")
        print("4. Ensure 'TCP/IP' protocol is Enabled in SQL Server Configuration Manager on the server.")

    print("\n" + "=" * 60)
    input("Press Enter to exit...")

if __name__ == "__main__":
    test_connection()
