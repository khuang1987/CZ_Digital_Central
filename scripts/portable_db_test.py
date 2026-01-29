import sys
import os
import time

# Try importing pyodbc (required)
try:
    import pyodbc
except ImportError:
    print("CRITICAL ERROR: 'pyodbc' library is not installed.")
    print("Please install it running: pip install pyodbc")
    print("Or for offline: pip install --no-index --find-links=offline_packages pyodbc")
    input("Press Enter to exit...")
    sys.exit(1)

def load_simple_env(env_path):
    """Simple .env loader to avoid dependency on python-dotenv"""
    config = {}
    if not os.path.exists(env_path):
        return config
    
    print(f"Loading configuration from: {env_path}")
    with open(env_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, value = line.split('=', 1)
                config[key.strip()] = value.strip()
    return config

def main():
    print("=" * 60)
    print("  MDDAP Portable Database Connection Tester")
    print("=" * 60)

    # 1. Determine paths
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Check for .env in current dir, or parent dir (if script is in scripts/)
    env_locs = [
        os.path.join(current_dir, ".env"),
        os.path.join(os.path.dirname(current_dir), ".env")
    ]
    
    config = {}
    for p in env_locs:
        if os.path.exists(p):
            config = load_simple_env(p)
            break
            
    # 2. Get Settings (Env -> Default -> User Input)
    server = config.get("MDDAP_SQL_SERVER", os.getenv("MDDAP_SQL_SERVER", "localhost\\SQLEXPRESS"))
    db = config.get("MDDAP_SQL_DATABASE", os.getenv("MDDAP_SQL_DATABASE", "mddap_v2"))
    driver = config.get("MDDAP_SQL_DRIVER", os.getenv("MDDAP_SQL_DRIVER", "ODBC Driver 17 for SQL Server"))
    user = config.get("MDDAP_SQL_USER", os.getenv("MDDAP_SQL_USER", ""))
    password = config.get("MDDAP_SQL_PASSWORD", os.getenv("MDDAP_SQL_PASSWORD", ""))

    print("-" * 60)
    print(f"Server:   {server}")
    print(f"Database: {db}")
    print(f"Driver:   {driver}")
    print(f"User:     {user if user else '(Windows Auth)'}")
    print("-" * 60)
    
    # 3. Build Connection String
    conn_str_parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={server}",
        f"DATABASE={db}",
        "Encrypt=no",
        "TrustServerCertificate=no"
    ]
    
    if user and password:
        # SQL Auth
        conn_str_parts.append(f"UID={user}")
        conn_str_parts.append(f"PWD={password}")
    else:
        # Windows Auth
        conn_str_parts.append("Trusted_Connection=yes")
        
    conn_str = ";".join(conn_str_parts)

    # 4. Connect
    print("Attempting connection...", end=" ", flush=True)
    try:
        start_time = time.time()
        conn = pyodbc.connect(conn_str, timeout=10)
        duration = time.time() - start_time
        
        print(f"SUCCESS! ({duration:.2f}s) \u2705")
        
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        ver = cursor.fetchone()[0]
        print(f"\n[Server Version]: {ver.splitlines()[0]}")
        
        try:
            cursor.execute("SELECT DB_NAME()")
            curr_db = cursor.fetchone()[0]
            print(f"[Current DB]: {curr_db}")
        except:
            pass
            
        conn.close()
        
    except pyodbc.Error as e:
        print("FAILED! \u274C")
        print("\n" + "=" * 20 + " ERROR DETAIL " + "=" * 20)
        try:
            # Decode SQL Server error layout if possible
            print(f"Error Code: {e.args[0]}")
            print(f"Message: {e.args[1]}")
        except:
            print(e)
        print("=" * 54)
        print("Troubleshooting:")
        print("1. Check Firewall on Server (Port 1433 must be Open).")
        print("2. Check TCP/IP Protocol enabled in SQL Config Manager.")
        print("3. Verify Ping to Server IP.")
        print("4. Verify Username/Password.")

    print("\n" + "=" * 60)
    if sys.platform == "win32":
        os.system("pause")
    else:
        input("Press Enter to exit...")

if __name__ == "__main__":
    main()
