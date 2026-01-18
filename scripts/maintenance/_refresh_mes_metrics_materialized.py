import pyodbc
from pathlib import Path
import re


def _resolve_sql_file(filename: str) -> Path:
    # Look in the same directory as this script (scripts/maintenance)
    current_dir = Path(__file__).resolve().parent
    return current_dir / filename


INIT_SQL_FILE = _resolve_sql_file("_init_mes_metrics_materialized.sql")
COMPUTE_SQL_FILE = _resolve_sql_file("_compute_mes_metrics_materialized.sql")
INCREMENTAL_SQL_FILE = _resolve_sql_file("_compute_mes_metrics_incremental.sql")


def _split_go(sql_text: str) -> list[str]:
    # Split batches by lines that contain only 'GO' (SSMS batch separator).
    # Do NOT split on substring 'GO' because it appears in tokens like 'GROUP'.
    parts = re.split(r"(?im)^\s*GO\s*$", sql_text)
    return [stmt.strip() for stmt in parts if stmt.strip()]


def _connect_sqlserver_any(database: str) -> pyodbc.Connection:
    servers = [r"localhost\SQLEXPRESS", "(local)"]
    last_err: Exception | None = None

    for server in servers:
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};"
            f"DATABASE={database};"
            "Trusted_Connection=yes;"
            "Encrypt=no;"
            "Connection Timeout=5;"
        )
        try:
            conn = pyodbc.connect(conn_str, autocommit=False)
            print(f"✓ connected to SQL Server: {server} ({database})")
            return conn
        except Exception as e:
            last_err = e
            continue

    raise RuntimeError(f"failed to connect to SQL Server (tried: {servers}): {last_err}")


def _get_current_snapshot(cur) -> str | None:
    cur.execute(
        """
        SELECT base_object_name
        FROM sys.synonyms
        WHERE name = 'mes_metrics_current'
            AND schema_id = SCHEMA_ID('dbo')
        """
    )
    row = cur.fetchone()
    if not row:
        return None
    base = row[0]
    if base is None:
        return None
    base_lower = str(base).lower()
    # Normalize synonym target to strict names if possible, or return as is
    if base_lower.endswith("mes_metrics_snapshot_a"):
        return "dbo.mes_metrics_snapshot_a"
    if base_lower.endswith("mes_metrics_snapshot_b"):
        return "dbo.mes_metrics_snapshot_b"
    return str(base)


def _choose_next_snapshot(current: str | None) -> str:
    if current is None:
        return "dbo.mes_metrics_snapshot_a"
    if current.lower().endswith("mes_metrics_snapshot_a"):
        return "dbo.mes_metrics_snapshot_b"
    return "dbo.mes_metrics_snapshot_a"


def _get_max_id(cur, table: str) -> int | None:
    try:
        cur.execute(f"SELECT MAX(id) FROM {table}")
        row = cur.fetchone()
        return row[0] if row else None
    except Exception:
        # Table might not exist or empty
        return None


import os

def main() -> None:
    # Threshold for incremental update (rows). If delta is higher, force full refresh.
    INCREMENTAL_THRESHOLD = 100000
    FORCE_REFRESH = os.environ.get("FORCE_REFRESH", "0") == "1"

    with _connect_sqlserver_any("mddap_v2") as conn:
        conn.timeout = 0
        cur = conn.cursor()
        cur.execute("SET LOCK_TIMEOUT 5000")

        # 1. Ensure schema/tables exist
        init_sql = INIT_SQL_FILE.read_text(encoding="utf-8")
        init_stmts = _split_go(init_sql)
        for i, stmt in enumerate(init_stmts, start=1):
            try:
                cur.execute(stmt)
                conn.commit()
            except Exception:
                print(f"! init statement failed at {i}/{len(init_stmts)}")
                raise

        # 2. Determine State
        current_snapshot = _get_current_snapshot(cur)
        
        # Get Max IDs
        max_raw_id = _get_max_id(cur, "dbo.raw_mes") or 0
        max_snap_id = 0
        if current_snapshot:
            max_snap_id = _get_max_id(cur, current_snapshot) or 0

        print(f"State: RawMax={max_raw_id}, SnapMax={max_snap_id}, CurrentSnap={current_snapshot}")

        # 3. Decide Strategy
        strategy = "FULL"
        target_table = _choose_next_snapshot(current_snapshot)
        min_id_arg = 0

        if FORCE_REFRESH:
            print("-> Force Refresh requested (bypassing up-to-date check)")
        elif current_snapshot and max_snap_id > 0:
            if max_raw_id == max_snap_id:
                print("✓ Data is already up to date. No refresh needed.")
                return
            elif max_raw_id > max_snap_id:
                delta = max_raw_id - max_snap_id
                if delta <= INCREMENTAL_THRESHOLD:
                    strategy = "INCREMENTAL"
                    target_table = current_snapshot
                    min_id_arg = max_snap_id + 1
                    print(f"-> Incremental Refresh selected (Delta={delta} rows)")
                else:
                    print(f"-> Full Refresh selected (Delta={delta} > Threshold {INCREMENTAL_THRESHOLD})")
            else:
                # max_raw < max_snap ? Unusual. Maybe raw table truncated. Full refresh to be safe.
                print("-> Full Refresh selected (Raw ID < Snap ID, potential reset detected)")
        else:
            print("-> Full Refresh selected (First run or empty snapshot)")

        # 4. Execute Logic
        if strategy == "INCREMENTAL":
            sql_template = INCREMENTAL_SQL_FILE.read_text(encoding="utf-8")
            sql_final = sql_template.replace("{{TARGET_TABLE}}", target_table)
            sql_final = sql_final.replace("{{MIN_ID}}", str(min_id_arg))
            
            # Execute Incremental Batches
            stmts = _split_go(sql_final)
            print(f"Executing {len(stmts)} incremental batches...")
            for i, stmt in enumerate(stmts, start=1):
                print(f"[inc] {i}/{len(stmts)} executing...")
                try:
                    cur.execute(stmt)
                    conn.commit()
                except Exception:
                    print(f"! incremental statement failed at {i}/{len(stmts)}")
                    print(f"Snippet: {stmt[:200]}...")
                    raise
            
            print(f"✓ Incremental refresh done. Appended {max_raw_id - max_snap_id} rows to {target_table}.")

        else:
            # Full Refresh (Existing Logic)
            compute_sql = COMPUTE_SQL_FILE.read_text(encoding="utf-8")
            if "." not in target_table:
                raise RuntimeError(f"target table must be schema-qualified, got: {target_table}")
            
            compute_sql = compute_sql.replace("{{TARGET_TABLE}}", target_table)
            stmts = _split_go(compute_sql)
            
            print(f"Executing {len(stmts)} full refresh batches into {target_table}...")
            for i, stmt in enumerate(stmts, start=1):
                print(f"[full] {i}/{len(stmts)} executing...")
                try:
                    cur.execute(stmt)
                    conn.commit()
                except Exception:
                    print(f"! full statement failed at {i}/{len(stmts)}")
                    print(f"Snippet: {stmt[:200]}...")
                    raise

            # Verification and Switch
            cur.execute(f"SELECT COUNT(*) FROM {target_table}")
            new_cnt = cur.fetchone()[0]
            
            # Basic sanity check
            if new_cnt == 0 and max_raw_id > 0:
                print("! Warning: Snapshot produced 0 rows but raw data exists.")
            
            # Switch Synonym
            print(f"Switching synonym dbo.mes_metrics_current -> {target_table}")
            cur.execute("IF OBJECT_ID('dbo.mes_metrics_current', 'SN') IS NOT NULL DROP SYNONYM dbo.mes_metrics_current;")
            cur.execute(f"CREATE SYNONYM dbo.mes_metrics_current FOR {target_table};")
            conn.commit()
            
            print(f"✓ Full refresh done. Snapshot: {target_table} ({new_cnt:,} rows).")



if __name__ == "__main__":
    main()
