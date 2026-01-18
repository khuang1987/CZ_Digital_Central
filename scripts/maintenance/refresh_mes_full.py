"""Maintenance: Full refresh helper for MES (SQLite).

Purpose:
  Helper script to purge MES-related data in SQLite and then run a full refresh process.

Safety:
  Requires --yes to actually purge and execute the refresh.

Run:
  python scripts/maintenance/refresh_mes_full.py --yes
"""

import argparse
import os
import sqlite3
import subprocess
import sys
import glob
from pathlib import Path


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA busy_timeout = 30000;")
    return conn


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
    ).fetchone()
    return row is not None


def _count(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> int:
    return int(conn.execute(sql, params).fetchone()[0])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db-path",
        default=None,
        help="SQLite db path (default: data_pipelines/database/mddap_v2.db)",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Actually purge MES data and run full refresh",
    )
    args = parser.parse_args()

    project_root = _project_root()
    sys.path.insert(0, str(project_root))

    db_path = (
        Path(args.db_path)
        if args.db_path
        else project_root / "data_pipelines" / "database" / "mddap_v2.db"
    )

    cfg_path = (
        project_root
        / "data_pipelines"
        / "sources"
        / "mes"
        / "config"
        / "config_mes_batch_report.yaml"
    )

    from shared_infrastructure.utils.etl_utils import load_config

    cfg = load_config(str(cfg_path))
    mes_sources = cfg.get("source", {}).get("mes_sources", [])

    files = []
    for s in mes_sources:
        if not s.get("enabled", True):
            continue
        pattern = s.get("pattern")
        if pattern:
            files.extend(glob.glob(pattern))
        elif s.get("path"):
            files.append(s["path"])

    files = sorted(set(files))

    print("=== MES Full Refresh (SQLite) ===")
    print("DB:", db_path)
    print("Config:", cfg_path)
    print("MES files matched:", len(files))
    if files:
        print("First file:", files[0])
        print("Last file:", files[-1])

    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    conn = _connect(db_path)
    try:
        raw_mes_rows = _count(conn, "SELECT COUNT(*) FROM raw_mes") if _table_exists(conn, "raw_mes") else 0
        state_rows = _count(conn, "SELECT COUNT(*) FROM etl_file_state WHERE etl_name LIKE 'mes_raw_%'")
        print("Before: raw_mes rows:", raw_mes_rows)
        print("Before: etl_file_state mes_raw_* rows:", state_rows)

        print("\nPlanned purge:")
        print("- DELETE FROM raw_mes")
        print("- DELETE FROM etl_file_state WHERE etl_name LIKE 'mes_raw_%'")
        print("- (optional) keep other domains unchanged")

        if not args.yes:
            print("\nDry-run mode. Re-run with --yes to execute purge and full refresh.")
            return 2

        conn.execute("BEGIN")
        if _table_exists(conn, "raw_mes"):
            conn.execute("DELETE FROM raw_mes")
        conn.execute("DELETE FROM etl_file_state WHERE etl_name LIKE 'mes_raw_%'")
        conn.commit()
        print("\nPurge done.")
    finally:
        conn.close()

    env = os.environ.copy()
    env["PYTHONPATH"] = str(project_root)

    def run_step(title: str, args_list: list[str]) -> None:
        print("\n---", title, "---")
        try:
            subprocess.run(args_list, cwd=str(project_root), env=env, check=True)
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] Step failed: {title}")
            print(f"[ERROR] Return code: {e.returncode}")
            raise

    run_step(
        "Import raw_mes from 30-MES导出数据",
        [sys.executable, "data_pipelines/sources/mes/etl/etl_mes_batch_output_raw.py"],
    )
    run_step(
        "Update v_mes_metrics view",
        [sys.executable, "scripts/update_db_view.py"],
    )
    run_step(
        "Export mes_batch_report partitions",
        [
            sys.executable,
            "scripts/export_core_to_a1.py",
            "--mode",
            "partitioned",
            "--months",
            "all",
            "--datasets",
            "mes_batch_report",
        ],
    )

    conn = _connect(db_path)
    try:
        raw_mes_rows_after = _count(conn, "SELECT COUNT(*) FROM raw_mes") if _table_exists(conn, "raw_mes") else 0
        state_rows_after = _count(conn, "SELECT COUNT(*) FROM etl_file_state WHERE etl_name LIKE 'mes_raw_%'")
        distinct_files = _count(conn, "SELECT COUNT(DISTINCT source_file) FROM raw_mes") if _table_exists(conn, "raw_mes") else 0
        print("\n=== Refresh Summary ===")
        print("After: raw_mes rows:", raw_mes_rows_after)
        print("After: raw_mes distinct source_file:", distinct_files)
        print("After: etl_file_state mes_raw_* rows:", state_rows_after)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
