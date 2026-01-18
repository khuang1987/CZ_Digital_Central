"""Maintenance: Purge MES raw data by year.

Purpose:
  Remove MES raw rows and related ETL file-state entries for files belonging to a given year.
  Useful to re-run a subset of historical imports.

Modes:
  - Default is dry-run (no deletion) unless --yes is provided.
  - Can optionally purge SQL Server as well (see --purge-sqlserver).

Run:
  python scripts/maintenance/purge_mes_raw_by_year.py --year 2024 --yes
"""

import argparse
import glob
import os
import sqlite3
import sys
from pathlib import Path

try:
    import pyodbc  # type: ignore
except Exception:  # pragma: no cover
    pyodbc = None


def _build_sqlserver_conn_str(sql_server: str, sql_db: str, sql_driver: str) -> str:
    return (
        f"DRIVER={{{sql_driver}}};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_db};"
        "Trusted_Connection=yes;"
        "Encrypt=no;"
        "TrustServerCertificate=no;"
    )


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _connect_sqlite(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA busy_timeout = 30000;")
    return conn


def _load_config(cfg_path: Path) -> dict:
    sys.path.insert(0, str(_project_root()))
    from shared_infrastructure.utils.etl_utils import load_config

    return load_config(str(cfg_path))


def _resolve_mes_files(cfg: dict, only_year: str) -> list[str]:
    files: list[str] = []
    year = str(only_year).strip()

    for src in cfg.get("source", {}).get("mes_sources", []):
        if not src.get("enabled", True):
            continue
        pattern = src.get("pattern")
        if pattern:
            candidates = glob.glob(pattern)
        else:
            p = src.get("path")
            candidates = [p] if p else []

        for fp in candidates:
            base = os.path.basename(fp)
            # match _YYYY or _YYYYMM
            if f"_{year}." in base or f"_{year}" in base and base.split(f"_{year}")[-1][:2].isdigit():
                files.append(fp)

    return sorted(set(files))


def main() -> int:
    parser = argparse.ArgumentParser(description="Purge MES raw rows by year (SQLite + etl_file_state)")
    parser.add_argument("--year", required=True, help="Year to purge (e.g. 2025)")
    parser.add_argument(
        "--db-path",
        default=None,
        help="SQLite db path (default: data_pipelines/database/mddap_v2.db)",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Actually delete rows (otherwise dry-run)",
    )
    parser.add_argument(
        "--purge-sqlserver",
        action="store_true",
        help="Also purge SQL Server (dbo.raw_mes / dbo.etl_file_state) if reachable",
    )
    parser.add_argument("--sql-server", default=r"localhost\SQLEXPRESS")
    parser.add_argument("--sql-db", default="mddap_v2")
    parser.add_argument("--sql-driver", default="ODBC Driver 17 for SQL Server")
    args = parser.parse_args()

    project_root = _project_root()
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

    cfg = _load_config(cfg_path)
    year_files = _resolve_mes_files(cfg, args.year)

    print("=== Purge MES raw_mes by year ===")
    print("DB:", db_path)
    print("Config:", cfg_path)
    print("Year:", args.year)
    print("Matched source files:", len(year_files))
    for f in year_files:
        print("-", f)

    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    sqlserver_requested = bool(args.purge_sqlserver)
    sqlserver_enabled = bool(sqlserver_requested and pyodbc is not None)
    sql_raw_cnt = None
    sql_state_cnt = None

    with _connect_sqlite(db_path) as conn:
        cur = conn.cursor()

        # counts
        if year_files:
            q_marks = ",".join(["?"] * len(year_files))
            raw_cnt = cur.execute(
                f"SELECT COUNT(*) FROM raw_mes WHERE source_file IN ({q_marks})", tuple(year_files)
            ).fetchone()[0]
        else:
            raw_cnt = 0

        state_cnt = cur.execute(
            "SELECT COUNT(*) FROM etl_file_state WHERE etl_name LIKE 'mes_raw_%' AND file_path LIKE ?",
            (f"%_{args.year}%",),
        ).fetchone()[0]

        print("\nPlanned purge:")
        print(f"- raw_mes rows to delete: {raw_cnt}")
        print(f"- etl_file_state rows to delete (mes_raw_* + year match): {state_cnt}")

    if sqlserver_requested and pyodbc is None:
        print("\n[WARN] pyodbc not available; skip SQL Server purge")

    if sqlserver_enabled:
        conn_str = _build_sqlserver_conn_str(args.sql_server, args.sql_db, args.sql_driver)
        try:
            with pyodbc.connect(conn_str, autocommit=False) as sql_conn:
                cur = sql_conn.cursor()

                if year_files:
                    placeholders = ",".join(["?"] * len(year_files))
                    sql_raw_cnt = cur.execute(
                        f"SELECT COUNT(*) FROM dbo.raw_mes WHERE source_file IN ({placeholders})",
                        tuple(year_files),
                    ).fetchone()[0]
                else:
                    sql_raw_cnt = 0

                sql_state_cnt = cur.execute(
                    "SELECT COUNT(*) FROM dbo.etl_file_state WHERE etl_name LIKE 'mes_raw_%' AND file_path LIKE ?",
                    (f"%_{args.year}%",),
                ).fetchone()[0]

            print("\nPlanned purge (SQL Server):")
            print(f"- dbo.raw_mes rows to delete: {sql_raw_cnt}")
            print(f"- dbo.etl_file_state rows to delete: {sql_state_cnt}")
        except Exception as e:
            sqlserver_enabled = False
            print(
                f"\n[WARN] SQL Server not reachable or tables missing; skip SQL Server purge. {type(e).__name__}: {e}"
            )

    if not args.yes:
        print("\nDry-run. Re-run with --yes to execute deletion.")
        return 2

    # SQLite delete (primary)
    with _connect_sqlite(db_path) as conn:
        conn.execute("BEGIN")
        if year_files:
            q_marks = ",".join(["?"] * len(year_files))
            conn.execute(f"DELETE FROM raw_mes WHERE source_file IN ({q_marks})", tuple(year_files))
        conn.execute(
            "DELETE FROM etl_file_state WHERE etl_name LIKE 'mes_raw_%' AND file_path LIKE ?",
            (f"%_{args.year}%",),
        )
        conn.commit()

    if sqlserver_enabled:
        conn_str = _build_sqlserver_conn_str(args.sql_server, args.sql_db, args.sql_driver)
        try:
            with pyodbc.connect(conn_str, autocommit=False) as sql_conn:
                cur = sql_conn.cursor()
                if year_files:
                    placeholders = ",".join(["?"] * len(year_files))
                    cur.execute(
                        f"DELETE FROM dbo.raw_mes WHERE source_file IN ({placeholders})",
                        tuple(year_files),
                    )
                cur.execute(
                    "DELETE FROM dbo.etl_file_state WHERE etl_name LIKE 'mes_raw_%' AND file_path LIKE ?",
                    (f"%_{args.year}%",),
                )
                sql_conn.commit()
            print("SQL Server purge done.")
        except Exception as e:
            print(f"[WARN] SQL Server purge failed; SQLite already purged. {type(e).__name__}: {e}")

    print("\nPurge done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
