import argparse
from pathlib import Path
from typing import Dict, List, Tuple

import sqlite3

import pyodbc


def _get_sqlite_counts(db_path: Path, tables: List[str]) -> Dict[str, int]:
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        out: Dict[str, int] = {}
        for t in tables:
            cur.execute(f"SELECT COUNT(1) FROM {t}")
            out[t] = int(cur.fetchone()[0])
        return out
    finally:
        conn.close()


def _get_sqlserver_counts(conn_str: str, tables: List[str], schema: str = "dbo") -> Dict[str, int]:
    conn = pyodbc.connect(conn_str)
    try:
        cur = conn.cursor()
        out: Dict[str, int] = {}
        for t in tables:
            cur.execute(f"SELECT COUNT(1) FROM [{schema}].[{t}]")
            row = cur.fetchone()
            out[t] = int(row[0]) if row is not None else 0
        return out
    finally:
        conn.close()


def _default_sqlite_db_path(project_root: Path) -> Path:
    return project_root / "data_pipelines" / "database" / "mddap_v2.db"


def _build_sqlserver_conn_str(server: str, database: str, driver: str) -> str:
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )


def compare_counts(
    sqlite_db: Path,
    sql_server: str,
    sql_database: str,
    driver: str,
    tables: List[str],
    schema: str = "dbo",
) -> Tuple[bool, Dict[str, Tuple[int, int]]]:
    sqlite_counts = _get_sqlite_counts(sqlite_db, tables)
    conn_str = _build_sqlserver_conn_str(sql_server, sql_database, driver)
    sqlserver_counts = _get_sqlserver_counts(conn_str, tables, schema=schema)

    results: Dict[str, Tuple[int, int]] = {}
    ok = True
    for t in tables:
        sc = sqlite_counts.get(t, -1)
        mc = sqlserver_counts.get(t, -1)
        results[t] = (sc, mc)
        if sc != mc:
            ok = False
    return ok, results


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare row counts between SQLite and SQL Server")
    parser.add_argument(
        "--project-root",
        type=str,
        default=str(Path(__file__).resolve().parents[2]),
        help="Project root path (defaults to scripts/..)",
    )
    parser.add_argument(
        "--sqlite-db",
        type=str,
        default=None,
        help="Path to SQLite db (defaults to <project_root>/data_pipelines/database/mddap_v2.db)",
    )
    parser.add_argument("--sql-server", type=str, default=r"localhost\SQLEXPRESS")
    parser.add_argument("--sql-database", type=str, default="mddap_v2")
    parser.add_argument("--driver", type=str, default="ODBC Driver 17 for SQL Server")
    parser.add_argument("--schema", type=str, default="dbo")
    parser.add_argument(
        "--tables",
        nargs="+",
        default=[
            "raw_sap_routing",
            "raw_sfc",
            "raw_sfc_inspection",
            "raw_mes",
            "planner_tasks",
        ],
    )

    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    sqlite_db = Path(args.sqlite_db).resolve() if args.sqlite_db else _default_sqlite_db_path(project_root)

    if not sqlite_db.exists():
        raise FileNotFoundError(f"SQLite DB not found: {sqlite_db}")

    ok, results = compare_counts(
        sqlite_db=sqlite_db,
        sql_server=args.sql_server,
        sql_database=args.sql_database,
        driver=args.driver,
        tables=args.tables,
        schema=args.schema,
    )

    header_left = "SQLite"
    header_right = "SQLServer"
    print(f"SQLite DB: {sqlite_db}")
    print(f"SQL Server: {args.sql_server} / DB: {args.sql_database} / Schema: {args.schema}")
    print("-")

    width_table = max(len(t) for t in results.keys()) if results else 10
    print(f"{'table'.ljust(width_table)} | {header_left.rjust(10)} | {header_right.rjust(10)} | match")
    print(f"{'-' * width_table}-+-{'-' * 12}-+-{'-' * 12}-+------")
    for t, (sc, mc) in results.items():
        match = "YES" if sc == mc else "NO"
        print(f"{t.ljust(width_table)} | {str(sc).rjust(10)} | {str(mc).rjust(10)} | {match}")

    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
