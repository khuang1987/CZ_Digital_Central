import argparse
from typing import List, Tuple

import pyodbc


def _conn_str(server: str, database: str, driver: str) -> str:
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
        "Encrypt=no;"
    )


def _safe_count(cur, obj: str) -> int | None:
    """Return COUNT(*) if object exists, else None."""
    schema, name = (obj.split(".", 1) + [""])[:2] if "." in obj else ("dbo", obj)

    cur.execute(
        """
        SELECT 1
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        UNION ALL
        SELECT 1
        FROM INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """,
        (schema, name, schema, name),
    )
    exists = cur.fetchone() is not None
    if not exists:
        return None

    cur.execute(f"SELECT COUNT(1) FROM {schema}.[{name}]")
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _print_counts(cur, objects: List[str]) -> None:
    print("=" * 80)
    print("SQL Server post-check: row counts")
    print("=" * 80)
    missing = 0
    for obj in objects:
        c = _safe_count(cur, obj)
        if c is None:
            print(f"- {obj}: MISSING")
            missing += 1
        else:
            print(f"- {obj}: {c:,}")
    if missing:
        print("-")
        print(f"Missing objects: {missing}")

    return missing


def main() -> int:
    parser = argparse.ArgumentParser(description="SQL Server post-check: basic row counts")
    parser.add_argument("--sql-server", type=str, default=r"localhost\SQLEXPRESS")
    parser.add_argument("--sql-database", type=str, default="mddap_v2")
    parser.add_argument("--driver", type=str, default="ODBC Driver 17 for SQL Server")
    parser.add_argument(
        "--objects",
        nargs="+",
        default=[
            "dbo.raw_mes",
            "dbo.raw_sfc",
            "dbo.raw_sfc_inspection",
            "dbo.raw_sap_routing",
            "dbo.planner_tasks",
            "dbo.planner_task_labels",
            "dbo.dim_calendar",
            "dbo.raw_calendar",
            "dbo.mes_metrics_snapshot_a",
            "dbo.mes_metrics_snapshot_b",
            "dbo.v_mes_metrics",
        ],
    )

    args = parser.parse_args()

    conn = pyodbc.connect(_conn_str(args.sql_server, args.sql_database, args.driver))
    try:
        cur = conn.cursor()
        missing = _print_counts(cur, list(args.objects))
    finally:
        conn.close()

    # Non-zero exit if any required objects are missing
    return 2 if missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
