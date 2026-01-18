import argparse
from pathlib import Path
from typing import Dict, List, Tuple

import sqlite3
import datetime
import math

import pandas as pd
import pyodbc

import traceback


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


def _get_sqlserver_columns(conn: pyodbc.Connection, table_name: str, schema: str = "dbo") -> List[str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """,
        (schema, table_name),
    )
    return [str(r[0]) for r in cur.fetchall() if r and r[0]]


def _get_sqlserver_column_types(conn: pyodbc.Connection, table_name: str, schema: str = "dbo") -> Dict[str, str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """,
        (schema, table_name),
    )
    return {str(r[0]): str(r[1]).lower() for r in cur.fetchall() if r and r[0] and r[1]}


def _get_sqlite_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    return [str(r[1]) for r in cur.fetchall() if r and r[1]]


def _read_sqlite_table(sqlite_db: Path, table_name: str) -> pd.DataFrame:
    conn = sqlite3.connect(str(sqlite_db))
    try:
        return pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    finally:
        conn.close()


def _read_sqlite_table_limited(sqlite_db: Path, table_name: str, max_rows: int | None) -> pd.DataFrame:
    if not max_rows:
        return _read_sqlite_table(sqlite_db, table_name)

    conn = sqlite3.connect(str(sqlite_db))
    try:
        return pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT {int(max_rows)}", conn)
    finally:
        conn.close()


def _clean_value(v):
    if v is None:
        return None
    if isinstance(v, str):
        return "".join(ch for ch in v if ord(ch) >= 32 or ch in "\t\n\r")
    if pd.isna(v):
        return None
    if hasattr(v, "to_pydatetime"):
        try:
            return v.to_pydatetime()
        except Exception:
            return None
    return v


def _coerce_by_sql_type(v, sql_type: str):
    """Coerce SQLite/pandas values into types that pyodbc can send to SQL Server reliably."""
    if v is None:
        return None

    # normalize pandas/numpy missing values
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    t = (sql_type or "").lower()

    # datetime/date handling
    if t in {"datetime", "datetime2", "smalldatetime", "date", "time"}:
        if isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
            return v
        if isinstance(v, str):
            try:
                dt = pd.to_datetime(v, errors="coerce")
                if pd.isna(dt):
                    return None
                py_dt = dt.to_pydatetime()
                if t == "date":
                    return py_dt.date()
                if t == "time":
                    return py_dt.time()
                return py_dt
            except Exception:
                return None
        return None

    # numeric handling
    if t in {"float", "real"}:
        if isinstance(v, (int, float)):
            f = float(v)
            return f if math.isfinite(f) else None
        if isinstance(v, str):
            try:
                f = float(v.replace(",", "").strip())
                return f if math.isfinite(f) else None
            except Exception:
                return None
        return None

    if t in {"int", "bigint", "smallint", "tinyint"}:
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, int):
            return v
        if isinstance(v, float):
            try:
                if not math.isfinite(v):
                    return None
                return int(v)
            except Exception:
                return None
        if isinstance(v, str):
            try:
                f = float(v.replace(",", "").strip())
                if not math.isfinite(f):
                    return None
                return int(f)
            except Exception:
                return None
        return None

    # nvarchar/text: keep cleaned string
    cleaned = _clean_value(v)
    if isinstance(cleaned, str) or cleaned is None:
        return cleaned
    # If it's not a string already, keep as-is (SQL Server nvarchar can accept numbers too)
    return cleaned


def _bulk_insert_sqlserver(
    conn: pyodbc.Connection,
    df: pd.DataFrame,
    table_name: str,
    schema: str = "dbo",
    chunk_size: int = 1000,
) -> None:
    if df.empty:
        return

    sql_cols = _get_sqlserver_columns(conn, table_name, schema=schema)
    sql_types = _get_sqlserver_column_types(conn, table_name, schema=schema)

    # Avoid inserting identity id if present; also ensure only columns that exist in SQL Server are used.
    # Also skip created_at/updated_at during backfill (they are nullable and often stored as strings in SQLite).
    skip_cols = {"id", "created_at", "updated_at"}
    insert_cols = [c for c in df.columns if c in sql_cols and c.lower() not in skip_cols]
    if not insert_cols:
        raise RuntimeError(
            "No insertable columns (SQLite columns do not match SQL Server columns). "
            f"table={schema}.{table_name}; sqlite_cols={list(df.columns)}; sqlserver_cols={sql_cols}"
        )

    print(f"{schema}.{table_name}: insert_cols ({len(insert_cols)}): {insert_cols}")

    cols_sql = ", ".join(f"[{c}]" for c in insert_cols)
    placeholders = ", ".join(["?"] * len(insert_cols))
    insert_sql = f"INSERT INTO [{schema}].[{table_name}] ({cols_sql}) VALUES ({placeholders})"

    cur = conn.cursor()
    try:
        cur.fast_executemany = True
    except Exception:
        pass

    total_rows = len(df)
    for start in range(0, total_rows, chunk_size):
        end = min(start + chunk_size, total_rows)
        chunk_df = df.iloc[start:end][insert_cols]
        rows = [tuple(_coerce_by_sql_type(r[c], sql_types.get(c, "")) for c in insert_cols) for _, r in chunk_df.iterrows()]

        try:
            cur.executemany(insert_sql, rows)
            conn.commit()
        except Exception as e:
            print(f"{schema}.{table_name}: executemany failed for chunk {start}-{end-1}: {e}")
            conn.rollback()
            # Fall back to per-row inserts to avoid losing whole chunk
            failed = 0
            for row_vals in rows:
                try:
                    cur.execute(insert_sql, row_vals)
                except Exception as row_e:
                    # skip bad row
                    failed += 1
                    if failed <= 3:
                        print(f"{schema}.{table_name}: row insert failed: {row_e}")
            conn.commit()

            if failed:
                print(f"{schema}.{table_name}: chunk {start}-{end-1} fallback inserted with failures={failed}")

        # Always print progress (large tables need visibility)
        print(f"{schema}.{table_name}: inserted {end}/{total_rows}")


def _delete_sqlserver_table(conn: pyodbc.Connection, table_name: str, schema: str = "dbo") -> None:
    """DELETE rows from SQL Server table, deleting known dependent child tables first."""

    # Minimal, explicit dependency handling for known FK relationship
    # planner_task_labels(TaskId) -> planner_tasks(TaskId)
    delete_order = {
        "planner_tasks": ["planner_task_labels"],
    }

    cur = conn.cursor()
    for child in delete_order.get(table_name, []):
        cur.execute(f"DELETE FROM [{schema}].[{child}]")
    cur.execute(f"DELETE FROM [{schema}].[{table_name}]")


def backfill_table(
    sqlite_db: Path,
    sql_server: str,
    sql_database: str,
    driver: str,
    table_name: str,
    schema: str = "dbo",
    chunk_size: int = 1000,
    max_rows: int | None = None,
) -> Tuple[int, int]:
    df = _read_sqlite_table_limited(sqlite_db, table_name, max_rows)

    conn_str = _build_sqlserver_conn_str(sql_server, sql_database, driver)
    conn = pyodbc.connect(conn_str, autocommit=False)
    try:
        print(f"{schema}.{table_name}: reading sqlite rows={len(df)}")
        _delete_sqlserver_table(conn, table_name, schema=schema)
        conn.commit()
        print(f"{schema}.{table_name}: delete complete")

        _bulk_insert_sqlserver(conn, df, table_name, schema=schema, chunk_size=chunk_size)
        print(f"{schema}.{table_name}: insert complete")

        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(1) FROM [{schema}].[{table_name}]")
        sql_count = int(cur.fetchone()[0])
        return len(df), sql_count
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill SQL Server tables from SQLite (DELETE + full reload)")
    parser.add_argument(
        "--project-root",
        type=str,
        default=str(Path(__file__).resolve().parents[2]),
        help="Project root path (defaults to repo root)",
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
    parser.add_argument("--chunk-size", type=int, default=1000)
    parser.add_argument("--max-rows", type=int, default=None, help="Limit rows read from SQLite (debug)")
    parser.add_argument(
        "--tables",
        nargs="+",
        default=["raw_mes", "planner_tasks"],
        help="Tables to backfill (SQL Server side will be DELETE + full reload)",
    )

    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    sqlite_db = Path(args.sqlite_db).resolve() if args.sqlite_db else _default_sqlite_db_path(project_root)

    if not sqlite_db.exists():
        raise FileNotFoundError(f"SQLite DB not found: {sqlite_db}")

    print(f"SQLite DB: {sqlite_db}")
    print(f"SQL Server: {args.sql_server} / DB: {args.sql_database} / Schema: {args.schema}")
    print("-")
    print("WARNING: This will DELETE and fully reload the specified SQL Server tables.")
    print("-")

    any_mismatch = False
    for t in args.tables:
        try:
            sqlite_count, sql_count = backfill_table(
                sqlite_db=sqlite_db,
                sql_server=args.sql_server,
                sql_database=args.sql_database,
                driver=args.driver,
                table_name=t,
                schema=args.schema,
                chunk_size=args.chunk_size,
                max_rows=args.max_rows,
            )
            match = "YES" if sqlite_count == sql_count else "NO"
            if match == "NO":
                any_mismatch = True
            print(f"{t}: sqlite={sqlite_count}, sqlserver={sql_count}, match={match}")
        except Exception as e:
            any_mismatch = True
            print(f"{t}: FAILED: {e}")
            print(traceback.format_exc())

    return 0 if not any_mismatch else 2


if __name__ == "__main__":
    raise SystemExit(main())
