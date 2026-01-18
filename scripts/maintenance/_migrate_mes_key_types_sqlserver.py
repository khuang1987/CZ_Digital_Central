"""Maintenance: Migrate MES/routing join key column types in SQL Server.

Purpose:
  One-off migration helper to convert key columns used for routing joins to numeric types.
  This is intended to fix mismatches caused by string vs numeric join keys.

Targets (SQL Server):
  - dbo.raw_mes: Operation -> INT, Plant -> INT, [Group] -> BIGINT
  - dbo.raw_sap_routing: Operation -> INT, Plant -> INT, [Group] -> BIGINT
  - dbo.raw_sfc: Operation -> INT

Safety:
  Prompts for confirmation. Also aborts if non-numeric values are detected.

Run:
  python scripts/maintenance/_migrate_mes_key_types_sqlserver.py
"""

import sys
from typing import Iterable

try:
    import pyodbc
except ImportError:
    print("ERROR: pyodbc not installed")
    raise


DB_NAME = "mddap_v2"
SERVERS = [r"localhost\SQLEXPRESS", "(local)"]


def _connect() -> pyodbc.Connection:
    last_err: Exception | None = None
    for server in SERVERS:
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};"
            f"DATABASE={DB_NAME};"
            "Trusted_Connection=yes;"
            "Encrypt=no;"
            "Connection Timeout=10;"
        )
        try:
            conn = pyodbc.connect(conn_str, autocommit=False)
            print(f"✓ connected: {server} ({DB_NAME})")
            return conn
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"failed to connect to SQL Server (tried={SERVERS}): {last_err}")


def _fetch_one(cur, sql: str, params: Iterable | None = None):
    cur.execute(sql, params or [])
    return cur.fetchone()


def _exec(cur, sql: str) -> None:
    cur.execute(sql)


def _exec_step(cur, title: str, sql: str) -> None:
    print(f"\n--> {title}")
    cur.execute(sql)


def _print_column_types(cur, table: str, cols: list[str]) -> None:
    placeholders = ",".join(["?"] * len(cols))
    cur.execute(
        f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=?
          AND COLUMN_NAME IN ({placeholders})
        ORDER BY COLUMN_NAME
        """,
        [table] + cols,
    )
    rows = cur.fetchall()
    print(f"\n[dbo.{table}] column types:")
    for r in rows:
        print(f"  {r[0]}: {r[1]}({r[2]})")


def main() -> None:
    print("=" * 80)
    print("Migrate key column types to INT/BIGINT (SQL Server)")
    print("Targets:")
    print("  dbo.raw_mes: Operation INT, Plant INT, [Group] BIGINT")
    print("  dbo.raw_sap_routing: Operation INT, Plant INT, [Group] BIGINT")
    print("  dbo.raw_sfc: Operation INT")
    print("Notes:")
    print("  - keeps old columns as *_str for rollback")
    print("  - aborts if any non-numeric values are found")
    print("=" * 80)

    confirm = input("Type YES to continue: ").strip()
    if confirm != "YES":
        print("Aborted.")
        

    with _connect() as conn:
        cur = conn.cursor()
        cur.execute("SET XACT_ABORT ON")

        try:
            cur.execute("BEGIN TRAN")

            # quick schema overview
            _print_column_types(cur, "raw_mes", ["Operation", "Plant", "Group"])  # Group is stored as [Group]
            _print_column_types(cur, "raw_sap_routing", ["Operation", "Plant", "Group"])  # Group is stored as [Group]
            _print_column_types(cur, "raw_sfc", ["Operation"])  # Operation

            # -----------------------------------------------------------------
            # dbo.raw_mes
            # -----------------------------------------------------------------
            _exec_step(cur, "raw_mes: add staging columns", "IF COL_LENGTH('dbo.raw_mes', 'Operation_i') IS NULL ALTER TABLE dbo.raw_mes ADD Operation_i INT NULL;")
            _exec_step(cur, "raw_mes: add staging columns", "IF COL_LENGTH('dbo.raw_mes', 'Plant_i') IS NULL ALTER TABLE dbo.raw_mes ADD Plant_i INT NULL;")
            _exec_step(cur, "raw_mes: add staging columns", "IF COL_LENGTH('dbo.raw_mes', 'Group_i') IS NULL ALTER TABLE dbo.raw_mes ADD Group_i BIGINT NULL;")

            _exec_step(
                cur,
                "raw_mes: backfill staging columns",
                """
                UPDATE dbo.raw_mes
                SET
                    Operation_i = TRY_CONVERT(int, TRY_CONVERT(float, LTRIM(RTRIM(CAST([Operation] AS NVARCHAR(32)))))),
                    Plant_i     = TRY_CONVERT(int, TRY_CONVERT(float, LTRIM(RTRIM(CAST([Plant]     AS NVARCHAR(32)))))),
                    Group_i     = TRY_CONVERT(bigint, TRY_CONVERT(float, LTRIM(RTRIM(CAST([Group]  AS NVARCHAR(64))))))
                WHERE 1=1;
                """,
            )

            # fail-fast checks
            _exec_step(
                cur,
                "raw_mes: check Operation convertible",
                """
                IF EXISTS (
                    SELECT 1 FROM dbo.raw_mes
                    WHERE [Operation] IS NOT NULL AND LTRIM(RTRIM(CAST([Operation] AS NVARCHAR(32)))) <> ''
                      AND Operation_i IS NULL
                )
                BEGIN
                    SELECT TOP 50 id, [Operation]
                    FROM dbo.raw_mes
                    WHERE [Operation] IS NOT NULL AND LTRIM(RTRIM(CAST([Operation] AS NVARCHAR(32)))) <> ''
                      AND Operation_i IS NULL
                    ORDER BY id;
                    THROW 51000, 'raw_mes.Operation has non-numeric values; aborting', 1;
                END
                """,
            )
            _exec_step(
                cur,
                "raw_mes: check Plant convertible",
                """
                IF EXISTS (
                    SELECT 1 FROM dbo.raw_mes
                    WHERE [Plant] IS NOT NULL AND LTRIM(RTRIM(CAST([Plant] AS NVARCHAR(32)))) <> ''
                      AND Plant_i IS NULL
                )
                BEGIN
                    SELECT TOP 50 id, [Plant]
                    FROM dbo.raw_mes
                    WHERE [Plant] IS NOT NULL AND LTRIM(RTRIM(CAST([Plant] AS NVARCHAR(32)))) <> ''
                      AND Plant_i IS NULL
                    ORDER BY id;
                    THROW 51001, 'raw_mes.Plant has non-numeric values; aborting', 1;
                END
                """,
            )
            _exec_step(
                cur,
                "raw_mes: check Group convertible",
                """
                IF EXISTS (
                    SELECT 1 FROM dbo.raw_mes
                    WHERE [Group] IS NOT NULL AND LTRIM(RTRIM(CAST([Group] AS NVARCHAR(64)))) <> ''
                      AND Group_i IS NULL
                )
                BEGIN
                    SELECT TOP 50 id, [Group]
                    FROM dbo.raw_mes
                    WHERE [Group] IS NOT NULL AND LTRIM(RTRIM(CAST([Group] AS NVARCHAR(64)))) <> ''
                      AND Group_i IS NULL
                    ORDER BY id;
                    THROW 51002, 'raw_mes.Group has non-numeric values; aborting', 1;
                END
                """,
            )

            # rename only if not already migrated
            # (if already renamed once, these will fail; that's OK and will be caught)
            _exec_step(cur, "raw_mes: rename Operation -> Operation_str", "IF COL_LENGTH('dbo.raw_mes','Operation_str') IS NULL EXEC sp_rename 'dbo.raw_mes.Operation', 'Operation_str', 'COLUMN';")
            _exec_step(cur, "raw_mes: rename Plant -> Plant_str", "IF COL_LENGTH('dbo.raw_mes','Plant_str') IS NULL EXEC sp_rename 'dbo.raw_mes.Plant', 'Plant_str', 'COLUMN';")
            # NOTE: for sp_rename, avoid bracketed column name inside the string (reserved word is ok)
            _exec_step(cur, "raw_mes: rename Group -> Group_str", "IF COL_LENGTH('dbo.raw_mes','Group_str') IS NULL EXEC sp_rename 'dbo.raw_mes.Group', 'Group_str', 'COLUMN';")

            _exec_step(cur, "raw_mes: rename Operation_i -> Operation", "EXEC sp_rename 'dbo.raw_mes.Operation_i', 'Operation', 'COLUMN';")
            _exec_step(cur, "raw_mes: rename Plant_i -> Plant", "EXEC sp_rename 'dbo.raw_mes.Plant_i', 'Plant', 'COLUMN';")
            _exec_step(cur, "raw_mes: rename Group_i -> Group", "EXEC sp_rename 'dbo.raw_mes.Group_i', 'Group', 'COLUMN';")

            # -----------------------------------------------------------------
            # dbo.raw_sap_routing
            # -----------------------------------------------------------------
            _exec_step(cur, "raw_sap_routing: add staging columns", "IF COL_LENGTH('dbo.raw_sap_routing', 'Operation_i') IS NULL ALTER TABLE dbo.raw_sap_routing ADD Operation_i INT NULL;")
            _exec_step(cur, "raw_sap_routing: add staging columns", "IF COL_LENGTH('dbo.raw_sap_routing', 'Plant_i') IS NULL ALTER TABLE dbo.raw_sap_routing ADD Plant_i INT NULL;")
            _exec_step(cur, "raw_sap_routing: add staging columns", "IF COL_LENGTH('dbo.raw_sap_routing', 'Group_i') IS NULL ALTER TABLE dbo.raw_sap_routing ADD Group_i BIGINT NULL;")

            _exec_step(
                cur,
                "raw_sap_routing: backfill staging columns",
                """
                UPDATE dbo.raw_sap_routing
                SET
                    Operation_i = TRY_CONVERT(int, TRY_CONVERT(float, LTRIM(RTRIM(CAST([Operation] AS NVARCHAR(32)))))),
                    Plant_i     = TRY_CONVERT(int, TRY_CONVERT(float, LTRIM(RTRIM(CAST([Plant]     AS NVARCHAR(32)))))),
                    Group_i     = TRY_CONVERT(bigint, TRY_CONVERT(float, LTRIM(RTRIM(CAST([Group]  AS NVARCHAR(64))))))
                WHERE 1=1;
                """,
            )

            _exec_step(
                cur,
                "raw_sap_routing: check Operation convertible",
                """
                IF EXISTS (
                    SELECT 1 FROM dbo.raw_sap_routing
                    WHERE [Operation] IS NOT NULL AND LTRIM(RTRIM(CAST([Operation] AS NVARCHAR(32)))) <> ''
                      AND Operation_i IS NULL
                )
                BEGIN
                    SELECT TOP 50 id, [Operation]
                    FROM dbo.raw_sap_routing
                    WHERE [Operation] IS NOT NULL AND LTRIM(RTRIM(CAST([Operation] AS NVARCHAR(32)))) <> ''
                      AND Operation_i IS NULL
                    ORDER BY id;
                    THROW 51010, 'raw_sap_routing.Operation has non-numeric values; aborting', 1;
                END
                """,
            )

            _exec_step(cur, "raw_sap_routing: rename Operation -> Operation_str", "IF COL_LENGTH('dbo.raw_sap_routing','Operation_str') IS NULL EXEC sp_rename 'dbo.raw_sap_routing.Operation', 'Operation_str', 'COLUMN';")
            _exec_step(cur, "raw_sap_routing: rename Plant -> Plant_str", "IF COL_LENGTH('dbo.raw_sap_routing','Plant_str') IS NULL EXEC sp_rename 'dbo.raw_sap_routing.Plant', 'Plant_str', 'COLUMN';")
            _exec_step(cur, "raw_sap_routing: rename Group -> Group_str", "IF COL_LENGTH('dbo.raw_sap_routing','Group_str') IS NULL EXEC sp_rename 'dbo.raw_sap_routing.Group', 'Group_str', 'COLUMN';")

            _exec_step(cur, "raw_sap_routing: rename Operation_i -> Operation", "EXEC sp_rename 'dbo.raw_sap_routing.Operation_i', 'Operation', 'COLUMN';")
            _exec_step(cur, "raw_sap_routing: rename Plant_i -> Plant", "EXEC sp_rename 'dbo.raw_sap_routing.Plant_i', 'Plant', 'COLUMN';")
            _exec_step(cur, "raw_sap_routing: rename Group_i -> Group", "EXEC sp_rename 'dbo.raw_sap_routing.Group_i', 'Group', 'COLUMN';")

            # -----------------------------------------------------------------
            # dbo.raw_sfc
            # -----------------------------------------------------------------
            _exec_step(cur, "raw_sfc: add staging column", "IF COL_LENGTH('dbo.raw_sfc', 'Operation_i') IS NULL ALTER TABLE dbo.raw_sfc ADD Operation_i INT NULL;")
            _exec_step(
                cur,
                "raw_sfc: backfill staging column",
                """
                UPDATE dbo.raw_sfc
                SET Operation_i = TRY_CONVERT(int, TRY_CONVERT(float, LTRIM(RTRIM(CAST([Operation] AS NVARCHAR(32))))))
                WHERE 1=1;
                """,
            )
            _exec_step(
                cur,
                "raw_sfc: check Operation convertible",
                """
                IF EXISTS (
                    SELECT 1 FROM dbo.raw_sfc
                    WHERE [Operation] IS NOT NULL AND LTRIM(RTRIM(CAST([Operation] AS NVARCHAR(32)))) <> ''
                      AND Operation_i IS NULL
                )
                BEGIN
                    SELECT TOP 50 id, [Operation]
                    FROM dbo.raw_sfc
                    WHERE [Operation] IS NOT NULL AND LTRIM(RTRIM(CAST([Operation] AS NVARCHAR(32)))) <> ''
                      AND Operation_i IS NULL
                    ORDER BY id;
                    THROW 51020, 'raw_sfc.Operation has non-numeric values; aborting', 1;
                END
                """,
            )

            _exec_step(cur, "raw_sfc: rename Operation -> Operation_str", "IF COL_LENGTH('dbo.raw_sfc','Operation_str') IS NULL EXEC sp_rename 'dbo.raw_sfc.Operation', 'Operation_str', 'COLUMN';")
            _exec_step(cur, "raw_sfc: rename Operation_i -> Operation", "EXEC sp_rename 'dbo.raw_sfc.Operation_i', 'Operation', 'COLUMN';")

            cur.execute("COMMIT")
            conn.commit()

            print("\n✓ migration committed")
            _print_column_types(cur, "raw_mes", ["Operation", "Plant", "Group", "Operation_str", "Plant_str", "Group_str"]) 
            _print_column_types(cur, "raw_sap_routing", ["Operation", "Plant", "Group", "Operation_str", "Plant_str", "Group_str"]) 
            _print_column_types(cur, "raw_sfc", ["Operation", "Operation_str"]) 

            print("\nNext:")
            print("  1) rerun MES/SAP/SFC raw ETLs (so new rows use correct types)")
            print("  2) rerun snapshot refresh: scripts/_refresh_mes_metrics_materialized.py")

        except Exception as e:
            try:
                cur.execute("ROLLBACK")
                conn.rollback()
            except Exception:
                pass
            print(f"\nERROR: {type(e).__name__}: {e}")
            print("Rolled back.")
            sys.exit(1)


if __name__ == "__main__":
    main()
