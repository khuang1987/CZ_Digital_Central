import argparse
import os
import sys
from datetime import date
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager


def _get_db() -> SQLServerOnlyManager:
    return SQLServerOnlyManager(
        sql_server=os.getenv("MDDAP_SQL_SERVER", r"localhost\\SQLEXPRESS"),
        sql_db=os.getenv("MDDAP_SQL_DATABASE", "mddap_v2"),
        driver=os.getenv("MDDAP_SQL_DRIVER", "ODBC Driver 17 for SQL Server"),
    )


def _ensure_meta_tables(conn) -> None:
    cur = conn.cursor()

    cur.execute(
        """
        IF OBJECT_ID('dbo.meta_table_stats_daily', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.meta_table_stats_daily (
                snapshot_date DATE NOT NULL,
                table_schema NVARCHAR(64) NOT NULL,
                table_name NVARCHAR(256) NOT NULL,
                row_count BIGINT NULL,
                today_inserted BIGINT NULL,
                today_updated BIGINT NULL,
                last_updated_at DATETIME2 NULL,
                computed_at DATETIME2 NOT NULL CONSTRAINT DF_meta_table_stats_daily_computed_at DEFAULT SYSUTCDATETIME(),
                CONSTRAINT PK_meta_table_stats_daily PRIMARY KEY (snapshot_date, table_schema, table_name)
            );

            CREATE INDEX IX_meta_table_stats_daily_table ON dbo.meta_table_stats_daily(table_schema, table_name);
            CREATE INDEX IX_meta_table_stats_daily_snapshot ON dbo.meta_table_stats_daily(snapshot_date);
        END
        """
    )

    cur.execute(
        """
        IF OBJECT_ID('dbo.meta_table_dq_daily', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.meta_table_dq_daily (
                snapshot_date DATE NOT NULL,
                table_schema NVARCHAR(64) NOT NULL,
                table_name NVARCHAR(256) NOT NULL,
                dq_score FLOAT NULL,
                null_rate_max FLOAT NULL,
                null_rate_col NVARCHAR(256) NULL,
                dup_rate FLOAT NULL,
                freshness_hours FLOAT NULL,
                column_count INT NULL,
                schema_changed BIT NULL,
                issues_summary NVARCHAR(2000) NULL,
                computed_at DATETIME2 NOT NULL CONSTRAINT DF_meta_table_dq_daily_computed_at DEFAULT SYSUTCDATETIME(),
                CONSTRAINT PK_meta_table_dq_daily PRIMARY KEY (snapshot_date, table_schema, table_name)
            );

            CREATE INDEX IX_meta_table_dq_daily_table ON dbo.meta_table_dq_daily(table_schema, table_name);
            CREATE INDEX IX_meta_table_dq_daily_snapshot ON dbo.meta_table_dq_daily(snapshot_date);
        END
        """
    )

    conn.commit()


def _get_candidate_tables(conn):
    sql = """
    SELECT DISTINCT
        c.TABLE_SCHEMA,
        c.TABLE_NAME
    FROM INFORMATION_SCHEMA.COLUMNS c
    JOIN INFORMATION_SCHEMA.TABLES t
      ON c.TABLE_SCHEMA = t.TABLE_SCHEMA
     AND c.TABLE_NAME = t.TABLE_NAME
    WHERE c.TABLE_SCHEMA = 'dbo'
      AND t.TABLE_TYPE = 'BASE TABLE'
      AND c.COLUMN_NAME IN ('created_at', 'updated_at')
    ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME
    """
    df = pd.read_sql_query(sql, conn)
    return [(r["TABLE_SCHEMA"], r["TABLE_NAME"]) for _, r in df.iterrows()]


def _get_table_row_counts(conn, tables):
    if not tables:
        return {}

    names = [t[1] for t in tables]
    placeholders = ",".join(["?"] * len(names))
    sql = f"""
    SELECT
        t.name AS table_name,
        SUM(p.rows) AS row_count
    FROM sys.tables t
    JOIN sys.partitions p
      ON p.object_id = t.object_id
     AND p.index_id IN (0, 1)
    WHERE SCHEMA_NAME(t.schema_id) = 'dbo'
      AND t.name IN ({placeholders})
    GROUP BY t.name
    """

    cur = conn.cursor()
    cur.execute(sql, tuple(names))
    rows = cur.fetchall()
    return {str(r[0]): int(r[1] or 0) for r in rows}


def _safe_ident(name: str) -> str:
    return "[" + str(name).replace("]", "]]" ) + "]"


def _has_column(conn, table: str, col: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=? AND COLUMN_NAME=?",
        (table, col),
    )
    return cur.fetchone() is not None


def _get_columns_for_dq(conn, table: str, max_cols: int):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT TOP (?)
            COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA='dbo'
          AND TABLE_NAME=?
          AND COLUMN_NAME NOT IN ('created_at','updated_at')
        ORDER BY ORDINAL_POSITION
        """,
        (int(max_cols), table),
    )
    cols = [str(r[0]) for r in cur.fetchall()]

    cols_filtered = []
    for c in cols:
        if c.lower() in {"id"}:
            continue
        cols_filtered.append(c)
    return cols_filtered


def _calc_stats_for_table(conn, snapshot: date, schema: str, table: str, lookback_days: int, row_count_map: dict):
    created_exists = _has_column(conn, table, "created_at")
    updated_exists = _has_column(conn, table, "updated_at")

    row_count = row_count_map.get(table)

    filter_col = "updated_at" if updated_exists else "created_at"

    today_inserted = None
    today_updated = None
    last_updated_at = None

    cur = conn.cursor()

    if created_exists:
        cur.execute(
            f"SELECT COUNT_BIG(*) FROM dbo.{_safe_ident(table)} WHERE TRY_CONVERT(date, { _safe_ident('created_at') }) = ?",
            (snapshot,),
        )
        today_inserted = int(cur.fetchone()[0] or 0)

    if updated_exists:
        cur.execute(
            f"SELECT COUNT_BIG(*) FROM dbo.{_safe_ident(table)} WHERE TRY_CONVERT(date, { _safe_ident('updated_at') }) = ?",
            (snapshot,),
        )
        today_updated = int(cur.fetchone()[0] or 0)

    cur.execute(
        f"SELECT MAX(TRY_CONVERT(datetime2, { _safe_ident(filter_col) })) FROM dbo.{_safe_ident(table)}",
    )
    last_updated_at = cur.fetchone()[0]

    return {
        "snapshot_date": snapshot,
        "table_schema": schema,
        "table_name": table,
        "row_count": row_count,
        "today_inserted": today_inserted,
        "today_updated": today_updated,
        "last_updated_at": last_updated_at,
    }


def _calc_dq_for_table(conn, snapshot: date, schema: str, table: str, lookback_days: int, dq_cols_max: int):
    updated_exists = _has_column(conn, table, "updated_at")
    created_exists = _has_column(conn, table, "created_at")

    date_col = "updated_at" if updated_exists else ("created_at" if created_exists else None)

    cur = conn.cursor()

    cur.execute(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=?",
        (table,),
    )
    column_count = int(cur.fetchone()[0] or 0)

    prev_col_count = None
    cur.execute(
        """
        SELECT TOP 1 column_count
        FROM dbo.meta_table_dq_daily
        WHERE table_schema=? AND table_name=? AND snapshot_date < ?
        ORDER BY snapshot_date DESC
        """,
        (schema, table, snapshot),
    )
    row = cur.fetchone()
    if row is not None and row[0] is not None:
        prev_col_count = int(row[0])

    schema_changed = None
    if prev_col_count is not None:
        schema_changed = 1 if prev_col_count != column_count else 0

    freshness_hours = None
    if date_col is not None:
        cur.execute(
            f"SELECT MAX(TRY_CONVERT(datetime2, { _safe_ident(date_col) })) FROM dbo.{_safe_ident(table)}"
        )
        max_dt = cur.fetchone()[0]
        if max_dt is not None:
            cur.execute("SELECT DATEDIFF(second, ?, SYSUTCDATETIME()) / 3600.0", (max_dt,))
            freshness_hours = float(cur.fetchone()[0] or 0)

    cols = _get_columns_for_dq(conn, table, dq_cols_max)

    null_rate_max = None
    null_rate_col = None

    where_clause = ""
    if date_col is not None and lookback_days is not None and int(lookback_days) > 0:
        where_clause = f"WHERE TRY_CONVERT(date, { _safe_ident(date_col) }) >= DATEADD(day, -?, ?)"

    for c in cols:
        sql = (
            f"SELECT COUNT_BIG(*) AS total, "
            f"SUM(CASE WHEN { _safe_ident(c) } IS NULL THEN 1 ELSE 0 END) AS nulls "
            f"FROM dbo.{_safe_ident(table)} {where_clause}"
        )
        params = ()
        if where_clause:
            params = (int(lookback_days), snapshot)
        cur.execute(sql, params)
        total, nulls = cur.fetchone()
        total = int(total or 0)
        nulls = int(nulls or 0)
        if total <= 0:
            continue
        rate = float(nulls) / float(total)
        if null_rate_max is None or rate > null_rate_max:
            null_rate_max = rate
            null_rate_col = c

    dup_rate = None
    if _has_column(conn, table, "id"):
        sql = f"SELECT COUNT_BIG(*) AS total, COUNT_BIG(DISTINCT { _safe_ident('id') }) AS distinct_cnt FROM dbo.{_safe_ident(table)} {where_clause}"
        params = ()
        if where_clause:
            params = (int(lookback_days), snapshot)
        cur.execute(sql, params)
        total, distinct_cnt = cur.fetchone()
        total = int(total or 0)
        distinct_cnt = int(distinct_cnt or 0)
        if total > 0:
            dup_rate = max(0.0, 1.0 - (float(distinct_cnt) / float(total)))

    dq_score = 100.0
    issues = []

    if null_rate_max is not None:
        dq_score -= min(60.0, null_rate_max * 60.0)
        if null_rate_max >= 0.05:
            issues.append(f"NULL_HIGH:{null_rate_col}={null_rate_max:.1%}")

    if dup_rate is not None:
        dq_score -= min(40.0, dup_rate * 40.0)
        if dup_rate >= 0.001:
            issues.append(f"DUP_ID={dup_rate:.1%}")

    if freshness_hours is not None:
        if freshness_hours >= 24:
            penalty = min(30.0, (freshness_hours - 24.0) * 0.5)
            dq_score -= penalty
            issues.append(f"STALE={freshness_hours:.1f}h")

    if schema_changed == 1:
        dq_score -= 10.0
        issues.append("SCHEMA_CHANGED")

    dq_score = float(max(0.0, min(100.0, dq_score)))

    issues_summary = "; ".join(issues) if issues else "OK"

    return {
        "snapshot_date": snapshot,
        "table_schema": schema,
        "table_name": table,
        "dq_score": dq_score,
        "null_rate_max": null_rate_max,
        "null_rate_col": null_rate_col,
        "dup_rate": dup_rate,
        "freshness_hours": freshness_hours,
        "column_count": column_count,
        "schema_changed": schema_changed,
        "issues_summary": issues_summary,
    }


def run(snapshot: date, lookback_days: int, dq_cols_max: int) -> None:
    db = _get_db()
    with db.get_connection() as conn:
        _ensure_meta_tables(conn)

        tables = _get_candidate_tables(conn)

        row_count_map = _get_table_row_counts(conn, tables)

        stats_rows = []
        dq_rows = []

        for schema, table in tables:
            try:
                stats_rows.append(
                    _calc_stats_for_table(conn, snapshot, schema, table, lookback_days, row_count_map)
                )
                dq_rows.append(_calc_dq_for_table(conn, snapshot, schema, table, lookback_days, dq_cols_max))
            except Exception as e:
                stats_rows.append(
                    {
                        "snapshot_date": snapshot,
                        "table_schema": schema,
                        "table_name": table,
                        "row_count": row_count_map.get(table),
                        "today_inserted": None,
                        "today_updated": None,
                        "last_updated_at": None,
                    }
                )
                dq_rows.append(
                    {
                        "snapshot_date": snapshot,
                        "table_schema": schema,
                        "table_name": table,
                        "dq_score": 0.0,
                        "null_rate_max": None,
                        "null_rate_col": None,
                        "dup_rate": None,
                        "freshness_hours": None,
                        "column_count": None,
                        "schema_changed": None,
                        "issues_summary": f"ERROR:{str(e)[:180]}",
                    }
                )

        cur = conn.cursor()
        cur.execute(
            "DELETE FROM dbo.meta_table_stats_daily WHERE snapshot_date = ? AND table_schema = 'dbo'",
            (snapshot,),
        )
        cur.execute(
            "DELETE FROM dbo.meta_table_dq_daily WHERE snapshot_date = ? AND table_schema = 'dbo'",
            (snapshot,),
        )
        conn.commit()

        if stats_rows:
            df_stats = pd.DataFrame(stats_rows)
            # Dedup to prevent PK violation
            df_stats.drop_duplicates(subset=["snapshot_date", "table_schema", "table_name"], inplace=True)
            df_stats["computed_at"] = pd.Timestamp.utcnow()
            db.bulk_insert(df_stats, "meta_table_stats_daily", if_exists="append")

        if dq_rows:
            df_dq = pd.DataFrame(dq_rows)
            df_dq.drop_duplicates(subset=["snapshot_date", "table_schema", "table_name"], inplace=True)
            df_dq["computed_at"] = pd.Timestamp.utcnow()
            db.bulk_insert(df_dq, "meta_table_dq_daily", if_exists="append")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default="", help="YYYY-MM-DD. Defaults to today.")
    parser.add_argument("--lookback-days", type=int, default=7)
    parser.add_argument("--dq-cols-max", type=int, default=10)
    args = parser.parse_args()

    if args.snapshot_date:
        snapshot = date.fromisoformat(args.snapshot_date)
    else:
        snapshot = date.today()

    run(snapshot=snapshot, lookback_days=int(args.lookback_days), dq_cols_max=int(args.dq_cols_max))


if __name__ == "__main__":
    main()
