"""MES WIP (CMES_WIP) ETL (SQL Server Only)

- Input: Multiple Excel files in folder (e.g. CMES_WIP_CKH_YYYYMMDD.xlsx)
- Keeps: Only latest N days (default 7) based on date in filename (last 8 digits)
- Loads into SQL Server table: dbo.raw_mes_wip_cmes
- Exports: Daily Parquet files (flat folder, no year/month subfolders). Parquet filename ends with YYYYMMDD.

Run:
  python data_pipelines/sources/mes/etl/etl_mes_wip_cmes.py
  python data_pipelines/sources/mes/etl/etl_mes_wip_cmes.py --days 7 --rebuild

"""

import argparse
import logging
import os
import re
import sys
import warnings
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SOURCE_DIR_DEFAULT = (
    r"C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\30-MES导出数据\CMES_WIP"
)

A1_OUTPUT_DIR = Path(
    r"C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\A1_ETL_Output"
)
OUTPUT_DIR_DEFAULT = A1_OUTPUT_DIR / "02_CURATED_PARTITIONED" / "cmes_wip"

TABLE_NAME = "raw_mes_wip_cmes"
ETL_NAME = "mes_wip_cmes"


def get_db_manager() -> SQLServerOnlyManager:
    return SQLServerOnlyManager(
        sql_server=os.getenv("MDDAP_SQL_SERVER", r"localhost\SQLEXPRESS"),
        sql_db=os.getenv("MDDAP_SQL_DATABASE", "mddap_v2"),
        driver=os.getenv("MDDAP_SQL_DRIVER", "ODBC Driver 17 for SQL Server"),
    )


def _parse_yyyymmdd_from_filename(file_path: str) -> Optional[str]:
    base = os.path.basename(file_path)
    m = re.search(r"(\d{8})(?=\.xlsx$)", base, flags=re.IGNORECASE)
    return m.group(1) if m else None


def _snapshot_date_from_filename(file_path: str) -> Optional[date]:
    ymd = _parse_yyyymmdd_from_filename(file_path)
    if not ymd:
        return None
    try:
        return date(int(ymd[:4]), int(ymd[4:6]), int(ymd[6:8]))
    except Exception:
        return None


def _parquet_prefix_from_filename(file_path: str) -> str:
    base = os.path.basename(file_path)
    ymd = _parse_yyyymmdd_from_filename(file_path)
    if ymd and base.lower().endswith(ymd.lower() + ".xlsx"):
        return base[: -len(ymd) - len(".xlsx")]
    return "CMES_WIP_"


def _normalize_column_name(name: str) -> str:
    s = str(name).strip().replace("\ufeff", "")
    if not s:
        return ""

    # Replace common separators with underscore
    s = re.sub(r"[\s\-\/\(\)\.]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")

    # Keep alnum and underscore only
    s = re.sub(r"[^0-9A-Za-z_]", "", s)
    if not s:
        return ""

    # TitleCase tokens (ERPCode stays ERPCode-like)
    parts = [p for p in s.split("_") if p]
    return "".join([p[:1].upper() + p[1:] for p in parts])


def _read_wip_excel(file_path: str) -> pd.DataFrame:
    xl = pd.ExcelFile(file_path)
    sheet = "Export" if "Export" in xl.sheet_names else xl.sheet_names[0]
    df = pd.read_excel(file_path, sheet_name=sheet)
    return df


def _clean_wip_df(df: pd.DataFrame, *, file_path: str) -> Tuple[pd.DataFrame, date, str]:
    if df is None or df.empty:
        raise ValueError("Empty WIP dataframe")

    snap = _snapshot_date_from_filename(file_path)
    if snap is None:
        raise ValueError(f"Cannot parse YYYYMMDD from filename: {os.path.basename(file_path)}")

    prefix = _parquet_prefix_from_filename(file_path)

    df2 = df.copy()

    # Normalize columns
    new_cols = []
    used = {}
    for c in df2.columns:
        n = _normalize_column_name(c)
        if not n:
            n = "Col"
        if n in used:
            used[n] += 1
            n = f"{n}_{used[n]}"
        else:
            used[n] = 0
        new_cols.append(n)
    df2.columns = new_cols

    # Drop fully empty rows
    df2 = df2.dropna(how="all")

    # Add snapshot_date and source_file
    # Keep as Python date for SQL Server DATE column insertion
    df2["snapshot_date"] = snap
    df2["source_file"] = os.path.basename(file_path)

    # Best-effort datetime parsing for common columns
    for c in ["TrackInDate", "TrackOutDate", "DateEnteredStep", "LastProcessedTime"]:
        if c in df2.columns:
            df2[c] = pd.to_datetime(df2[c], errors="coerce")

    # Best-effort numeric parsing
    for c in ["MaterialQty", "OrderQty", "ProductionOrder", "ERPCode"]:
        if c in df2.columns:
            df2[c] = pd.to_numeric(df2[c], errors="coerce")

    return df2, snap, prefix


def _infer_sql_type(series: pd.Series) -> str:
    if pd.api.types.is_datetime64_any_dtype(series):
        return "DATETIME2 NULL"
    if pd.api.types.is_bool_dtype(series):
        return "BIT NULL"
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT NULL"
    if pd.api.types.is_float_dtype(series):
        return "FLOAT NULL"
    return "NVARCHAR(512) NULL"


def ensure_table(db: SQLServerOnlyManager, df_sample: pd.DataFrame) -> None:
    with db.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            IF OBJECT_ID('dbo.{TABLE_NAME}', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.{TABLE_NAME} (
                    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    snapshot_date DATE NOT NULL,
                    source_file NVARCHAR(260) NULL,
                    created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
                    updated_at DATETIME2 NOT NULL DEFAULT GETDATE()
                );
            END
            """
        )
        conn.commit()

        cur.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=?",
            (TABLE_NAME,),
        )
        existing = {r[0] for r in cur.fetchall()}

        for col in df_sample.columns:
            if col in existing:
                continue
            if col in {"id"}:
                continue
            if col == "snapshot_date":
                continue
            if col == "source_file":
                continue
            sql_type = _infer_sql_type(df_sample[col])
            cur.execute(f"ALTER TABLE dbo.{TABLE_NAME} ADD [{col}] {sql_type};")

        # Ensure snapshot_date index for cleanup/query
        try:
            cur.execute(
                f"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='idx_{TABLE_NAME}_snapshot_date' AND object_id = OBJECT_ID('dbo.{TABLE_NAME}')) "
                f"CREATE INDEX idx_{TABLE_NAME}_snapshot_date ON dbo.{TABLE_NAME}(snapshot_date);"
            )
        except Exception:
            pass

        conn.commit()


def _delete_snapshot(conn, snap: date) -> int:
    cur = conn.cursor()
    cur.execute(f"DELETE FROM dbo.{TABLE_NAME} WHERE snapshot_date = ?", (snap,))
    deleted = int(cur.rowcount or 0)
    conn.commit()
    return deleted


def _delete_older_than(conn, cutoff: date) -> int:
    cur = conn.cursor()
    cur.execute(f"DELETE FROM dbo.{TABLE_NAME} WHERE snapshot_date < ?", (cutoff,))
    deleted = int(cur.rowcount or 0)
    conn.commit()
    return deleted


def _list_recent_files(source_dir: str, days: int) -> List[str]:
    p = Path(source_dir)
    files = sorted([str(x) for x in p.glob("CMES_WIP_*.xlsx")])
    dated: List[Tuple[date, str]] = []
    for f in files:
        d = _snapshot_date_from_filename(f)
        if d is None:
            continue
        dated.append((d, f))

    if not dated:
        return []

    max_d = max(d for d, _ in dated)
    cutoff = max_d - timedelta(days=max(0, days - 1))
    keep = [f for d, f in dated if d >= cutoff]
    return sorted(keep)


def export_parquet_for_dates(db: SQLServerOnlyManager, *, output_dir: Path, dates_to_export: List[date], prefix_map: dict) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    with db.get_connection() as conn:
        for d in sorted(set(dates_to_export)):
            yyyymmdd = d.strftime("%Y%m%d")
            prefix = prefix_map.get(d, "CMES_WIP_")
            dst = output_dir / f"{prefix}{yyyymmdd}.parquet"
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message=r"pandas only supports SQLAlchemy connectable.*",
                    category=UserWarning,
                )
                df = pd.read_sql_query(
                    f"SELECT * FROM dbo.{TABLE_NAME} WHERE snapshot_date = ?",
                    conn,
                    params=(d,),
                )
            if df.empty:
                logger.warning(f"No rows for snapshot_date={yyyymmdd}, skip parquet: {dst}")
                continue
            df.to_parquet(dst, index=False)
            logger.info(f"Exported parquet: {dst} ({len(df)} rows)")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", default=SOURCE_DIR_DEFAULT)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR_DEFAULT))
    parser.add_argument("--rebuild", action="store_true")
    args = parser.parse_args(argv)

    db = get_db_manager()

    all_files = _list_recent_files(args.source_dir, int(args.days))
    if not all_files:
        logger.warning(f"No CMES_WIP files found in: {args.source_dir}")
        return 0

    logger.info(f"Found {len(all_files)} files in last {args.days} days.")

    if args.rebuild:
        files_to_process = all_files
        with db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"IF OBJECT_ID('dbo.{TABLE_NAME}', 'U') IS NOT NULL DROP TABLE dbo.{TABLE_NAME};")
            conn.commit()
    else:
        # Filter to only changed files
        files_to_process = db.filter_changed_files(ETL_NAME, all_files)
        logger.info(f"Files to process (changed/new): {len(files_to_process)}")

    imported_dates: List[date] = []
    prefix_map = {}

    # Calculate cutoff based on ALL files (to maintain sliding window)
    dates_in_files = [
        _snapshot_date_from_filename(f) 
        for f in all_files 
        if _snapshot_date_from_filename(f) is not None
    ]
    
    if not dates_in_files:
        cutoff_keep = date.today()
    else:
        cutoff_keep = max(dates_in_files) - timedelta(days=max(0, int(args.days) - 1))

    for i, f in enumerate(files_to_process):
        logger.info(f"[{i+1}/{len(files_to_process)}] Import: {os.path.basename(f)}")

        try:
            df_raw = _read_wip_excel(f)
            df, snap, prefix = _clean_wip_df(df_raw, file_path=f)
        except Exception as e:
            logger.error(f"Failed to read/clean {f}: {e}")
            continue

        ensure_table(db, df)

        with db.get_connection() as conn:
            deleted = _delete_snapshot(conn, snap)
            logger.info(f"  Deleted existing rows for {snap}: {deleted}")

        inserted = db.bulk_insert(df, TABLE_NAME, if_exists="append")
        logger.info(f"  Inserted rows: {inserted}")

        try:
            db.mark_file_processed(ETL_NAME, f)
        except Exception:
            pass

        imported_dates.append(snap)
        prefix_map[snap] = prefix

    # Cleanup old snapshots in DB
    with db.get_connection() as conn:
        deleted_old = _delete_older_than(conn, cutoff_keep)
        if deleted_old:
            logger.info(f"Deleted rows older than {cutoff_keep}: {deleted_old}")

    # Export parquet for imported dates (and ensure only last N days remain in folder)
    out_dir = Path(args.output_dir)
    export_parquet_for_dates(db, output_dir=out_dir, dates_to_export=imported_dates, prefix_map=prefix_map)

    # Cleanup parquet files older than cutoff
    try:
        for p in out_dir.glob("*.parquet"):
            m = re.search(r"(\d{8})(?=\.parquet$)", p.name, flags=re.IGNORECASE)
            if not m:
                continue
            d = date(int(m.group(1)[:4]), int(m.group(1)[4:6]), int(m.group(1)[6:8]))
            if d < cutoff_keep:
                p.unlink(missing_ok=True)
    except Exception:
        pass

    logger.info("CMES WIP ETL done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
