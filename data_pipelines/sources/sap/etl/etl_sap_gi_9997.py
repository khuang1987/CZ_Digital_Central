import argparse
import hashlib
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]
project_root_str = str(PROJECT_ROOT)
if project_root_str not in sys.path:
    sys.path.insert(0, project_root_str)

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DEFAULT_SOURCE_FILE = r"C:\Users\huangk14\OneDrive - Medtronic PLC\General - CZ OPS生产每日产出登记\KH SA统计\9997发料记录.XLSX"

TABLE_NAME = "raw_sap_gi_9997"
DATASET_NAME = "sap_gi_9997"


def get_db_manager() -> SQLServerOnlyManager:
    return SQLServerOnlyManager(
        sql_server=os.getenv('MDDAP_SQL_SERVER', r'localhost\SQLEXPRESS'),
        sql_db=os.getenv('MDDAP_SQL_DATABASE', 'mddap_v2'),
        driver=os.getenv('MDDAP_SQL_DRIVER', 'ODBC Driver 17 for SQL Server'),
    )


def _normalize_column_name(name: str) -> str:
    s = str(name).strip()
    s = s.replace('\ufeff', '')
    s = s.replace('\n', ' ').replace('\r', ' ')
    s = ' '.join(s.split())

    # Best-effort mapping to English-like keys for common SAP fields
    lowered = s.lower()
    mapping = {
        'posting date': 'PostingDate',
        'postingdate': 'PostingDate',
        '过账日期': 'PostingDate',
        '凭证日期': 'DocumentDate',
        'document date': 'DocumentDate',
        'material': 'Material',
        '物料': 'Material',
        'material description': 'MaterialDesc',
        '物料描述': 'MaterialDesc',
        'plant': 'Plant',
        '工厂': 'Plant',
        'storage location': 'StorageLocation',
        '库存地点': 'StorageLocation',
        'movement type': 'MovementType',
        '移动类型': 'MovementType',
        'quantity': 'Quantity',
        '数量': 'Quantity',
        'unit': 'Unit',
        '单位': 'Unit',
        'document number': 'DocumentNumber',
        '物料凭证': 'DocumentNumber',
        'document item': 'DocumentItem',
        'item': 'DocumentItem',
        '批次': 'Batch',
        'batch': 'Batch',
        'order': 'OrderNumber',
        'order number': 'OrderNumber',
        '订单': 'OrderNumber',
        'cost center': 'CostCenter',
        '成本中心': 'CostCenter',
    }
    if lowered in mapping:
        return mapping[lowered]

    # Keep safe ASCII-ish token; if mostly non-ascii, fallback to a generic name later
    out = []
    for ch in s:
        if ch.isalnum():
            out.append(ch)
        elif ch in {' ', '-', '_', '/', '.', '(', ')'}:
            out.append('_')
    s2 = ''.join(out)
    while '__' in s2:
        s2 = s2.replace('__', '_')
    s2 = s2.strip('_')

    if not s2:
        return ''

    # TitleCase-ish
    parts = [p for p in s2.split('_') if p]
    s3 = ''.join([p[:1].upper() + p[1:] for p in parts])
    return s3


def _detect_header_and_read(file_path: str, sheet: Optional[str]) -> Tuple[pd.DataFrame, str]:
    xl = pd.ExcelFile(file_path)
    sheet_name = sheet or (xl.sheet_names[0] if xl.sheet_names else None)
    if sheet_name is not None and xl.sheet_names and sheet_name not in xl.sheet_names:
        logger.warning(
            f"Sheet '{sheet_name}' not found. Available: {xl.sheet_names}. Fallback to first sheet."
        )
        sheet_name = xl.sheet_names[0]
    if sheet_name is None:
        raise ValueError("No sheet found in Excel")

    # Try normal header
    df_try = pd.read_excel(file_path, sheet_name=sheet_name, header=0, usecols="A:T")
    unnamed = [c for c in df_try.columns if str(c).startswith('Unnamed')]
    if len(df_try.columns) > 0 and (len(unnamed) / max(1, len(df_try.columns))) < 0.6:
        return df_try, sheet_name

    # Fallback: find best header row within first 30 rows
    df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None, usecols="A:T")
    best_idx = 0
    best_score = -1
    max_scan = min(30, len(df_raw))
    for i in range(max_scan):
        row = df_raw.iloc[i]
        non_null = int(row.notna().sum())
        non_unnamed = int(sum(1 for v in row.tolist() if isinstance(v, str) and v.strip() and not v.strip().lower().startswith('unnamed')))
        score = non_null * 2 + non_unnamed
        if score > best_score:
            best_score = score
            best_idx = i

    df = pd.read_excel(file_path, sheet_name=sheet_name, header=best_idx, usecols="A:T")
    return df, sheet_name


def _coerce_posting_date(df: pd.DataFrame) -> pd.Series:
    candidates = [
        'PostingDate',
        'Posting Date',
        '过账日期',
        'DocumentDate',
        'Document Date',
        '凭证日期',
        'Date',
        '日期',
    ]

    for c in candidates:
        if c in df.columns:
            s = pd.to_datetime(df[c], errors='coerce')
            if s.notna().any():
                return s.dt.strftime('%Y-%m-%d')

    return pd.Series([None] * len(df))


def clean_data(df: pd.DataFrame, *, source_file: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    # Normalize column names
    normalized: List[str] = []
    used: Dict[str, int] = {}
    for c in df.columns:
        name = _normalize_column_name(c)
        if not name:
            name = 'Col'
        if name in used:
            used[name] += 1
            name = f"{name}_{used[name]}"
        else:
            used[name] = 0
        normalized.append(name)

    df = df.copy()
    df.columns = normalized

    # Drop fully empty rows
    df = df.dropna(how='all')

    # Ensure PostingDate exists
    if 'PostingDate' not in df.columns:
        df['PostingDate'] = _coerce_posting_date(df)
    else:
        df['PostingDate'] = pd.to_datetime(df['PostingDate'], errors='coerce').dt.strftime('%Y-%m-%d')

    # Coerce Quantity if exists
    if 'Quantity' in df.columns:
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')

    # Standardize common keys as strings
    for col in ['Material', 'Plant', 'StorageLocation', 'MovementType', 'DocumentNumber', 'DocumentItem', 'Batch', 'OrderNumber']:
        if col in df.columns:
            df[col] = df[col].astype('string').str.strip()

    df['source_file'] = os.path.basename(source_file)

    # Ensure audit timestamps exist for staging-table insert.
    # NOTE: merge_insert_by_hash creates staging table via SELECT INTO, which does not keep DEFAULT constraints.
    # If created_at/updated_at are NOT NULL in the base table, the staging table will also be NOT NULL but without defaults.
    # So we must provide these columns explicitly.
    now_utc = datetime.utcnow()
    df['created_at'] = now_utc
    df['updated_at'] = now_utc

    # record_hash based on all business columns (excluding audit)
    audit_cols = {'id', 'created_at', 'updated_at'}
    base_cols = [c for c in df.columns if c not in audit_cols]

    def _row_hash(row) -> str:
        vals = []
        for c in base_cols:
            v = row.get(c)
            if v is None or (isinstance(v, float) and pd.isna(v)):
                vals.append('')
            else:
                vals.append(str(v))
        raw = '|'.join(vals)
        return hashlib.md5(raw.encode('utf-8')).hexdigest()

    df['record_hash'] = df.apply(_row_hash, axis=1)

    before = len(df)
    df = df.drop_duplicates(subset=['record_hash'], keep='last')
    after = len(df)
    if before > after:
        logger.info(f"Internal dedup: removed {before - after} duplicate rows")

    return df


def create_table(db: SQLServerOnlyManager, df_sample: pd.DataFrame) -> None:
    with db.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            IF OBJECT_ID('dbo.{TABLE_NAME}', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.{TABLE_NAME} (
                    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    PostingDate NVARCHAR(30) NULL,
                    source_file NVARCHAR(260) NULL,
                    record_hash NVARCHAR(64) NULL,
                    created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
                    updated_at DATETIME2 NOT NULL DEFAULT GETDATE()
                );
            END
            """
        )
        conn.commit()

        cur.execute(
            "SELECT COLUMN_NAME, DATA_TYPE "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=?",
            (TABLE_NAME,),
        )
        existing = {r[0] for r in cur.fetchall()}

        def _infer_sql_type(series: pd.Series) -> str:
            if pd.api.types.is_numeric_dtype(series):
                return 'FLOAT NULL'
            return 'NVARCHAR(255) NULL'

        # Add any new columns from input df
        for col in df_sample.columns:
            if col in existing:
                continue
            if col in {'id'}:
                continue
            sql_type = _infer_sql_type(df_sample[col])
            cur.execute(f"ALTER TABLE dbo.{TABLE_NAME} ADD [{col}] {sql_type};")

        conn.commit()


def _table_row_count(db: SQLServerOnlyManager) -> int:
    try:
        with db.get_connection() as conn:
            cur = conn.cursor()
            row = cur.execute(f"SELECT COUNT(1) FROM dbo.{TABLE_NAME}").fetchone()
            return int(row[0] or 0) if row else 0
    except Exception:
        return 0


def import_file(db: SQLServerOnlyManager, *, file_path: str, sheet: Optional[str], rebuild: bool) -> int:
    if not os.path.exists(file_path):
        logger.warning(f"跳过任务: 源文件未找到 -> {file_path}")
        return 0

    if rebuild:
        logger.info(f"Rebuild mode: truncating table {TABLE_NAME}")
        with db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"IF OBJECT_ID('dbo.{TABLE_NAME}', 'U') IS NOT NULL TRUNCATE TABLE dbo.{TABLE_NAME};")
            conn.commit()

    raw_df, sheet_name = _detect_header_and_read(file_path, sheet)
    logger.info(f"Loaded Excel: {os.path.basename(file_path)} (sheet={sheet_name}, rows={len(raw_df)})")

    df = clean_data(raw_df, source_file=file_path)
    if df.empty:
        logger.info("No rows after cleaning")
        return 0

    create_table(db, df)

    before_cnt = _table_row_count(db)
    inserted = db.merge_insert_by_hash(df, TABLE_NAME, hash_column='record_hash')
    after_cnt = _table_row_count(db)
    logger.info(
        f"Imported {os.path.basename(file_path)}: inserted={int(inserted or 0)}, before={before_cnt}, after={after_cnt}"
    )

    if after_cnt == 0:
        raise RuntimeError(
            "9997 GI import produced 0 rows in dbo.raw_sap_gi_9997. "
            "Please check Excel header/columns and cleaning logic."
        )

    try:
        db.mark_file_processed(DATASET_NAME, file_path)
    except Exception:
        pass

    return int(inserted or 0)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', default=DEFAULT_SOURCE_FILE)
    parser.add_argument('--sheet', default='Sheet1')
    parser.add_argument('--rebuild', action='store_true')
    parser.add_argument('--force', action='store_true', help='Force refresh even if file is unchanged')
    args = parser.parse_args(argv)

    db = get_db_manager()

    if not args.force and not args.rebuild:
        try:
            changed = db.filter_changed_files(DATASET_NAME, [args.file])
        except Exception:
            changed = [args.file]
    else:
        changed = [args.file]
        logger.info(f"Force/Rebuild mode: processing {args.file}")

    if not args.rebuild and not changed and not args.force:
        existing_cnt = _table_row_count(db)
        if existing_cnt == 0:
            logger.warning("Source file unchanged but target table is empty; force re-import")
            changed = [args.file]
        else:
            logger.info("Source file unchanged, skip")
            return 0

    start = datetime.now()
    imported = import_file(db, file_path=args.file, sheet=args.sheet, rebuild=args.rebuild)
    elapsed = (datetime.now() - start).total_seconds()
    logger.info(f"Done: imported={imported}, elapsed={elapsed:.1f}s")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
