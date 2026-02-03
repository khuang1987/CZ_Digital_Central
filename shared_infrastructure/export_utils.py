
import os
import sys
import json
import logging
import warnings
from datetime import date
from pathlib import Path
from typing import Optional, List, Tuple

import pandas as pd
import pyodbc

# Try import pyarrow
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = None
    pq = None

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ==============================================================================
# Date & Partition Helpers
# ==============================================================================

def _month_ym(d: date) -> str:
    """Return YYYY-MM string from date"""
    return d.strftime('%Y-%m')

def _add_months(d: date, months: int) -> date:
    """Add months to a date"""
    new_month = d.month - 1 + months
    year = d.year + new_month // 12
    month = new_month % 12 + 1
    day = d.day
    # handle end of month
    import calendar
    last_day = calendar.monthrange(year, month)[1]
    day = min(day, last_day)
    return date(year, month, day)

def _month_list_last_n(end_ym: str, n: int) -> List[str]:
    """Return list of YYYY-MM strings for last n months ending at end_ym"""
    try:
        y, m = map(int, end_ym.split('-'))
        curr = date(y, m, 1)
        res = []
        for i in range(n):
            d = _add_months(curr, -i)
            res.append(_month_ym(d))
        return res
    except:
        return []

def _max_month_in_table(conn, table_name: str, date_col: str) -> Optional[str]:
    """Get the latest month present in the table"""
    try:
        _, _, qualified = _qualify_table_name(table_name)
        cur = conn.cursor()
        sql = (
            f"SELECT MAX(CONVERT(char(7), TRY_CONVERT(date, [{date_col}]), 120)) "
            f"FROM {qualified}"
        )
        cur.execute(sql)
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
        return None
    except Exception:
        return None

# ==============================================================================
# Constants & Defaults
# ==============================================================================
# Helper to get default A1 output dir if not provided
def get_default_output_dir() -> Path:
    env_export_dir = os.getenv("MDDAP_EXPORT_DIR")
    if env_export_dir:
        return Path(env_export_dir)

    relative_path = r"CZ Production - 文档\General\POWER BI 数据源 V2\A1_ETL_Output"
    onedrive_root = os.getenv("MDDAP_ONEDRIVE_ROOT")
    if onedrive_root:
        candidate = Path(onedrive_root) / relative_path
        if candidate.exists():
            return candidate
            
    # Fallback
    fallback = Path(r"C:\Users\huangk14\OneDrive - Medtronic PLC") / relative_path
    if fallback.exists():
        return fallback
        
    # Default to local if all else fails (safety)
    return Path("data/output")

# Prefixes for specific datasets
PARTITIONED_EXPORT_PREFIX = {
    'mes_batch_report': 'mes_metrics',
    'sfc_wip_czm': 'raw_sfc_wip_czm',
    'sfc_repair': 'raw_sfc_repair',
    'sap_labor_hours': 'raw_sap_labor_hours',
    'sap_gi_9997': 'sap_gi_9997', # Normalized
    'planner_tasks': 'planner_tasks',
    'sfc_nc': 'raw_sfc_nc',
}

# ==============================================================================
# Schema & Parquet Helpers
# ==============================================================================

def _read_existing_schema(parquet_path: Path):
    if not pq: raise RuntimeError("pyarrow required")
    if parquet_path.exists():
        return pq.read_schema(parquet_path)
    return None

def _parquet_row_count(parquet_path: Path) -> Optional[int]:
    if not pq: return None
    if not parquet_path.exists():
        return None
    try:
        pf = pq.ParquetFile(parquet_path)
        return int(pf.metadata.num_rows) if pf.metadata is not None else None
    except Exception:
        return None

def _meta_path_for_parquet(parquet_path: Path) -> Path:
    return parquet_path.with_suffix(parquet_path.suffix + '.meta.json')

def _qualify_table_name(table_name: str, default_schema: str = 'dbo'):
    if '.' in table_name:
        schema, name = table_name.split('.', 1)
        schema = schema.strip('[]')
        name = name.strip('[]')
    else:
        schema = default_schema
        name = table_name
    qualified = f"[{schema}].[{name}]"
    return schema, name, qualified

def _sqlserver_table_to_arrow_schema(conn, table_name: str):
    if not pa: raise RuntimeError("pyarrow required")
    
    schema, name, _ = _qualify_table_name(table_name)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """,
        (schema, name),
    )
    rows = cur.fetchall()
    if not rows:
        return None

    def _map_sqlserver_type_to_arrow(sql_type: str):
        t = (sql_type or "").lower()
        if t in {"bit", "tinyint", "smallint", "int", "bigint"}:
            return pa.int64()
        if t in {"real", "float", "decimal", "numeric", "money", "smallmoney"}:
            return pa.float64()
        return pa.string()

    fields = [pa.field(r[0], _map_sqlserver_type_to_arrow(r[1])) for r in rows]
    return pa.schema(fields)

def _df_to_parquet_with_schema(df: pd.DataFrame, out_path: Path, schema) -> None:
    if not pa or not pq: raise RuntimeError("pyarrow required")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    if schema is None:
        df.to_parquet(out_path, index=False)
        return

    # Align columns
    expected_cols = [f.name for f in schema]
    df_out = df.copy()
    for col in expected_cols:
        if col not in df_out.columns:
            df_out[col] = None
    df_out = df_out[expected_cols]

    # Coerce dtypes
    for field in schema:
        col = field.name
        try:
            if pa.types.is_integer(field.type):
                df_out[col] = pd.to_numeric(df_out[col], errors='coerce').astype('Int64')
            elif pa.types.is_floating(field.type):
                df_out[col] = pd.to_numeric(df_out[col], errors='coerce')
            elif pa.types.is_string(field.type):
                df_out[col] = df_out[col].astype('string')
        except Exception:
            pass

    table = pa.Table.from_pandas(df_out, preserve_index=False)
    table = table.cast(schema, safe=False)
    pq.write_table(table, out_path)

# ==============================================================================
# Metadata & DB Helpers
# ==============================================================================

def _ensure_export_partition_meta_table(conn) -> None:
    sql = (
        "IF OBJECT_ID('dbo.export_partition_meta', 'U') IS NULL "
        "BEGIN "
        "CREATE TABLE dbo.export_partition_meta ("
        "  dataset NVARCHAR(128) NOT NULL,"
        "  table_name NVARCHAR(256) NOT NULL,"
        "  date_col NVARCHAR(128) NOT NULL,"
        "  ym CHAR(7) NOT NULL,"
        "  row_count BIGINT NULL,"
        "  record_hash_checksum BIGINT NULL,"
        "  parquet_path NVARCHAR(1024) NULL,"
        "  exported_at DATE NULL,"
        "  updated_at DATETIME2 NOT NULL CONSTRAINT DF_export_partition_meta_updated_at DEFAULT SYSUTCDATETIME(),"
        "  CONSTRAINT PK_export_partition_meta PRIMARY KEY (dataset, ym)"
        ");"
        "END"
    )
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

def _sql_partition_stats(conn, table_name: str, date_col: str, ym: str) -> Tuple[int, Optional[int]]:
    _, _, qualified = _qualify_table_name(table_name)
    
    # Check if record_hash exists
    cur = conn.cursor()
    has_hash = False
    try:
        # Simplest check
        cur.execute(f"SELECT TOP 1 record_hash FROM {qualified}")
        has_hash = True
    except:
        pass

    if has_hash:
        sql = (
            f"SELECT COUNT(1) AS cnt, "
            f"CHECKSUM_AGG(BINARY_CHECKSUM(CAST([record_hash] AS NVARCHAR(512)))) AS h "
            f"FROM {qualified} "
            f"WHERE CONVERT(char(7), TRY_CONVERT(date, [{date_col}]), 120) = ?"
        )
        cur.execute(sql, (ym,))
        row = cur.fetchone()
        cnt = int(row[0]) if row and row[0] is not None else 0
        h = int(row[1]) if row and row[1] is not None else None
        return cnt, h

    sql = (
        f"SELECT COUNT(1) AS cnt "
        f"FROM {qualified} "
        f"WHERE CONVERT(char(7), TRY_CONVERT(date, [{date_col}]), 120) = ?"
    )
    cur.execute(sql, (ym,))
    row = cur.fetchone()
    cnt = int(row[0]) if row and row[0] is not None else 0
    return cnt, None

def _write_partition_meta_to_sql(conn, meta: dict) -> None:
    try:
        _ensure_export_partition_meta_table(conn)
        cur = conn.cursor()
        cur.execute(
            "MERGE dbo.export_partition_meta AS t "
            "USING (SELECT ? AS dataset, ? AS ym) AS s "
            "ON (t.dataset = s.dataset AND t.ym = s.ym) "
            "WHEN MATCHED THEN UPDATE SET "
            "  table_name = ?, date_col = ?, row_count = ?, record_hash_checksum = ?, parquet_path = ?, exported_at = ?, updated_at = SYSUTCDATETIME() "
            "WHEN NOT MATCHED THEN INSERT (dataset, table_name, date_col, ym, row_count, record_hash_checksum, parquet_path, exported_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
            (
                meta.get('dataset'), meta.get('ym'),
                meta.get('table'), meta.get('date_col'), meta.get('row_count'), meta.get('record_hash_checksum'), meta.get('parquet_path'), meta.get('exported_at'),
                meta.get('dataset'), meta.get('table'), meta.get('date_col'), meta.get('ym'), meta.get('row_count'), meta.get('record_hash_checksum'), meta.get('parquet_path'), meta.get('exported_at'),
            ),
        )
        conn.commit()
    except Exception:
        pass

def _read_partition_meta_from_sql(conn, dataset: str, ym: str) -> Optional[dict]:
    try:
        _ensure_export_partition_meta_table(conn)
        cur = conn.cursor()
        cur.execute(
            "SELECT row_count, record_hash_checksum FROM dbo.export_partition_meta WHERE dataset = ? AND ym = ?",
            (dataset, ym),
        )
        row = cur.fetchone()
        if not row: return None
        return {'row_count': row[0], 'record_hash_checksum': row[1]}
    except:
        return None

def _sqlserver_table_exists(conn, table_name: str, schema: str = 'dbo') -> bool:
    """Check if table exists in SQL Server"""
    try:
        cur = conn.cursor()
        # Handle schema in table_name if present
        if '.' in table_name:
            sch, tbl = table_name.split('.', 1)
            sch = sch.strip('[]')
            tbl = tbl.strip('[]')
        else:
            sch = schema
            tbl = table_name.strip('[]')
            
        cur.execute("""
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """, (sch, tbl))
        return cur.fetchone() is not None
    except Exception:
        return False

# ==============================================================================
# Main Export Function
# ==============================================================================

def export_partitioned_table(
    conn,
    dataset: str,
    table_name: str,
    date_col: str,
    months: List[str],
    *,
    output_dir: Path,
    reconcile: bool = True,
    force: bool = False
) -> bool:
    """
    Exports specified months of a table to Parquet with incremental checks.
    """
    failed = False
    
    # Resolve base path
    partitioned_base = output_dir / '02_CURATED_PARTITIONED'
    
    for ym in months:
        if len(ym) != 7: continue 
        
        year = ym[:4]
        yyyymm = ym.replace('-', '')
        prefix = PARTITIONED_EXPORT_PREFIX.get(dataset, dataset)
        dst_path = partitioned_base / dataset / year / f"{prefix}_{yyyymm}.parquet"
        
        # Incremental Check
        if reconcile and not force:
            try:
                if dst_path.exists():
                    # Check DB state
                    sql_cnt, sql_h = _sql_partition_stats(conn, table_name, date_col, ym)
                    if sql_cnt == 0:
                        logger.info(f"Skip empty partition: {table_name} ({ym})")
                        continue
                        
                    # Check Metadata (Fastest)
                    meta = _read_partition_meta_from_sql(conn, dataset, ym)
                    if meta:
                        if meta['row_count'] == sql_cnt and (sql_h is None or meta['record_hash_checksum'] == sql_h):
                             logger.info(f"Skip up-to-date: {table_name} ({ym})")
                             continue
                    
                    # Fallback: Check Parquet File (Slower but reliable if meta missing)
                    pq_cnt = _parquet_row_count(dst_path)
                    if pq_cnt == sql_cnt:
                        # Assuming count match is enough if hash missing
                        logger.info(f"Skip up-to-date (count match): {table_name} ({ym})")
                        continue
            except Exception as e:
                logger.warning(f"Reconcile check failed for {ym}, forcing export: {e}")

        # Perform Export
        try:
            _, _, qualified = _qualify_table_name(table_name)
            df = pd.read_sql_query(
                f"SELECT * FROM {qualified} WHERE CONVERT(char(7), TRY_CONVERT(date, [{date_col}]), 120) = ?",
                conn,
                params=(ym,),
            )
            
            if df.empty:
                logger.info(f"No data found for {ym}, skipping.")
                continue

            # Schema Handling
            schema = _read_existing_schema(dst_path)
            if not schema:
                schema = _sqlserver_table_to_arrow_schema(conn, table_name)
                
            _df_to_parquet_with_schema(df, dst_path, schema)
            
            # Update Meta
            if reconcile:
                sql_cnt, sql_h = _sql_partition_stats(conn, table_name, date_col, ym)
                _write_partition_meta_to_sql(conn, {
                    'dataset': dataset,
                    'table': table_name,
                    'date_col': date_col,
                    'ym': ym,
                    'row_count': sql_cnt,
                    'record_hash_checksum': sql_h,
                    'parquet_path': str(dst_path),
                    'exported_at': date.today().isoformat()
                })
            
            logger.info(f"Exported {dataset} ({ym}) -> {dst_path.name} ({len(df)} rows)")
            
        except Exception as e:
            logger.error(f"Failed to export {dataset} ({ym}): {e}")
            failed = True

    return failed
