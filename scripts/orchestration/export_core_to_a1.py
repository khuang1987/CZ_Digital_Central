import argparse
import json
import logging
import shutil
import sys
from datetime import date
from pathlib import Path
from typing import Optional, List, Tuple

import pandas as pd
import pyodbc

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


A1_OUTPUT_DIR = Path(r"C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\A1_ETL_Output")


STATIC_OUTPUT_DIR = A1_OUTPUT_DIR / '01_CURATED_STATIC'
PARTITIONED_OUTPUT_DIR = A1_OUTPUT_DIR / '02_CURATED_PARTITIONED'


PARTITIONED_EXPORTS = {
    'sfc_wip_czm': ('raw_sfc_wip_czm', 'snapshot_date'),
    'sfc_repair': ('raw_sfc_repair', 'actual_order_date'),
    'sap_labor_hours': ('raw_sap_labor_hours', 'PostingDate'),
    'sap_gi_9997': ('raw_sap_gi_9997', 'PostingDate'),
    'sfc_batch_report': ('raw_sfc', 'TrackOutTime'),
    # V2: MES 只采集 raw_*，指标由 SQLite view v_mes_metrics 计算
    'mes_batch_report': ('v_mes_metrics', 'TrackOutDate'),
    'sfc_product_inspection': ('raw_sfc_inspection', 'ReportDate'),
    'planner_tasks': ('planner_tasks', 'CreatedDate'),
    'sfc_nc': ('raw_sfc_nc', 'record_time'),
}


PARTITIONED_EXPORT_PREFIX = {
    'mes_batch_report': 'mes_metrics',
}


SQLSERVER_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost\SQLEXPRESS;"
    "DATABASE=mddap_v2;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
)


STATIC_EXPORTS = {
    'sap_routing.parquet': 'raw_sap_routing',
    'planner_task_labels.parquet': 'planner_task_labels',
    'dim_operation_mapping.parquet': 'dim_operation_mapping',
}


EXPORTS = {
    # output parquet filename -> sqlite table
    'SAP_Routing_latest.parquet': 'raw_sap_routing',
    'SFC_batch_report_latest.parquet': 'raw_sfc',
    'MES_batch_report_latest.parquet': 'mes_batch_report_latest',
    'SFC_Product_Inspection_latest.parquet': 'raw_sfc_inspection',
    'Planner_tasks_latest.parquet': 'planner_tasks',
    'Planner_task_labels_latest.parquet': 'planner_task_labels',
    '03_METADATA/core_tables/TriggerCaseRegistry.parquet': 'TriggerCaseRegistry',
    '03_METADATA/core_tables/KPI_Data.parquet': 'KPI_Data',
    '03_METADATA/core_tables/KPI_Definition.parquet': 'KPI_Definition',
    '03_METADATA/core_tables/planned_labor_hours.parquet': 'planned_labor_hours',
    '03_METADATA/core_tables/dim_area_mapping.parquet': 'dim_area_mapping',
    '03_METADATA/core_tables/dim_calendar.parquet': 'dim_calendar',
    '03_METADATA/core_tables/dim_operation_cleaning_rule.parquet': 'dim_operation_cleaning_rule',
    '03_METADATA/core_tables/dim_operation_mapping.parquet': 'dim_operation_mapping',
    '03_METADATA/core_tables/dim_workcenter_mapping.parquet': 'dim_workcenter_mapping',
}


SCHEMA_FALLBACK_PARQUETS = {
    'SAP_Routing_latest.parquet': PROJECT_ROOT / 'data_pipelines' / 'sources' / 'sap' / 'publish' / 'SAP_Routing_latest.parquet',
    'SFC_batch_report_latest.parquet': PROJECT_ROOT / 'data_pipelines' / 'sources' / 'sfc' / 'publish' / 'SFC_batch_report_latest.parquet',
    'MES_batch_report_latest.parquet': PROJECT_ROOT / 'data_pipelines' / 'sources' / 'mes' / 'publish' / 'MES_batch_report_latest.parquet',
    'SFC_Product_Inspection_latest.parquet': PROJECT_ROOT / 'data_pipelines' / 'sources' / 'sfc' / 'publish' / 'SFC_Product_Inspection_latest.parquet',
    'Planner_tasks_latest.parquet': PROJECT_ROOT / 'data_pipelines' / 'sources' / 'planner' / 'publish' / 'Planner_tasks_latest.parquet',
    'Planner_task_labels_latest.parquet': PROJECT_ROOT / 'data_pipelines' / 'sources' / 'planner' / 'publish' / 'Planner_task_labels_latest.parquet',
}


def _read_existing_schema(parquet_path: Path, fallback_path: Optional[Path] = None):
    try:
        import pyarrow.parquet as pq
    except Exception as e:
        raise RuntimeError(f"pyarrow is required for parquet export: {e}")

    if parquet_path.exists():
        return pq.read_schema(parquet_path)
    if fallback_path is not None and fallback_path.exists():
        return pq.read_schema(fallback_path)
    return None


def _parquet_row_count(parquet_path: Path) -> Optional[int]:
    try:
        import pyarrow.parquet as pq
    except Exception:
        return None

    if not parquet_path.exists():
        return None

    try:
        pf = pq.ParquetFile(parquet_path)
        return int(pf.metadata.num_rows) if pf.metadata is not None else None
    except Exception:
        return None


def _meta_path_for_parquet(parquet_path: Path) -> Path:
    return parquet_path.with_suffix(parquet_path.suffix + '.meta.json')


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


def _read_partition_meta_from_file(parquet_path: Path) -> Optional[dict]:
    meta_path = _meta_path_for_parquet(parquet_path)
    if not meta_path.exists():
        return None
    try:
        return json.loads(meta_path.read_text(encoding='utf-8'))
    except Exception:
        return None


def _read_partition_meta_from_sql(conn, *, dataset: str, ym: str) -> Optional[dict]:
    try:
        _ensure_export_partition_meta_table(conn)
        cur = conn.cursor()
        cur.execute(
            "SELECT dataset, table_name, date_col, ym, row_count, record_hash_checksum, parquet_path, exported_at "
            "FROM dbo.export_partition_meta WHERE dataset = ? AND ym = ?",
            (dataset, ym),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            'dataset': row[0],
            'table': row[1],
            'date_col': row[2],
            'ym': row[3],
            'row_count': int(row[4]) if row[4] is not None else None,
            'record_hash_checksum': int(row[5]) if row[5] is not None else None,
            'parquet_path': row[6],
            'exported_at': row[7].isoformat() if row[7] is not None else None,
        }
    except Exception:
        return None


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
                meta.get('dataset'),
                meta.get('ym'),
                meta.get('table'),
                meta.get('date_col'),
                meta.get('row_count'),
                meta.get('record_hash_checksum'),
                meta.get('parquet_path'),
                meta.get('exported_at'),
                meta.get('dataset'),
                meta.get('table'),
                meta.get('date_col'),
                meta.get('ym'),
                meta.get('row_count'),
                meta.get('record_hash_checksum'),
                meta.get('parquet_path'),
                meta.get('exported_at'),
            ),
        )
        conn.commit()
    except Exception:
        pass


def _read_partition_meta(conn, parquet_path: Path, *, dataset: str, ym: str, meta_store: str) -> Optional[dict]:
    if meta_store in {'sql', 'both'}:
        meta = _read_partition_meta_from_sql(conn, dataset=dataset, ym=ym)
        if meta is not None:
            return meta

    # Backward compatible fallback: read legacy meta.json once, then upsert into SQL
    legacy = _read_partition_meta_from_file(parquet_path)
    if legacy is not None and meta_store in {'sql', 'both'}:
        legacy = dict(legacy)
        legacy.setdefault('dataset', dataset)
        legacy.setdefault('ym', ym)
        legacy.setdefault('parquet_path', str(parquet_path))
        _write_partition_meta_to_sql(conn, legacy)
    return legacy


def _write_partition_meta(conn, parquet_path: Path, meta: dict, *, meta_store: str) -> None:
    if meta_store in {'sql', 'both'}:
        _write_partition_meta_to_sql(conn, meta)

    if meta_store in {'json', 'both'}:
        meta_path = _meta_path_for_parquet(parquet_path)
        try:
            meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')
        except Exception:
            # meta is best-effort; parquet is the authoritative artifact
            pass


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
    try:
        import pyarrow as pa
    except Exception as e:
        raise RuntimeError(f"pyarrow is required for parquet export: {e}")

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
        if t in {"real", "float"}:
            return pa.float64()
        if t in {"decimal", "numeric", "money", "smallmoney"}:
            return pa.float64()
        if t in {"date", "datetime", "datetime2", "smalldatetime", "time", "datetimeoffset"}:
            return pa.string()
        if t in {"uniqueidentifier"}:
            return pa.string()
        if t in {"char", "nchar", "varchar", "nvarchar", "text", "ntext"}:
            return pa.string()
        return pa.string()

    fields = [pa.field(r[0], _map_sqlserver_type_to_arrow(r[1])) for r in rows]
    return pa.schema(fields)


def _override_arrow_schema_for_dataset(schema, *, dataset: str, table_name: str):
    if schema is None:
        return None

    if dataset != 'mes_batch_report':
        return schema

    try:
        import pyarrow as pa
    except Exception:
        return schema

    # Prevent legacy parquet schema (float) from forcing Operation/Plant to 80.0/1303.0.
    force_string_cols = {'Operation', 'Plant'}
    existing_field_names = {f.name for f in schema}
    new_fields = []
    
    for f in schema:
        if f.name in force_string_cols:
            new_fields.append(pa.field(f.name, pa.string()))
        else:
            new_fields.append(f)
            
    # Ensure VSM is included (Schema Evolution)
    if 'VSM' not in existing_field_names:
        new_fields.append(pa.field('VSM', pa.string()))

    # Ensure other new/critical fields are included (Schema Evolution)
    # These might be missing in older parquets but are present in the SQL View
    additional_fields = [
        ('PNW(d)', pa.float64()),
        ('LNW(d)', pa.float64()),
        ('ScrapQty', pa.float64()),
        ('unit_time', pa.float64()),
        ('PreviousBatchEndTime', pa.string()), # DateTime usually string in parquet from pyodbc/pandas
        ('EH_machine', pa.float64()),
        ('EH_labor', pa.float64()),
        ('Setup', pa.string()),
        ('Setup Time (h)', pa.float64()),
        ('OEE', pa.float64()),
        ('TrackOutOperator', pa.string()),
        ('Product_Desc', pa.string()),
        ('ProductNumber', pa.string()),
        ('factory_name', pa.string()),
        ('id', pa.int64())
    ]

    for name, dtype in additional_fields:
        if name not in existing_field_names:
             new_fields.append(pa.field(name, dtype))
        
    return pa.schema(new_fields)


def _df_to_parquet_with_schema(df: pd.DataFrame, out_path: Path, schema) -> None:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except Exception as e:
        raise RuntimeError(f"pyarrow is required for parquet export: {e}")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    if schema is None:
        df.to_parquet(out_path, index=False)
        return

    # Align columns to schema (drop extras, add missings as nulls)
    expected_cols = [f.name for f in schema]
    df_out = df.copy()
    for col in expected_cols:
        if col not in df_out.columns:
            df_out[col] = None
    df_out = df_out[expected_cols]

    # Coerce dtypes to schema to avoid cast failures caused by SQLite dynamic typing
    for field in schema:
        col = field.name
        try:
            if pa.types.is_integer(field.type):
                df_out[col] = pd.to_numeric(df_out[col], errors='coerce').astype('Int64')
            elif pa.types.is_floating(field.type):
                df_out[col] = pd.to_numeric(df_out[col], errors='coerce')
            elif pa.types.is_string(field.type):
                # Keep as string; preserve nulls
                df_out[col] = df_out[col].astype('string')
        except Exception:
            # Best-effort coercion; pyarrow cast below will be authoritative
            pass

    table = pa.Table.from_pandas(df_out, preserve_index=False)
    table = table.cast(schema, safe=False)
    pq.write_table(table, out_path)


def _sqlserver_table_has_column(conn, table_name: str, column_name: str) -> bool:
    try:
        schema, name, _ = _qualify_table_name(table_name)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?
            """,
            (schema, name, column_name),
        )
        return cur.fetchone() is not None
    except Exception:
        return False


def _sqlserver_table_exists(conn, table_name: str) -> bool:
    try:
        schema, name, _ = _qualify_table_name(table_name)
        cur = conn.cursor()
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
        return cur.fetchone() is not None
    except Exception:
        return False


def _month_ym(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def _add_months(d: date, delta_months: int) -> date:
    y = d.year
    m = d.month + delta_months
    while m > 12:
        y += 1
        m -= 12
    while m < 1:
        y -= 1
        m += 12
    return date(y, m, 1)


def _discover_months(conn, table_name: str, date_col: str) -> List[str]:
    """Discover unique months (YYYY-MM) in table - kept for backward compatibility"""
    if not _sqlserver_table_has_column(conn, table_name, date_col):
        return []

    try:
        _, _, qualified = _qualify_table_name(table_name)
        sql = (
            f"SELECT DISTINCT CONVERT(char(7), TRY_CONVERT(date, [{date_col}]), 120) AS ym "
            f"FROM {qualified} "
            f"WHERE TRY_CONVERT(date, [{date_col}]) IS NOT NULL "
            f"ORDER BY ym"
        )
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        months = [r[0] for r in rows if isinstance(r[0], str) and len(r[0]) == 7]
        return months
    except Exception as e:
        logger.warning(f"Failed to discover months for {table_name}.{date_col}: {e}")
        return []


def _sql_partition_stats(conn, table_name: str, date_col: str, ym: str) -> Tuple[int, Optional[int]]:
    """Return (row_count, record_hash_checksum) for a given month.

    If table has a record_hash column, compute CHECKSUM_AGG(BINARY_CHECKSUM(record_hash)) to detect changes.
    Otherwise returns checksum as None.
    """
    _, _, qualified = _qualify_table_name(table_name)
    has_hash = _sqlserver_table_has_column(conn, table_name, 'record_hash')

    if has_hash:
        sql = (
            f"SELECT COUNT(1) AS cnt, "
            f"CHECKSUM_AGG(BINARY_CHECKSUM(CAST([record_hash] AS NVARCHAR(512)))) AS h "
            f"FROM {qualified} "
            f"WHERE CONVERT(char(7), TRY_CONVERT(date, [{date_col}]), 120) = ?"
        )
        cur = conn.cursor()
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
    cur = conn.cursor()
    cur.execute(sql, (ym,))
    row = cur.fetchone()
    cnt = int(row[0]) if row and row[0] is not None else 0
    return cnt, None


def _month_list_last_n(base_month: str, n: int) -> List[str]:
    try:
        y, m = base_month.split('-')
        start = date(int(y), int(m), 1)
    except Exception:
        return [base_month]

    months: List[str] = []
    for i in range(max(1, int(n))):
        months.append(_month_ym(_add_months(start, -i)))
    return sorted(set(months))


def _discover_dates(conn, table_name: str, date_col: str) -> List[str]:
    """Discover unique dates (YYYY-MM-DD) in table for date-based partitioning"""
    if not _sqlserver_table_has_column(conn, table_name, date_col):
        return []

    try:
        _, _, qualified = _qualify_table_name(table_name)
        sql = (
            f"SELECT DISTINCT CONVERT(char(10), TRY_CONVERT(date, [{date_col}]), 120) AS dt "
            f"FROM {qualified} "
            f"WHERE TRY_CONVERT(date, [{date_col}]) IS NOT NULL "
            f"ORDER BY dt"
        )
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        dates = [r[0] for r in rows if isinstance(r[0], str) and len(r[0]) == 10]
        return dates
    except Exception as e:
        logger.warning(f"Failed to discover dates for {table_name}.{date_col}: {e}")
        return []


def _max_month_in_table(conn, table_name: str, date_col: str) -> Optional[str]:
    """Return max month (YYYY-MM) based on TRY_CONVERT(date, date_col)."""
    if not _sqlserver_table_has_column(conn, table_name, date_col):
        return None

    try:
        _, _, qualified = _qualify_table_name(table_name)
        sql = (
            f"SELECT CONVERT(char(7), MAX(TRY_CONVERT(date, [{date_col}])), 120) AS ym "
            f"FROM {qualified} "
            f"WHERE TRY_CONVERT(date, [{date_col}]) IS NOT NULL"
        )
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        ym = row[0] if row else None
        if isinstance(ym, str) and len(ym) == 7:
            return ym
        return None
    except Exception as e:
        logger.warning(f"Failed to get max month for {table_name}.{date_col}: {e}")
        return None


def _export_partitioned_exports(
    conn,
    dataset: str,
    table_name: str,
    date_col: str,
    months: List[str],
    *,
    reconcile: bool = False,
    force: bool = False,
    reconcile_change_months: Optional[set] = None,
    meta_store: str = 'sql',
) -> bool:
    """Export table partitioned by month: organized as YYYY/{prefix}_{YYYYMM}.parquet"""
    failed = False

    if not _sqlserver_table_has_column(conn, table_name, date_col):
        logger.warning(f"Skip partitioned export: {table_name} missing date column {date_col}")
        return False

    for ym in months:
        if len(ym) != 7:  # Expect YYYY-MM
            logger.warning(f"Skip invalid month format: {ym}")
            continue
        
        year = ym[:4]
        yyyymm = ym.replace('-', '')
        prefix = PARTITIONED_EXPORT_PREFIX.get(dataset, dataset)
        dst_path = PARTITIONED_OUTPUT_DIR / dataset / year / f"{prefix}_{yyyymm}.parquet"

        if reconcile and not force:
            try:
                if reconcile_change_months is not None and ym not in reconcile_change_months:
                    # Old month: do not open parquet metadata; only require file presence.
                    if dst_path.exists():
                        continue

                    sql_cnt, _ = _sql_partition_stats(conn, table_name, date_col, ym)
                    if sql_cnt <= 0:
                        logger.info(f"Skip empty partition: {table_name} ({ym})")
                        continue

                    logger.info(f"Reconcile: missing parquet, will export {table_name} ({ym}) -> {dst_path}")
                else:
                    pq_cnt = _parquet_row_count(dst_path)
                    meta = _read_partition_meta(conn, dst_path, dataset=dataset, ym=ym, meta_store=meta_store)

                    if pq_cnt is None:
                        sql_cnt, _ = _sql_partition_stats(conn, table_name, date_col, ym)
                        if sql_cnt <= 0:
                            logger.info(f"Skip empty partition: {table_name} ({ym})")
                            continue
                        logger.info(f"Reconcile: missing/corrupt parquet, will export {table_name} ({ym}) -> {dst_path}")
                    else:
                        sql_cnt, sql_h = _sql_partition_stats(conn, table_name, date_col, ym)
                        if sql_cnt <= 0:
                            logger.info(f"Skip empty partition: {table_name} ({ym})")
                            continue

                        if meta is not None and isinstance(meta, dict):
                            meta_cnt = meta.get('row_count')
                            meta_h = meta.get('record_hash_checksum')
                            if meta_cnt == sql_cnt and (sql_h is None or meta_h == sql_h):
                                logger.info(f"Reconcile: up-to-date, skip {table_name} ({ym})")
                                continue
                        else:
                            if pq_cnt == sql_cnt:
                                logger.info(f"Reconcile: row_count match, skip {table_name} ({ym})")
                                continue
            except Exception as e:
                logger.warning(f"Reconcile failed for {table_name} ({ym}), fallback to export: {e}")

        try:
            schema = _read_existing_schema(dst_path, None)
            if schema is None:
                schema = _sqlserver_table_to_arrow_schema(conn, table_name)

            schema = _override_arrow_schema_for_dataset(schema, dataset=dataset, table_name=table_name)

            try:
                _, _, qualified = _qualify_table_name(table_name)
                df = pd.read_sql_query(
                    f"SELECT * FROM {qualified} WHERE CONVERT(char(7), TRY_CONVERT(date, [{date_col}]), 120) = ?",
                    conn,
                    params=(ym,),
                )
            except Exception as e:
                msg = str(e)
                if 'invalid object name' in msg.lower() and dst_path.exists():
                    logger.warning(f"Table missing, keep existing parquet: {table_name} -> {dst_path}")
                    continue
                raise RuntimeError(f"read_sql failed for table {table_name}: {e}")

            if df.empty:
                logger.info(f"Skip empty partition: {table_name} ({ym})")
                continue

            if dataset == 'mes_batch_report':
                for c in ('Operation', 'Plant'):
                    if c in df.columns:
                        s = df[c].astype('string')
                        s = s.str.strip()
                        s = s.str.replace(r"\.0$", "", regex=True)
                        df[c] = s

            _df_to_parquet_with_schema(df, dst_path, schema)
            if reconcile:
                try:
                    sql_cnt, sql_h = _sql_partition_stats(conn, table_name, date_col, ym)
                    _write_partition_meta(
                        conn,
                        dst_path,
                        {
                            'dataset': dataset,
                            'table': table_name,
                            'date_col': date_col,
                            'ym': ym,
                            'row_count': int(sql_cnt),
                            'record_hash_checksum': int(sql_h) if sql_h is not None else None,
                            'parquet_path': str(dst_path),
                            'exported_at': date.today().isoformat(),
                        },
                        meta_store=meta_store,
                    )
                except Exception:
                    pass
            logger.info(f"Exported {table_name} ({ym}) -> {dst_path} ({len(df)} rows)")
        except Exception as e:
            logger.error(f"Failed to export partitioned {table_name} ({ym}) to {dst_path}: {e}")
            failed = True

    return failed


def _export_partitioned_table(
    conn,
    dataset: str,
    table_name: str,
    date_col: str,
    months: List[str],
    *,
    reconcile: bool = False,
    force: bool = False,
    reconcile_change_months: Optional[set] = None,
    meta_store: str = 'sql',
) -> bool:
    return _export_partitioned_exports(
        conn,
        dataset,
        table_name,
        date_col,
        months,
        reconcile=reconcile,
        force=force,
        reconcile_change_months=reconcile_change_months,
        meta_store=meta_store,
    )


def _export_static_exports(conn) -> bool:
    failed = False
    for filename, table_name in STATIC_EXPORTS.items():
        dst_path = STATIC_OUTPUT_DIR / filename
        try:
            if not _sqlserver_table_exists(conn, table_name):
                logger.warning(f"Static table missing, skip: {table_name}")
                continue

            schema = _read_existing_schema(dst_path, None)
            if schema is None:
                schema = _sqlserver_table_to_arrow_schema(conn, table_name)

            if schema is None:
                logger.warning(f"Failed to infer schema (skip static export): {table_name}")
                continue

            try:
                _, _, qualified = _qualify_table_name(table_name)
                df = pd.read_sql_query(f"SELECT * FROM {qualified}", conn)
            except Exception as e:
                msg = str(e)
                if 'invalid object name' in msg.lower() and dst_path.exists():
                    logger.warning(f"Table missing, keep existing parquet: {table_name} -> {dst_path}")
                    continue
                raise RuntimeError(f"read_sql failed for table {table_name}: {e}")
            _df_to_parquet_with_schema(df, dst_path, schema)
            logger.info(f"Exported {table_name} -> {dst_path} ({len(df)} rows)")
        except Exception as e:
            logger.error(f"Failed to export static {table_name} to {dst_path}: {e}")
            failed = True

    return failed


def _export_legacy_exports(conn) -> bool:
    failed = False
    for filename, table_name in EXPORTS.items():
        dst_path = A1_OUTPUT_DIR / filename

        try:
            schema = _read_existing_schema(dst_path, SCHEMA_FALLBACK_PARQUETS.get(filename))
            if schema is None:
                schema = _sqlserver_table_to_arrow_schema(conn, table_name)

            try:
                _, _, qualified = _qualify_table_name(table_name)
                df = pd.read_sql_query(f"SELECT * FROM {qualified}", conn)
            except Exception as e:
                msg = str(e)
                if 'invalid object name' in msg.lower() and dst_path.exists():
                    logger.warning(f"Table missing, keep existing parquet: {table_name} -> {dst_path}")
                    continue
                raise RuntimeError(f"read_sql failed for table {table_name}: {e}")

            _df_to_parquet_with_schema(df, dst_path, schema)
            logger.info(f"Exported {table_name} -> {dst_path} ({len(df)} rows)")
        except Exception as e:
            logger.error(f"Failed to export {table_name} to {filename}: {e}")
            failed = True

    return failed


def _export_metadata_exports(conn) -> bool:
    failed = False
    for filename, table_name in EXPORTS.items():
        if not filename.startswith('03_METADATA/'):
            continue

        dst_path = A1_OUTPUT_DIR / filename
        try:
            if not _sqlserver_table_exists(conn, table_name):
                logger.warning(f"Metadata table missing, skip: {table_name}")
                continue

            schema = _read_existing_schema(dst_path, None)
            if schema is None:
                schema = _sqlserver_table_to_arrow_schema(conn, table_name)

            if schema is None:
                logger.warning(f"Failed to infer schema (skip metadata export): {table_name}")
                continue

            try:
                _, _, qualified = _qualify_table_name(table_name)
                df = pd.read_sql_query(f"SELECT * FROM {qualified}", conn)
            except Exception as e:
                msg = str(e)
                if 'invalid object name' in msg.lower() and dst_path.exists():
                    logger.warning(f"Table missing, keep existing parquet: {table_name} -> {dst_path}")
                    continue
                raise RuntimeError(f"read_sql failed for table {table_name}: {e}")
            _df_to_parquet_with_schema(df, dst_path, schema)
            logger.info(f"Exported {table_name} -> {dst_path} ({len(df)} rows)")
        except Exception as e:
            logger.error(f"Failed to export {table_name} to {filename}: {e}")
            failed = True

    return failed


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['partitioned', 'legacy', 'all'], default='partitioned')
    parser.add_argument('--backwrite-days', type=int, default=7)
    parser.add_argument('--months', choices=['open', 'all'], default='open')
    parser.add_argument('--months-list', nargs='*', default=None)
    parser.add_argument('--reconcile', action='store_true')
    parser.add_argument('--reconcile-last-n', type=int, default=3)
    parser.add_argument('--reconcile-all', action='store_true')
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--datasets', nargs='*', default=None)
    parser.add_argument('--meta-store', choices=['sql', 'json', 'both'], default='sql')
    parser.add_argument('--skip-static', action='store_true')
    parser.add_argument('--skip-metadata', action='store_true')
    parser.add_argument('--skip-monitoring', action='store_true')
    args = parser.parse_args(argv)

    A1_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    STATIC_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    PARTITIONED_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    failed = False
    with pyodbc.connect(SQLSERVER_CONN_STR) as conn:
        if args.mode in {'legacy', 'all'}:
            failed = _export_legacy_exports(conn) or failed

        if args.mode in {'partitioned', 'all'}:
            today = date.today()
            curr_month = _month_ym(today)
            selected_datasets = args.datasets if args.datasets else list(PARTITIONED_EXPORTS.keys())
            unknown = [d for d in selected_datasets if d not in PARTITIONED_EXPORTS]
            if unknown:
                raise ValueError(f"Unknown datasets: {unknown}. Valid: {list(PARTITIONED_EXPORTS.keys())}")

            for dataset in selected_datasets:
                table_name, date_col = PARTITIONED_EXPORTS[dataset]
                if args.months_list:
                    months = args.months_list
                    for m in months:
                        if not isinstance(m, str) or len(m) != 7 or m[4] != '-' or not m[:4].isdigit() or not m[5:].isdigit():
                            raise ValueError(f"Invalid --months-list value: {m}. Expect YYYY-MM")
                elif args.months == 'all':
                    # Export all months (organized by year folders)
                    months = _discover_months(conn, table_name, date_col)
                    if not months:
                        logger.warning(f"No months discovered for {table_name}; skip partitioned export")
                        continue
                else:
                    # Export latest available month for each dataset (handles data latency, e.g. SAP labor hours)
                    base_month = _max_month_in_table(conn, table_name, date_col) or curr_month
                    months = [base_month]
                    if args.backwrite_days is not None and today.day <= int(args.backwrite_days):
                        try:
                            y, m = base_month.split('-')
                            prev = _month_ym(_add_months(date(int(y), int(m), 1), -1))
                            if prev not in months:
                                months.append(prev)
                        except Exception:
                            pass

                if args.reconcile and (not args.months_list) and (not args.reconcile_all) and (args.months != 'all'):
                    months = _month_list_last_n(
                        _max_month_in_table(conn, table_name, date_col) or curr_month,
                        int(args.reconcile_last_n),
                    )

                failed = _export_partitioned_table(
                    conn,
                    dataset,
                    table_name,
                    date_col,
                    months,
                    reconcile=bool(args.reconcile),
                    force=bool(args.force),
                    reconcile_change_months=(
                        None
                        if (not args.reconcile or args.reconcile_all or args.months == 'all')
                        else set(_month_list_last_n(_max_month_in_table(conn, table_name, date_col) or curr_month, int(args.reconcile_last_n)))
                    ),
                    meta_store=str(args.meta_store),
                ) or failed

            if not args.skip_static:
                failed = _export_static_exports(conn) or failed
            if not args.skip_metadata:
                failed = _export_metadata_exports(conn) or failed

    # Copy monitoring trigger result outputs (CSV/TSV) into A1 metadata folder
    try:
        if args.skip_monitoring:
            return 1 if failed else 0
        monitoring_src_dir = PROJECT_ROOT / 'data_pipelines' / 'monitoring' / 'output'
        monitoring_dst_dir = A1_OUTPUT_DIR / '03_METADATA' / 'monitoring'
        monitoring_dst_dir.mkdir(parents=True, exist_ok=True)

        for name in ['kpi_global_matrix.csv', 'pareto_top_matrix.csv', 'kpi_trigger_results.csv', 'kpi_trigger_results.tsv']:
            src = monitoring_src_dir / name
            if not src.exists():
                continue
            shutil.copy2(src, monitoring_dst_dir / name)
            logger.info(f"Copied monitoring output -> {monitoring_dst_dir / name}")

        # Export KPI trigger results to Parquet (for Power BI)
        triggers_csv = monitoring_src_dir / 'kpi_trigger_results.csv'
        triggers_parquet = monitoring_dst_dir / 'kpi_trigger_results.parquet'
        if triggers_csv.exists():
            try:
                import pyarrow as pa

                # Prefer existing parquet schema if available (stability)
                trigger_schema = _read_existing_schema(triggers_parquet, None)
                if trigger_schema is None:
                    trigger_schema = pa.schema([
                        pa.field('A3Id', pa.string()),
                        pa.field('Category', pa.string()),
                        pa.field('TriggerType', pa.string()),
                        pa.field('TriggerName', pa.string()),
                        pa.field('TriggerLevel', pa.string()),
                        pa.field('TriggerDesc', pa.string()),
                        pa.field('ConsecutiveWeeks', pa.int64()),
                        pa.field('WeeklyDetails', pa.string()),
                        pa.field('TriggerStatus', pa.string()),
                        pa.field('LastUpdate', pa.string()),
                        pa.field('CaseStatus', pa.string()),
                        pa.field('IsCurrentTrigger', pa.string()),
                        pa.field('OpenedAt', pa.string()),
                        pa.field('ClosedAt', pa.string()),
                        pa.field('PlannerTaskId', pa.string()),
                        pa.field('Owner', pa.string()),
                        pa.field('ActionType', pa.string()),
                        pa.field('DataSource', pa.string()),
                    ])

                df_triggers = pd.read_csv(triggers_csv, dtype=str, encoding='utf-8-sig')
                if 'ConsecutiveWeeks' in df_triggers.columns:
                    df_triggers['ConsecutiveWeeks'] = pd.to_numeric(df_triggers['ConsecutiveWeeks'], errors='coerce').astype('Int64')

                _df_to_parquet_with_schema(df_triggers, triggers_parquet, trigger_schema)
                logger.info(f"Exported monitoring trigger results -> {triggers_parquet} ({len(df_triggers)} rows)")
            except Exception as e:
                logger.warning(f"Failed to export kpi_trigger_results.parquet: {e}")
    except Exception as e:
        logger.warning(f"Failed to copy monitoring outputs to A1: {e}")

    return 1 if failed else 0


if __name__ == '__main__':
    raise SystemExit(main())
