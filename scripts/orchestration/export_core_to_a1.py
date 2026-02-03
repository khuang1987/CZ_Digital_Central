import argparse
import json
import logging
import shutil
import sys
import warnings
from datetime import date

# Suppress pandas UserWarning: pandas only supports SQLAlchemy connectable...
warnings.filterwarnings("ignore", category=UserWarning, message=r".*pandas only supports SQLAlchemy connectable.*")
from pathlib import Path
from typing import Optional, List, Tuple

import pandas as pd
import pyodbc

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


import os
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

def get_output_dir() -> Path:
    # 1. Direct Override via Env Var
    env_export_dir = os.getenv("MDDAP_EXPORT_DIR")
    if env_export_dir:
        return Path(env_export_dir)

    # 2. Derive from OneDrive Root (Standard Team Path)
    # Default suffix path within OneDrive
    relative_path = r"CZ Production - 文档\General\POWER BI 数据源 V2\A1_ETL_Output"
    
    onedrive_root = os.getenv("MDDAP_ONEDRIVE_ROOT")
    if onedrive_root:
        candidate = Path(onedrive_root) / relative_path
        if candidate.exists():
            return candidate
            
    # 3. Fallback: Hardcoded Path (Legacy/Dev)
    # WARNING: This path works only for specific user (huangk14)
    fallback = Path(r"C:\Users\huangk14\OneDrive - Medtronic PLC") / relative_path
    if fallback.exists():
        logger.warning(f"Using fallback hardcoded output path: {fallback}")
        return fallback

    # 4. Error if nothing found
    raise RuntimeError(
        "Could not determine A1_OUTPUT_DIR. Please set 'MDDAP_EXPORT_DIR' or 'MDDAP_ONEDRIVE_ROOT' in .env"
    )

A1_OUTPUT_DIR = get_output_dir()


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



# ==============================================================================
# Shared Infrastructure Import
# ==============================================================================
try:
    from shared_infrastructure.export_utils import (
        export_partitioned_table, 
        get_default_output_dir, 
        _read_existing_schema, 
        _sqlserver_table_to_arrow_schema,
        _df_to_parquet_with_schema,
        _qualify_table_name,
        _sqlserver_table_exists,
        _month_ym,
        _add_months,
        _month_list_last_n,
        _max_month_in_table
    )
except ImportError as e:
    logger.error(f"Failed to import shared_infrastructure: {e}")
    sys.exit(1)

A1_OUTPUT_DIR = get_default_output_dir()
STATIC_OUTPUT_DIR = A1_OUTPUT_DIR / '01_CURATED_STATIC'
PARTITIONED_OUTPUT_DIR = A1_OUTPUT_DIR / '02_CURATED_PARTITIONED'

# ... (Configuration Maps remain same) ...
# ... (Functions below removed as they are now in export_utils) ...

# ==============================================================================
# Export Implementations
# ==============================================================================

def _discover_months(conn, table_name: str, date_col: str) -> List[str]:
    # Kept locally or moved really? 
    # Let's use simple logic here or import from utils if available (it wasn't in public API I exported)
    # Re-implement simple discovery or move to utils?
    # I realized I didn't export `_discover_months` in `__all__` style but it is def-ed in export_utils
    # But since it's a helper, I can just query here or add to utils.
    # Actually, export_partitioned_table takes `months` list. Or does it discover?
    # My export_utils.export_partitioned_table takes `months`.
    # So discovery is still needed here.
    
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
        return [r[0] for r in rows if isinstance(r[0], str) and len(r[0]) == 7]
    except Exception:
        return []

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
    
    # Filter months if reconcile_change_months is set (optimization)
    # If using export_utils logic, it handles reconcile logic generally.
    # But reconcile_change_months was specific logic in legacy script (last N months).
    
    target_months = months
    if reconcile and reconcile_change_months is not None:
        # If we only want to reconcile specific months (e.g. last 2), filter here
        # But wait, original logic was: "Iterate all months, IF month in change_set OR missing, export".
        # export_utils.export_partitioned_table iterates all months passed to it.
        # So we should pass ALL months, but maybe we can optimize?
        # No, let's look at export_utils: 
        # It checks DB vs Meta vs Parquet. If they match, it skips.
        # So passing ALL months is fine, it will skip old ones quickly if meta exists.
        pass

    return export_partitioned_table(
        conn,
        dataset,
        table_name,
        date_col,
        target_months,
        output_dir=A1_OUTPUT_DIR,
        reconcile=reconcile,
        force=force
    )

def _export_static_exports(conn) -> bool:
    failed = False
    for filename, table_name in STATIC_EXPORTS.items():
        dst_path = STATIC_OUTPUT_DIR / filename
        try:
            if not _sqlserver_table_exists(conn, table_name):
                logger.warning(f"Static table missing, skip: {table_name}")
                continue

            schema = _read_existing_schema(dst_path)
            if schema is None:
                # Use shared util
                schema = _sqlserver_table_to_arrow_schema(conn, table_name)

            if schema is None:
                logger.warning(f"Failed to infer schema (skip static export): {table_name}")
                continue

            try:
                _, _, qualified = _qualify_table_name(table_name)
                df = pd.read_sql_query(f"SELECT * FROM {qualified}", conn)
            except Exception as e:
                 logger.warning(f"Read failed: {e}")
                 continue
            
            _df_to_parquet_with_schema(df, dst_path, schema)
            logger.info(f"Exported {table_name} -> {dst_path} ({len(df)} rows)")
        except Exception as e:
            logger.error(f"Failed to export static {table_name}: {e}")
            failed = True
    return failed
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




# Legacy definitions removed (see export_utils.py)

def _export_legacy_exports(conn) -> bool:
    failed = False
    for filename, table_name in STATIC_EXPORTS.items():
        dst_path = STATIC_OUTPUT_DIR / filename
        try:
            if not _sqlserver_table_exists(conn, table_name):
                logger.warning(f"Static table missing, skip: {table_name}")
                continue

            schema = _read_existing_schema(dst_path)
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
            schema = _read_existing_schema(dst_path)
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

            schema = _read_existing_schema(dst_path)
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

                failed = export_partitioned_table(
                    conn,
                    dataset,
                    table_name,
                    date_col,
                    months,
                    output_dir=A1_OUTPUT_DIR,
                    reconcile=bool(args.reconcile),
                    force=bool(args.force)
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
                trigger_schema = _read_existing_schema(triggers_parquet)
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
