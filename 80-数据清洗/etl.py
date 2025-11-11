import os
import sys
import time
import glob
import logging
import shutil
import threading
from datetime import datetime
from typing import Dict, List, Any
import re
import pandas.api.types as ptypes

import pandas as pd
import yaml

try:
    import pyarrow as pa  # noqa: F401
    import pyarrow.parquet as pq  # noqa: F401
except Exception:  # pragma: no cover
    pass

# Ensure mixed object columns are safe for Parquet by casting to pandas string dtype
# Decodes bytes to UTF-8 where possible

def coerce_object_columns_to_string(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    for col in out.columns:
        dtype = out[col].dtype
        if ptypes.is_integer_dtype(dtype) or ptypes.is_float_dtype(dtype) or ptypes.is_bool_dtype(dtype) or ptypes.is_datetime64_any_dtype(dtype):
            continue
        if dtype == object or ptypes.is_object_dtype(dtype):
            try:
                if out[col].map(lambda x: isinstance(x, (bytes, bytearray))).any():
                    out[col] = out[col].map(lambda x: x.decode('utf-8', errors='ignore') if isinstance(x, (bytes, bytearray)) else x)
            except Exception:
                pass
            out[col] = out[col].astype("string")
    return out

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config", "config.yaml")
BASE_DIR = os.path.dirname(__file__)


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def ask_clear_manifest(cfg: Dict[str, Any]) -> bool:
    """Ask user if they want to clear manifest.csv with 5-second countdown."""
    manifest_path = os.path.join(BASE_DIR, cfg.get("manifest", {}).get("path", "logs/manifest.csv"))
    
    if not os.path.exists(manifest_path):
        print("Manifest file does not exist, starting fresh.")
        return False
    
    print("\n" + "="*60)
    print("ETL 数据清洗程序启动")
    print("="*60)
    print(f"检测到已存在的处理记录文件: {manifest_path}")
    print("是否清空处理记录，重新处理所有文件？")
    print("  [Y] 是 - 清空记录，重新处理所有文件")
    print("  [N] 否 - 保持记录，只处理新增/变更文件 (默认)")
    print("="*60)
    
    # 5秒倒计时
    def countdown():
        for i in range(5, 0, -1):
            print(f"\r将在 {i} 秒后使用默认选项 (N)...", end="", flush=True)
            time.sleep(1)
        print("\r使用默认选项: 保持记录，只处理新增/变更文件")
    
    # 启动倒计时线程
    countdown_thread = threading.Thread(target=countdown)
    countdown_thread.daemon = True
    countdown_thread.start()
    
    # 等待用户输入或超时
    try:
        import msvcrt
        start_time = time.time()
        while time.time() - start_time < 5:
            if msvcrt.kbhit():
                key = msvcrt.getch().decode('utf-8', errors='ignore').lower()
                if key in ['y', 'n', '\r']:
                    print(f"\n用户选择: {key}")
                    return key == 'y'
            time.sleep(0.1)
    except ImportError:
        # 非Windows系统，使用标准输入
        try:
            response = input("\n请输入选择 (Y/N): ").strip().lower()
            return response == 'y'
        except (EOFError, KeyboardInterrupt):
            pass
    
    return False  # 默认不清空


def setup_logging(cfg: Dict[str, Any]) -> None:
    log_file = os.path.join(BASE_DIR, cfg.get("logging", {}).get("file", "logs/etl.log"))
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    level_name = cfg.get("logging", {}).get("level", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def read_manifest(cfg: Dict[str, Any]) -> pd.DataFrame:
    manifest_path = os.path.join(BASE_DIR, cfg.get("manifest", {}).get("path", "logs/manifest.csv"))
    if os.path.exists(manifest_path):
        try:
            df = pd.read_csv(manifest_path)
            for col in ["size"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            return df
        except Exception:
            logging.warning("Failed to read manifest; starting fresh.")
    return pd.DataFrame(columns=["full_path", "size", "last_write_time"])


def write_manifest(cfg: Dict[str, Any], df: pd.DataFrame) -> None:
    manifest_path = os.path.join(BASE_DIR, cfg.get("manifest", {}).get("path", "logs/manifest.csv"))
    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
    df.to_csv(manifest_path, index=False, encoding="utf-8")


def list_source_files(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    src_root = cfg.get("source", {}).get("root_path", "").strip().strip('"')
    include_glob = cfg.get("source", {}).get("include_glob", "**/*.xlsx")
    exclude_patterns = cfg.get("source", {}).get("exclude_patterns", [])

    if not src_root:
        raise ValueError("source.root_path is empty. Please set it in config/config.yaml")
    if not os.path.isdir(src_root):
        raise FileNotFoundError(f"Source root does not exist: {src_root}")

    pattern = os.path.join(src_root, include_glob)
    candidates = glob.glob(pattern, recursive=True)

    def excluded(path: str) -> bool:
        name = os.path.basename(path)
        return any(pd.Series(name).str.match(pat.replace("*", ".*").replace("?", "."), case=False).iloc[0]
                   for pat in exclude_patterns) if exclude_patterns else False

    files = []
    for p in candidates:
        if os.path.isdir(p) or excluded(p):
            continue
        try:
            stat = os.stat(p)
            rel_dir = os.path.relpath(os.path.dirname(p), src_root)
            base = os.path.basename(p)
            files.append({
                "full_path": os.path.normpath(p),
                "size": int(stat.st_size),
                "last_write_time": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
                "rel_dir": rel_dir,
                "base": base,
            })
        except Exception as e:
            logging.warning(f"Stat failed: {p}: {e}")
    return files

_PREFIX_RE = re.compile(r"^(?P<prefix>[A-Z]+-)(?P<ym>\d{6})")

def parse_prefix_and_ym(filename: str) -> Dict[str, Any]:
    m = _PREFIX_RE.match(filename)
    if m:
        ym = m.group("ym")
        year = int(ym[:4])
        month = int(ym[4:6])
        return {"prefix": m.group("prefix"), "year": year, "month": month}
    # Fallback: try only prefix without ym
    m2 = re.match(r"^(?P<prefix>[A-Z]+-)", filename)
    if m2:
        return {"prefix": m2.group("prefix"), "year": None, "month": None}
    return {"prefix": "EM-", "year": None, "month": None}


def normalize_columns(df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    schema_cfg = cfg.get("schema", {})
    if schema_cfg.get("normalize_column_names", True):
        df = df.rename(columns=lambda c: str(c).strip().lower().replace(" ", "_").replace("-", "_") if c is not None else c)

    # Apply mappings: canonical: [alias1, alias2]
    mappings: Dict[str, List[str]] = schema_cfg.get("mappings", {})
    for canonical, aliases in mappings.items():
        if canonical in df.columns:
            continue
        for alias in aliases or []:
            if alias in df.columns:
                df = df.rename(columns={alias: canonical})
                break

    # Cast dtypes
    dtypes: Dict[str, str] = schema_cfg.get("dtypes", {})
    for col, dtype in dtypes.items():
        if col in df.columns:
            try:
                if dtype.startswith("datetime"):
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                elif dtype == "int64":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                elif dtype == "float64":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
                elif dtype == "bool":
                    df[col] = df[col].astype("boolean")
                else:
                    df[col] = df[col].astype("string")
            except Exception as e:
                sample = None
                try:
                    sample = df[col].head(2)
                except Exception:
                    sample = "<unavailable>"
                logging.warning(f"Failed cast {col} to {dtype}: {e}; sample={sample}")

    # Datetime timezone
    dt_cfg = schema_cfg.get("datetime", {})
    tz = dt_cfg.get("timezone")
    for col in dt_cfg.get("columns", []) or []:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                if tz:
                    # Localize naive to tz, keep tz-aware if already
                    if df[col].dt.tz is None:
                        df[col] = df[col].dt.tz_localize(tz, nonexistent="shift_forward", ambiguous="NaT")
                    else:
                        df[col] = df[col].dt.tz_convert(tz)
            except Exception as e:
                logging.warning(f"Failed timezone set for {col}: {e}")

    return df


def deduplicate(df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    dedup_cfg = cfg.get("deduplication", {})
    pk = dedup_cfg.get("primary_key", [])
    order_col = dedup_cfg.get("order_by_timestamp")

    if not len(df):
        return df

    if pk:
        if order_col and order_col in df.columns:
            df = df.sort_values(by=order_col, ascending=False, kind="stable")
        df = df.drop_duplicates(subset=pk, keep="first")
    else:
        df = df.drop_duplicates(keep="first")
    return df


def add_partitions(df: pd.DataFrame, cfg: Dict[str, Any], fallback_dt: datetime) -> pd.DataFrame:
    # Use order_by_timestamp for partition if available, otherwise fallback to file mtime
    order_col = cfg.get("deduplication", {}).get("order_by_timestamp")
    ts_series = None
    if order_col and order_col in df.columns:
        ts_series = pd.to_datetime(df[order_col], errors="coerce")
    # If ts_series missing or entirely NaT, fill with fallback for every row (aligned index)
    if ts_series is None or ts_series.isna().all():
        ts_series = pd.Series(pd.to_datetime(fallback_dt), index=df.index)

    df = df.copy()
    df["year"] = ts_series.dt.year.astype("Int64")
    df["month"] = ts_series.dt.month.astype("Int64")
    return df


def write_partitions(df: pd.DataFrame, cfg: Dict[str, Any]) -> None:
    if not len(df):
        return
    output_dir = os.path.join(BASE_DIR, cfg.get("output", {}).get("base_dir", "publish"))
    os.makedirs(output_dir, exist_ok=True)

    out_format = cfg.get("output", {}).get("format", "parquet").lower()
    compression = cfg.get("output", {}).get("parquet", {}).get("compression", "snappy")

    # Group by prefix and year only; write one file per (prefix, year): PREFIX-YYYY
    for (prefix, year), g in df.groupby(["prefix", "year"], dropna=False):
        if isinstance(prefix, tuple):
            prefix = prefix[0]
        if pd.isna(year):
            logging.warning("Skip group with NaN year")
            continue
        year = int(year)
        year_dir = os.path.join(output_dir, f"year={year}")
        os.makedirs(year_dir, exist_ok=True)

        file_name = f"{prefix}{year}.{'parquet' if out_format=='parquet' else 'csv'}"
        out_path = os.path.join(year_dir, file_name)

        combined = g
        if os.path.exists(out_path):
            try:
                if out_format == "parquet":
                    old = pd.read_parquet(out_path, engine="pyarrow")
                else:
                    old = pd.read_csv(out_path, encoding="utf-8")
                combined = pd.concat([old, g], ignore_index=True)
            except Exception as e:
                logging.warning(f"Failed to read existing yearly file, will overwrite: {out_path}: {e}")
                combined = g

        combined = deduplicate(combined, cfg)
        combined = coerce_object_columns_to_string(combined)

        tmp_path = out_path + ".tmp"
        if out_format == "parquet":
            combined.to_parquet(tmp_path, index=False, engine="pyarrow", compression=compression)
        else:
            combined.to_csv(tmp_path, index=False, encoding="utf-8")
        try:
            if os.path.exists(out_path):
                os.remove(out_path)
            os.replace(tmp_path, out_path)
        except Exception:
            shutil.move(tmp_path, out_path)

        logging.info(f"Written {len(combined)} rows -> {out_path}")


def process_files(cfg: Dict[str, Any]) -> None:
    manifest_df = read_manifest(cfg)
    files = list_source_files(cfg)
    files_df = pd.DataFrame(files)

    # Detect new or changed
    merged = files_df.merge(manifest_df, on="full_path", how="left", suffixes=("", "_old"))
    need = merged[(merged["size_old"].isna()) | (merged["size"] != merged["size_old"]) | (merged["last_write_time"] != merged["last_write_time_old"])]
    # Sort by last_write_time desc
    need = need.sort_values(by="last_write_time", ascending=False, kind="stable")
    # Per-folder cap: take top N per rel_dir
    per_folder_limit = int(cfg.get("runtime", {}).get("per_folder_limit", 10) or 10)
    # Group by rel_dir and take top N per folder
    grouped_files = []
    for rel_dir, group in need.groupby("rel_dir"):
        top_n = group.head(per_folder_limit)
        grouped_files.append(top_n)
    if grouped_files:
        need = pd.concat(grouped_files, ignore_index=True)
    else:
        need = pd.DataFrame(columns=need.columns)
    # Global cap as an extra safeguard
    max_n_cfg = int(cfg.get("runtime", {}).get("max_files_per_run", 0) or 0)
    if max_n_cfg > 0:
        need = need.head(max_n_cfg)

    if need.empty:
        logging.info("No new or changed files.")
        return

    # Prepare existing sources per (prefix, year) to allow short-circuit
    output_dir = os.path.join(BASE_DIR, cfg.get("output", {}).get("base_dir", "publish"))
    out_format = cfg.get("output", {}).get("format", "parquet").lower()

    def row_key(row: pd.Series) -> Any:
        base = row["base"]
        parsed = parse_prefix_and_ym(base)
        ts = datetime.fromisoformat(row["last_write_time"]) if isinstance(row["last_write_time"], str) else datetime.now()
        year = parsed["year"] or ts.year
        return (parsed["prefix"], year)

    keys = sorted(set(row_key(r) for _, r in need.iterrows()))
    existing_sources_by_key: Dict[Any, set] = {}
    for (prefix, y) in keys:
        year_dir = os.path.join(output_dir, f"year={y}")
        file_name = f"{prefix}{y}.{'parquet' if out_format=='parquet' else 'csv'}"
        out_path = os.path.join(year_dir, file_name)
        s = set()
        if os.path.exists(out_path):
            try:
                if out_format == "parquet":
                    old = pd.read_parquet(out_path, engine="pyarrow")
                else:
                    old = pd.read_csv(out_path, encoding="utf-8")
                if "_source_file" in old.columns:
                    s = set(old["_source_file"].dropna().astype(str).tolist())
            except Exception as e:
                logging.warning(f"Failed to read existing yearly file for short-circuit: {out_path}: {e}")
        existing_sources_by_key[(prefix, y)] = s

    # Group files by subfolder first, then process each subfolder independently
    subfolder_groups = {}
    for _, row in need.iterrows():
        rel_dir = row["rel_dir"]
        if rel_dir not in subfolder_groups:
            subfolder_groups[rel_dir] = []
        subfolder_groups[rel_dir].append(row)

    logging.info(f"Found {len(subfolder_groups)} subfolders to process: {list(subfolder_groups.keys())}")

    for rel_dir, folder_rows in subfolder_groups.items():
        logging.info(f"Processing subfolder: {rel_dir} with {len(folder_rows)} files")
        folder_data = []
        stop_keys_for_folder: set = set()
        
        for row in folder_rows:
            fpath = row["full_path"]
            base = row["base"]
            prefix_year = row_key(row)
            if prefix_year in stop_keys_for_folder:
                continue
            if base in existing_sources_by_key.get(prefix_year, set()):
                logging.info(f"Encountered already merged file for {prefix_year}: {base}; skipping older files for this key in folder {rel_dir}.")
                stop_keys_for_folder.add(prefix_year)
                continue

            logging.info(f"Processing: {fpath}")
            try:
                df = pd.read_excel(fpath, engine="openpyxl")
                df = normalize_columns(df, cfg)
                df = deduplicate(df, cfg)
                mtime = datetime.fromisoformat(row["last_write_time"]) if isinstance(row["last_write_time"], str) else datetime.now()
                parsed = parse_prefix_and_ym(base)
                df = add_partitions(df, cfg, fallback_dt=mtime)
                # Override partitions if parsed from filename
                if parsed["year"] and parsed["month"]:
                    df["year"] = int(parsed["year"])
                    # month is not used for output anymore; keep for lineage if needed
                df["prefix"] = parsed["prefix"]
                if len(df):
                    df["_source_file"] = base
                    folder_data.append(df)
                    # Update existing set so repeated files in same run are recognized
                    existing_sources_by_key.setdefault(prefix_year, set()).add(base)
            except Exception as e:
                logging.error(f"Failed to process {fpath}: {e}")
                if cfg.get("runtime", {}).get("on_error", "continue") != "continue":
                    raise
                try:
                    err_dir = os.path.join(BASE_DIR, "staging", "_errors")
                    os.makedirs(err_dir, exist_ok=True)
                    shutil.copy2(fpath, os.path.join(err_dir, os.path.basename(fpath)))
                except Exception as ce:
                    logging.warning(f"Failed to copy error file: {ce}")

        # Process this subfolder's data
        if folder_data:
            big = pd.concat(folder_data, ignore_index=True)
            logging.info(f"Writing {len(big)} rows for subfolder: {rel_dir}")
            write_partitions(big, cfg)
        else:
            logging.info(f"No data to write for subfolder: {rel_dir}")

    processed = need[["full_path", "size", "last_write_time"]].copy()
    remaining = manifest_df[~manifest_df["full_path"].isin(processed["full_path"])].copy()
    new_manifest = pd.concat([remaining, processed], ignore_index=True)
    write_manifest(cfg, new_manifest)


if __name__ == "__main__":
    cfg = load_config(CONFIG_PATH)
    
    # 询问是否清空 manifest
    should_clear = ask_clear_manifest(cfg)
    if should_clear:
        manifest_path = os.path.join(BASE_DIR, cfg.get("manifest", {}).get("path", "logs/manifest.csv"))
        if os.path.exists(manifest_path):
            os.remove(manifest_path)
            print("已清空处理记录，将重新处理所有文件。")
        else:
            print("处理记录文件不存在，将处理所有文件。")
    
    setup_logging(cfg)
    t0 = time.time()
    try:
        process_files(cfg)
        logging.info(f"Done in {time.time() - t0:.2f}s")
    except Exception as e:
        logging.exception(f"ETL failed: {e}")
        sys.exit(1)
