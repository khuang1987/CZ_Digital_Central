import argparse
import glob
import sqlite3
import sys
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA busy_timeout = 30000;")
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
    ).fetchone()
    return row is not None


def _load_mes_files_from_config(cfg_path: Path) -> List[str]:
    from shared_infrastructure.utils.etl_utils import load_config

    cfg = load_config(str(cfg_path))
    mes_sources = cfg.get("source", {}).get("mes_sources", [])

    files: List[str] = []
    for s in mes_sources:
        if not s.get("enabled", True):
            continue
        pattern = s.get("pattern")
        if pattern:
            files.extend(glob.glob(pattern))
        elif s.get("path"):
            files.append(s["path"])

    return sorted(set(files))


def _estimate_excel_rows(file_path: str) -> Tuple[int, str]:
    """Return (estimated_data_rows, error).

    Avoids openpyxl max_row (often unavailable/0 for some workbooks).
    Instead counts <row> tags in the first worksheet XML inside the xlsx zip.
    """
    try:
        with zipfile.ZipFile(file_path, "r") as zf:
            sheet_xml_candidates = sorted(
                [n for n in zf.namelist() if n.startswith("xl/worksheets/") and n.endswith(".xml")]
            )
            if not sheet_xml_candidates:
                return (0, "No worksheet xml found")

            sheet_xml = sheet_xml_candidates[0]

            count = 0
            with zf.open(sheet_xml, "r") as f:
                # Stream parse to handle namespaces like {..}row / x:row
                for event, elem in ET.iterparse(f, events=("end",)):
                    tag = elem.tag
                    if isinstance(tag, str) and tag.endswith("row"):
                        count += 1
                    elem.clear()

            # Assume first row is header (best-effort)
            return (max(count - 1, 0), "")
    except Exception as e:
        return (0, f"{type(e).__name__}: {e}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit MES raw_mes vs source Excels")
    parser.add_argument(
        "--db-path",
        default=None,
        help="SQLite db path (default: data_pipelines/database/mddap_v2.db)",
    )
    parser.add_argument(
        "--limit-files",
        type=int,
        default=0,
        help="Limit number of files to audit (0 = all)",
    )
    args = parser.parse_args()

    project_root = _project_root()
    db_path = (
        Path(args.db_path)
        if args.db_path
        else project_root / "data_pipelines" / "database" / "mddap_v2.db"
    )
    cfg_path = (
        project_root
        / "data_pipelines"
        / "sources"
        / "mes"
        / "config"
        / "config_mes_batch_report.yaml"
    )

    files = _load_mes_files_from_config(cfg_path)
    if args.limit_files and args.limit_files > 0:
        files = files[: args.limit_files]

    print("=== MES Raw Audit ===")
    print("DB:", db_path)
    print("Config:", cfg_path)
    print("Files from config:", len(files))

    conn = _connect(db_path)
    try:
        if not _table_exists(conn, "raw_mes"):
            print("[ERROR] table raw_mes not found")
            return 2

        total_raw = conn.execute("SELECT COUNT(*) AS n FROM raw_mes").fetchone()[0]
        distinct_files = conn.execute(
            "SELECT COUNT(DISTINCT source_file) AS n FROM raw_mes WHERE source_file IS NOT NULL"
        ).fetchone()[0]
        print("raw_mes rows:", total_raw)
        print("raw_mes distinct source_file:", distinct_files)

        # Per-file rows in sqlite
        rows_by_file: Dict[str, int] = {}
        for r in conn.execute(
            "SELECT source_file, COUNT(*) AS n FROM raw_mes GROUP BY source_file"
        ).fetchall():
            rows_by_file[r["source_file"]] = int(r["n"])

        # Quality stats (overall)
        key_cols = ["BatchNumber", "Operation", "Machine", "TrackOutTime", "TrackOutQuantity", "CFN", "ProductNumber"]
        existing_cols = [
            row["name"]
            for row in conn.execute("PRAGMA table_info(raw_mes)").fetchall()
        ]
        key_cols = [c for c in key_cols if c in existing_cols]

        null_stats = []
        for c in key_cols:
            n_null = conn.execute(
                f"SELECT COUNT(*) AS n FROM raw_mes WHERE {c} IS NULL OR TRIM(CAST({c} AS TEXT)) = ''"
            ).fetchone()[0]
            null_stats.append((c, int(n_null)))

        dup_record_hash = 0
        if "record_hash" in existing_cols:
            dup_record_hash = conn.execute(
                "SELECT COALESCE(SUM(cnt - 1), 0) FROM (SELECT record_hash, COUNT(*) cnt FROM raw_mes GROUP BY record_hash HAVING cnt > 1)"
            ).fetchone()[0]

        print("\n--- Data Quality (overall) ---")
        for c, n_null in null_stats:
            rate = (n_null / total_raw) if total_raw else 0
            print(f"{c}: null/blank={n_null} ({rate:.2%})")
        if "record_hash" in existing_cols:
            print("record_hash duplicates (extra rows):", int(dup_record_hash))

    finally:
        conn.close()

    print("\n--- Per-file reconciliation ---")
    print("Columns: file | excel_est_rows | sqlite_rows | delta(sqlite-excel) | excel_read_error")

    for fp in files:
        est_rows, err = _estimate_excel_rows(fp)
        sqlite_rows = rows_by_file.get(fp, 0)
        delta = sqlite_rows - est_rows
        print(f"{fp} | {est_rows} | {sqlite_rows} | {delta} | {err}")

    # Also show files in SQLite but not in config list
    config_set = set(files)
    extra_sqlite_files = [f for f in rows_by_file.keys() if f not in config_set]
    if extra_sqlite_files:
        print("\n--- SQLite has source_file not in current config ---")
        for f in sorted(extra_sqlite_files):
            print(f"{f} | sqlite_rows={rows_by_file.get(f, 0)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
