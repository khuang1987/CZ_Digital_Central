import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

import pyodbc


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class SqlObject:
    name: str
    obj_type: str  # table | view
    row_count: int | None


def _conn_str(server: str, database: str, driver: str) -> str:
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
        "Encrypt=no;"
    )


def _fetch_sql_objects(conn) -> Dict[str, SqlObject]:
    cur = conn.cursor()

    # Row counts for tables (fast, approximate but stable)
    cur.execute(
        """
        SELECT
            t.name AS table_name,
            SUM(p.row_count) AS row_count
        FROM sys.tables t
        JOIN sys.dm_db_partition_stats p
            ON p.object_id = t.object_id
        WHERE p.index_id IN (0, 1)
            AND SCHEMA_NAME(t.schema_id) = 'dbo'
        GROUP BY t.name
        """
    )
    table_counts = {str(r[0]): int(r[1]) for r in cur.fetchall() if r and r[0]}

    cur.execute(
        """
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'dbo' AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
    )
    tables = [str(r[0]) for r in cur.fetchall() if r and r[0]]

    cur.execute(
        """
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_SCHEMA = 'dbo'
        ORDER BY TABLE_NAME
        """
    )
    views = [str(r[0]) for r in cur.fetchall() if r and r[0]]

    out: Dict[str, SqlObject] = {}
    for t in tables:
        out[t] = SqlObject(name=t, obj_type="table", row_count=table_counts.get(t, 0))
    for v in views:
        out[v] = SqlObject(name=v, obj_type="view", row_count=None)

    # Synonyms (materialization uses dbo.mes_metrics_current)
    cur.execute(
        """
        SELECT name
        FROM sys.synonyms
        WHERE schema_id = SCHEMA_ID('dbo')
        ORDER BY name
        """
    )
    for r in cur.fetchall():
        if r and r[0]:
            out[str(r[0])] = SqlObject(name=str(r[0]), obj_type="synonym", row_count=None)

    return out


def _scan_repo_for_table_refs(
    files: Iterable[Path],
    sql_object_names: Iterable[str],
) -> Tuple[Dict[str, Set[Path]], Dict[Path, Set[str]], List[Path]]:
    """Return (obj_name->files, file->obj_names, sqlite_like_files).

    Important: to avoid false positives, we only match names that actually exist
    in SQL Server metadata (tables/views/synonyms).
    """
    table_to_files: Dict[str, Set[Path]] = {}
    file_to_tables: Dict[Path, Set[str]] = {}
    sqlite_like: List[Path] = []

    # Precompile patterns for actual SQL objects
    patterns: List[Tuple[str, re.Pattern]] = []
    for name in sorted(set(sql_object_names), key=lambda x: str(x).lower()):
        n = str(name).strip()
        if not n:
            continue
        # match either `name` or `dbo.name`
        pat = re.compile(rf"\b(dbo\.)?{re.escape(n)}\b", re.IGNORECASE)
        patterns.append((n, pat))

    for p in files:
        try:
            text = p.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        hits: Set[str] = set()

        for obj_name, pat in patterns:
            if pat.search(text):
                hits.add(obj_name)

        if hits:
            file_to_tables[p] = hits
            for t in hits:
                table_to_files.setdefault(t, set()).add(p)

        # Heuristic: still SQLite/Dual-based
        if (
            "sqlite3" in text
            or "mddap_v2.db" in text
            or "DatabaseManager" in text
            or "DualDatabaseManager" in text
        ):
            sqlite_like.append(p)

    return table_to_files, file_to_tables, sqlite_like


def _iter_candidate_files() -> List[Path]:
    patterns = [
        PROJECT_ROOT / "data_pipelines",
        PROJECT_ROOT / "scripts",
    ]

    out: List[Path] = []
    for base in patterns:
        if not base.exists():
            continue
        for p in base.rglob("*.py"):
            # skip caches
            if "__pycache__" in p.parts:
                continue
            out.append(p)
    return out


def _fmt_path(p: Path) -> str:
    try:
        return str(p.relative_to(PROJECT_ROOT))
    except Exception:
        return str(p)


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare SQL Server dbo objects vs ETL/script table references")
    parser.add_argument("--sql-server", type=str, default=r"localhost\SQLEXPRESS")
    parser.add_argument("--sql-database", type=str, default="mddap_v2")
    parser.add_argument("--driver", type=str, default="ODBC Driver 17 for SQL Server")
    args = parser.parse_args()

    conn = pyodbc.connect(_conn_str(args.sql_server, args.sql_database, args.driver))
    try:
        sql_objs = _fetch_sql_objects(conn)
    finally:
        conn.close()

    candidates = _iter_candidate_files()
    table_to_files, file_to_tables, sqlite_like_files = _scan_repo_for_table_refs(
        candidates,
        sql_object_names=sql_objs.keys(),
    )

    sql_names_lower = {k.lower(): k for k in sql_objs.keys()}

    referenced = {t.lower(): t for t in table_to_files.keys()}

    # 1) SQL exists but never referenced
    sql_not_referenced = [
        sql_objs[name]
        for name in sql_objs
        if name.lower() not in referenced
    ]

    # 2) Referenced but missing in SQL
    # With the "match only real SQL objects" strategy, this should be empty by design.
    referenced_missing: List[str] = []

    # 3) Tables with 0 rows
    zero_rows = sorted(
        [o for o in sql_objs.values() if o.obj_type == "table" and (o.row_count or 0) == 0],
        key=lambda x: x.name.lower(),
    )

    print("=" * 90)
    print("SQL Server vs ETL 引用对照报告")
    print("=" * 90)
    print(f"SQL Server: {args.sql_server} / DB: {args.sql_database}")
    print(f"Scanned python files: {len(candidates)}")
    print("-")
    print(f"SQL objects: tables={sum(1 for o in sql_objs.values() if o.obj_type=='table')}, views={sum(1 for o in sql_objs.values() if o.obj_type=='view')}")
    print(f"Referenced table/view tokens in code: {len(table_to_files)}")

    print("\n" + "=" * 90)
    print("A) SQL Server 存在但代码未引用（可疑多余/历史遗留候选）")
    print("=" * 90)
    if not sql_not_referenced:
        print("(none)")
    else:
        # show small tables first
        def _sort_key(o: SqlObject):
            rc = o.row_count if o.row_count is not None else -1
            return (0 if o.obj_type == "table" else 1, rc, o.name.lower())

        for o in sorted(sql_not_referenced, key=_sort_key):
            rc = "-" if o.row_count is None else f"{o.row_count:,}"
            print(f"- {o.obj_type}: dbo.{o.name} rows={rc}")

    print("\n" + "=" * 90)
    print("B) 代码引用但 SQL Server 中不存在（ETL/命名/迁移可能缺失）")
    print("=" * 90)
    if not referenced_missing:
        print("(none)")
    else:
        for t in referenced_missing:
            files = sorted((_fmt_path(p) for p in table_to_files.get(t, set())), key=str.lower)
            print(f"- {t} (refs={len(files)}):")
            for fp in files[:10]:
                print(f"    - {fp}")
            if len(files) > 10:
                print(f"    - ... ({len(files)-10} more)")

    print("\n" + "=" * 90)
    print("C) SQL Server 表存在但行数为 0（可能 ETL 没跑/写入目标不对）")
    print("=" * 90)
    if not zero_rows:
        print("(none)")
    else:
        for o in zero_rows:
            refs = table_to_files.get(o.name, set()) | table_to_files.get(o.name.lower(), set())
            print(f"- dbo.{o.name} rows=0 refs_in_code={len(refs)}")
            for fp in sorted((_fmt_path(p) for p in refs), key=str.lower)[:8]:
                print(f"    - {fp}")

    print("\n" + "=" * 90)
    print("D) 仍明显依赖 SQLite/DualDatabaseManager 的脚本（需要确认是否已纯 SQL Server 写入）")
    print("=" * 90)
    # prioritize ETL folders
    sqlite_like_files_sorted = sorted(sqlite_like_files, key=lambda p: _fmt_path(p).lower())
    if not sqlite_like_files_sorted:
        print("(none)")
    else:
        # show only the most relevant folders
        keep_prefixes = (
            "data_pipelines/sources/",
            "data_pipelines/monitoring/",
            "scripts/",
        )
        shown = 0
        for p in sqlite_like_files_sorted:
            rp = _fmt_path(p)
            if not rp.replace("\\", "/").startswith(keep_prefixes):
                continue
            print(f"- {rp}")
            shown += 1
            if shown >= 60:
                break
        if len(sqlite_like_files_sorted) > shown:
            print(f"- ... ({len(sqlite_like_files_sorted)-shown} more)")

    print("\n" + "=" * 90)
    print("E) 重点关注（现状推断）")
    print("=" * 90)
    print("- 如果某 raw_/dim_ 表在 SQL Server 行数为 0，同时对应 ETL 脚本仍在用 Dual/SQLite 状态逻辑，通常意味着：")
    print("  - ETL 可能写入 SQL Server 失败被吞掉；或")
    print("  - ETL 仍以 SQLite 为主，SQL Server 只是 best-effort；或")
    print("  - 实际写入表名与 SQL Server 表名不一致（大小写/前缀/历史表）")
    print("-")
    print("下一步建议：对每个 raw_ 表执行一次对应 ETL，并在 SQL Server 侧检查行数变化，以确认写入链路。")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
