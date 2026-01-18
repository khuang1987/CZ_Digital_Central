"""Maintenance: Widen record_hash columns and (re)create indexes in SQL Server.

Purpose:
  Ensure record_hash can store longer values and improve merge/lookup performance.

Effects:
  - Alters dbo.raw_sfc_inspection.record_hash to NVARCHAR(512) NOT NULL.
  - Alters dbo._stg_raw_sfc_inspection.record_hash to NVARCHAR(512) NOT NULL (if exists).
  - Creates unique index idx_raw_sfc_inspection_record_hash (if missing).
  - Alters dbo.raw_mes.record_hash to NVARCHAR(512) NOT NULL.
  - Alters dbo._stg_raw_mes.record_hash to NVARCHAR(512) NOT NULL (if exists).
  - Creates index idx_raw_mes_record_hash (if missing).

Run:
  python scripts/maintenance/alter_record_hash_512.py
"""

import sys

import pyodbc


def main() -> int:
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=(local);"
        "DATABASE=mddap_v2;"
        "Trusted_Connection=yes;"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )

    conn = pyodbc.connect(conn_str, timeout=30)
    try:
        cur = conn.cursor()

        print("Pre-check: record_hash column")
        cur.execute(
            "SELECT TABLE_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA='dbo' "
            "  AND TABLE_NAME IN ('raw_sfc_inspection','raw_mes') "
            "  AND COLUMN_NAME='record_hash' "
            "ORDER BY TABLE_NAME"
        )
        for r in cur.fetchall():
            print("-", r[0], r[1], r[2])
        sys.stdout.flush()

        cmds = [
            # raw_sfc_inspection
            "ALTER TABLE dbo.raw_sfc_inspection ALTER COLUMN record_hash NVARCHAR(512) NOT NULL",
            "IF OBJECT_ID('dbo._stg_raw_sfc_inspection','U') IS NOT NULL "
            "ALTER TABLE dbo._stg_raw_sfc_inspection ALTER COLUMN record_hash NVARCHAR(512) NOT NULL",
            "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_raw_sfc_inspection_record_hash' "
            "AND object_id = OBJECT_ID('dbo.raw_sfc_inspection')) "
            "CREATE UNIQUE INDEX idx_raw_sfc_inspection_record_hash ON dbo.raw_sfc_inspection(record_hash)",

            # raw_mes
            "ALTER TABLE dbo.raw_mes ALTER COLUMN record_hash NVARCHAR(512) NOT NULL",
            "IF OBJECT_ID('dbo._stg_raw_mes','U') IS NOT NULL "
            "ALTER TABLE dbo._stg_raw_mes ALTER COLUMN record_hash NVARCHAR(512) NOT NULL",
            "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_raw_mes_record_hash' "
            "AND object_id = OBJECT_ID('dbo.raw_mes')) "
            "CREATE INDEX idx_raw_mes_record_hash ON dbo.raw_mes(record_hash)",
        ]

        try:
            for idx, s in enumerate(cmds, start=1):
                print(f"[{idx}/{len(cmds)}] Executing: {s}")
                sys.stdout.flush()
                cur.execute(s)
                conn.commit()
        except KeyboardInterrupt:
            try:
                conn.rollback()
            except Exception:
                pass
            print("Interrupted by user. You can re-run this script safely to continue.")
            return 130

        print("Post-check: record_hash column")
        cur.execute(
            "SELECT TABLE_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA='dbo' "
            "  AND TABLE_NAME IN ('raw_sfc_inspection','raw_mes') "
            "  AND COLUMN_NAME='record_hash' "
            "ORDER BY TABLE_NAME"
        )
        rows = cur.fetchall()
        for r in rows:
            print("-", r[0], r[1], r[2])

        cur.execute(
            "SELECT COUNT(1) FROM sys.indexes "
            "WHERE object_id=OBJECT_ID('dbo.raw_sfc_inspection') AND name='idx_raw_sfc_inspection_record_hash'"
        )
        print("idx_raw_sfc_inspection_record_hash:", cur.fetchone()[0])

        cur.execute(
            "SELECT COUNT(1) FROM sys.indexes "
            "WHERE object_id=OBJECT_ID('dbo.raw_mes') AND name='idx_raw_mes_record_hash'"
        )
        print("idx_raw_mes_record_hash:", cur.fetchone()[0])

    finally:
        conn.close()

    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
