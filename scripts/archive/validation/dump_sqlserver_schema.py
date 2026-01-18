import argparse

import pyodbc


def build_conn_str(server: str, database: str, driver: str) -> str:
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )


def dump_table(cur, table: str, schema: str) -> None:
    cur.execute(
        """
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """,
        (schema, table),
    )
    rows = cur.fetchall()
    print(f"\n== {schema}.{table} ==")
    print(f"columns: {len(rows)}")
    for r in rows:
        name, dtype, nullable, maxlen = r
        extra = ""
        if maxlen is not None:
            extra = f"({maxlen})"
        print(f"- {name}: {dtype}{extra} null={nullable}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Dump SQL Server table schema (INFORMATION_SCHEMA.COLUMNS)")
    parser.add_argument("--sql-server", type=str, default=r"localhost\SQLEXPRESS")
    parser.add_argument("--sql-database", type=str, default="mddap_v2")
    parser.add_argument("--driver", type=str, default="ODBC Driver 17 for SQL Server")
    parser.add_argument("--schema", type=str, default="dbo")
    parser.add_argument(
        "--tables",
        nargs="+",
        default=["raw_mes", "planner_tasks", "planner_task_labels"],
    )

    args = parser.parse_args()

    conn = pyodbc.connect(build_conn_str(args.sql_server, args.sql_database, args.driver))
    try:
        cur = conn.cursor()
        for t in args.tables:
            dump_table(cur, t, args.schema)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
