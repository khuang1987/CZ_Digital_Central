
import pyodbc
import pandas as pd
import sys

args = sys.argv
table_name = args[1] if len(args) > 1 else 'raw_mes'

CONN_STR = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost\SQLEXPRESS;"
    r"DATABASE=mddap_v2;"
    r"Trusted_Connection=yes;"
)

def inspect():
    conn = pyodbc.connect(CONN_STR)
    query = f"SELECT TOP 5 * FROM {table_name}"
    try:
        df = pd.read_sql(query, conn)
        with open('table_info.txt', 'w', encoding='utf-8') as f:
            f.write(f"--- Columns in {table_name} ---\n")
            for col in df.columns:
                f.write(f"- {col}\n")
            f.write("\n--- Sample Data ---\n")
            f.write(df.head().to_string())
        print("Analysis written to table_info.txt")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    inspect()
