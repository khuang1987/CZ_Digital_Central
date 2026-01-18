"""
Dual Database Manager
Wrapper around DatabaseManager to support simultaneous writes to SQLite and SQL Server.
"""

import logging
import math
import os
import pyodbc
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from .db_utils import DatabaseManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DualDatabaseManager:
    """
    Dual Database Manager that writes to both SQLite and SQL Server.
    Read operations typically default to SQLite for now unless specified.
    """
    
    def __init__(self, sqlite_db_path: str, 
                 sql_server: str = r"localhost\SQLEXPRESS", 
                 sql_db: str = "mddap_v2",
                 driver: str = "ODBC Driver 17 for SQL Server"):
        """
        Initialize DualDatabaseManager.
        
        Args:
            sqlite_db_path: Path to SQLite DB file
            sql_server: SQL Server instance name
            sql_db: SQL Server database name
            driver: ODBC driver name
        """
        self.sqlite_manager = DatabaseManager(sqlite_db_path)
        self.sqlite_db_path = sqlite_db_path
        
        # SQL Server configuration
        self.sql_server = sql_server
        self.sql_db = sql_db
        self.driver = driver
        self._sqlserver_columns_cache: Dict[Tuple[str, str], set] = {}
        self.sql_connection_string = (
            f"DRIVER={{{driver}}};"
            f"SERVER={sql_server};"
            f"DATABASE={sql_db};"
            "Trusted_Connection=yes;"
            "Encrypt=no;"
            "TrustServerCertificate=no;"
        )

    def _get_sql_server_connection(self):
        """Get SQL Server connection"""
        return pyodbc.connect(self.sql_connection_string, autocommit=False)

    def get_connection(self):
        """
        Return the underlying SQLite connection for backward compatibility.
        Useful for read operations or custom SQLite-specific logic.
        """
        return self.sqlite_manager.get_connection()

    def _clean_string(self, value: Any) -> Any:
        """Clean strings for SQL Server (remove null bytes etc)"""
        if isinstance(value, str):
            # Remove control characters except tab, newline, carriage return
            return ''.join(ch for ch in value if ord(ch) >= 32 or ch in '\t\n\r')
        return value

    def _clean_param_value(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, float) and not math.isfinite(value):
            return None
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        if hasattr(value, "to_pydatetime"):
            try:
                value = value.to_pydatetime()
            except Exception:
                return None
        return self._clean_string(value)

    def _sqlserver_table_exists(self, table_name: str, schema: str = "dbo") -> bool:
        with self._get_sql_server_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT 1
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                """,
                (schema, table_name),
            )
            return cur.fetchone() is not None

    def _infer_sqlserver_type(self, series: pd.Series) -> str:
        name = str(series.name or "")
        lname = name.lower()

        if lname == "id":
            return "INT IDENTITY(1,1)"

        if pd.api.types.is_bool_dtype(series):
            return "BIT"
        if pd.api.types.is_integer_dtype(series):
            return "BIGINT"
        if pd.api.types.is_float_dtype(series):
            return "FLOAT"
        if pd.api.types.is_datetime64_any_dtype(series):
            return "DATETIME2"

        if "date" in lname and "time" not in lname:
            try:
                sample = series.dropna()
                if not sample.empty:
                    s = sample.astype(str)
                    if s.str.match(r"^\d{4}-\d{2}-\d{2}$").all():
                        return "DATE"
            except Exception:
                pass

        if lname in {"a3id", "record_hash"}:
            return "NVARCHAR(128)"
        if lname.endswith("_id"):
            return "NVARCHAR(128)"
        return "NVARCHAR(MAX)"

    def _ensure_sqlserver_table_for_df(self, table_name: str, df: pd.DataFrame, schema: str = "dbo") -> None:
        if df is None:
            return

        cols = list(df.columns)
        if not cols:
            return

        with self._get_sql_server_connection() as conn:
            cur = conn.cursor()

            if not self._sqlserver_table_exists(table_name, schema=schema):
                pk_col: Optional[str] = None
                if "A3Id" in cols:
                    pk_col = "A3Id"
                elif table_name == "dim_calendar" and "date" in cols:
                    pk_col = "date"

                col_defs: List[str] = []
                for c in cols:
                    ctype = self._infer_sqlserver_type(df[c])
                    if c.lower() == "id":
                        col_defs.append(f"[{c}] {ctype} PRIMARY KEY")
                        continue
                    if pk_col is not None and c == pk_col:
                        if ctype.startswith("NVARCHAR"):
                            col_defs.append(f"[{c}] {ctype} NOT NULL PRIMARY KEY")
                        else:
                            col_defs.append(f"[{c}] {ctype} NOT NULL PRIMARY KEY")
                        continue
                    col_defs.append(f"[{c}] {ctype} NULL")

                ddl = f"CREATE TABLE [{schema}].[{table_name}] (" + ", ".join(col_defs) + ")"
                cur.execute(ddl)
                conn.commit()

            # Add missing columns if needed
            sql_cols = self._get_sqlserver_columns(table_name, schema=schema)
            missing = [c for c in cols if c not in sql_cols]
            for c in missing:
                ctype = self._infer_sqlserver_type(df[c])
                if c.lower() == "id":
                    # Can't safely add IDENTITY to an existing table; skip.
                    continue
                try:
                    cur.execute(f"ALTER TABLE [{schema}].[{table_name}] ADD [{c}] {ctype} NULL")
                except Exception as e:
                    logging.warning(f"SQL Server add column failed for {schema}.{table_name}.{c}: {e}")
            if missing:
                conn.commit()

        # Refresh cache
        self._sqlserver_columns_cache.pop((schema, table_name), None)

    def sync_dataframe_to_sqlserver(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        schema: str = "dbo",
        chunk_size: int = 1000,
    ) -> int:
        if df is None or df.empty:
            return 0

        df_converted = df.copy()
        for col in df_converted.columns:
            if pd.api.types.is_datetime64_any_dtype(df_converted[col]):
                df_converted[col] = df_converted[col].apply(
                    lambda x: x.to_pydatetime() if hasattr(x, "to_pydatetime") else (x if pd.notna(x) else None)
                )

        self._ensure_sqlserver_table_for_df(table_name, df_converted, schema=schema)
        sql_cols = self._get_sqlserver_columns(table_name, schema=schema)
        insert_cols = [c for c in df_converted.columns if c in sql_cols and c.lower() != "id"]
        if not insert_cols:
            return 0

        cols_sql = ", ".join(f"[{c}]" for c in insert_cols)
        placeholders = ", ".join(["?" for _ in insert_cols])
        insert_sql = f"INSERT INTO [{schema}].[{table_name}] ({cols_sql}) VALUES ({placeholders})"

        with self._get_sql_server_connection() as conn:
            cur = conn.cursor()
            if if_exists == "replace":
                cur.execute(f"DELETE FROM [{schema}].[{table_name}]")
                conn.commit()

            try:
                cur.fast_executemany = True
            except Exception:
                pass

            total = 0
            for i in range(0, len(df_converted), chunk_size):
                chunk = df_converted.iloc[i:i + chunk_size]
                rows = []
                for _, row in chunk.iterrows():
                    rows.append(tuple(self._clean_param_value(row[c]) for c in insert_cols))
                try:
                    cur.executemany(insert_sql, rows)
                    conn.commit()
                except Exception as e:
                    logging.warning(f"SQL Server batch sync insert failed for {table_name} rows {i}-{i+len(rows)-1}: {e}")
                    conn.rollback()
                    for rv in rows:
                        try:
                            cur.execute(insert_sql, rv)
                        except Exception as row_e:
                            logging.warning(f"SQL Server row sync insert failed for {table_name}: {row_e}")
                    conn.commit()
                total += len(rows)

        return total

    def _get_sqlserver_columns(self, table_name: str, schema: str = "dbo") -> set:
        cache_key = (schema, table_name)
        if cache_key in self._sqlserver_columns_cache:
            return self._sqlserver_columns_cache[cache_key]

        cols: set = set()
        with self._get_sql_server_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                """,
                (schema, table_name),
            )
            for row in cur.fetchall():
                if row and row[0]:
                    cols.add(str(row[0]))

        self._sqlserver_columns_cache[cache_key] = cols
        return cols

    def execute_query(self, sql: str, params: tuple = ()) -> List[Dict]:
        """
        Execute query - currently reads from SQLite by default as the source of truth
        """
        return self.sqlite_manager.execute_query(sql, params)

    def init_database(self, schema_file: str) -> bool:
        """
        Initialize database schema.
        Currently only initializes SQLite. SQL Server schema is assumed to be managed 
        via migration scripts or manual setup, as SQLite DDL is not fully compatible.
        """
        return self.sqlite_manager.init_database(schema_file)

    def execute_sql(self, sql: str, params: tuple = ()) -> int:
        """
        Execute SQL (INSERT/UPDATE/DELETE) on BOTH databases.
        Note: This expects generic SQL that works on both, or we might need separate logic.
        For complex ETL, prefer upsert_dataframe.
        """
        # 1. Execute on SQLite
        sqlite_rows = self.sqlite_manager.execute_sql(sql, params)
        
        # 2. Execute on SQL Server (Best effort)
        try:
            # Note: SQLite syntax like ? placeholders works with pyodbc too
            # But SQL syntax might differ (e.g. CURRENT_TIMESTAMP vs GETDATE())
            # For simple statements this might work, but be careful.
            with self._get_sql_server_connection() as conn:
                cursor = conn.cursor()
                # Clean params
                cleaned_params = tuple(self._clean_param_value(p) for p in params)
                cursor.execute(sql, cleaned_params)
                conn.commit()
        except Exception as e:
            logging.error(f"SQL Server write failed (execute_sql): {e}")
            
        return sqlite_rows

    def upsert_dataframe(self, df: pd.DataFrame, table_name: str, 
                         unique_column: str = "record_hash",
                         batch_size: int = 1000) -> Dict[str, int]:
        """
        Upsert DataFrame to BOTH databases.
        """
        if df.empty:
            return {"inserted": 0, "updated": 0, "skipped": 0}

        # 1. Upsert to SQLite (Primary)
        logging.info(f"Writing to SQLite table: {table_name}")
        sqlite_stats = self.sqlite_manager.upsert_dataframe(df, table_name, unique_column, batch_size)
        
        # 2. Upsert to SQL Server (Secondary)
        logging.info(f"Syncing to SQL Server table: {table_name}")
        try:
            self._upsert_sqlserver(df, table_name, unique_column, batch_size)
        except Exception as e:
            logging.error(f"SQL Server sync failed for {table_name}: {e}")
            
        return sqlite_stats

    def _upsert_sqlserver(self, df: pd.DataFrame, table_name: str, 
                          unique_column: str, batch_size: int):
        """
        Internal method to upsert DataFrame to SQL Server.
        Uses Check-Update-Insert logic since T-SQL MERGE is complex to construct dynamically.
        """
        # Convert timestamps just like in sqlite manager
        df_converted = df.copy()
        for col in df_converted.columns:
            if pd.api.types.is_datetime64_any_dtype(df_converted[col]):
                df_converted[col] = df_converted[col].apply(
                    lambda x: x.isoformat() if pd.notna(x) else None
                )
        
        columns = list(df_converted.columns)
        # Handle 'id' column: usually we don't insert 'id' if it's auto-increment, 
        # unless we are migrating. For daily ETL, we typically let DB handle ID or rely on hash.
        # DatabaseManager excludes 'id' from updates but includes it in inserts if present?
        # Actually DatabaseManager's upsert SQL includes all columns in INSERT VALUES.
        
        # SQL Server specific: If 'id' is IDENTITY, we generally cannot insert it unless IDENTITY_INSERT is ON.
        # But for new records, we want SQL Server to generate IDs independently (or sync them?)
        # For simplicity in dual-write, let SQL Server generate its own IDs. 
        # So we exclude 'id' from insert/update if it exists in DataFrame but is meant to be auto-gen.
        # Assuming standard schema where 'id' is PK IDENTITY.
        
        insert_cols = [c for c in columns if c.lower() != 'id']
        update_cols = [c for c in insert_cols if c != unique_column]

        # SQL Server table/column introspection (avoid assuming columns like updated_at exist)
        schema = "dbo"
        sql_cols = self._get_sqlserver_columns(table_name, schema=schema)
        has_updated_at = "updated_at" in sql_cols
        update_cols = [c for c in update_cols if c in sql_cols]
        insert_cols = [c for c in insert_cols if c in sql_cols]
        if unique_column not in insert_cols and unique_column in sql_cols:
            insert_cols.append(unique_column)

        cols_str = ", ".join(f"[{c}]" for c in insert_cols)
        placeholders = ", ".join(["?" for _ in insert_cols])
        
        # Update statement
        update_set = ", ".join([f"[{c}] = ?" for c in update_cols])
        if has_updated_at:
            update_set = (update_set + ", " if update_set else "") + "[updated_at] = GETDATE()"
        update_sql = f"UPDATE [{schema}].[{table_name}] SET {update_set} WHERE [{unique_column}] = ?" if update_set else f"UPDATE [{schema}].[{table_name}] SET [{unique_column}] = [{unique_column}] WHERE [{unique_column}] = ?"
        
        # Insert statement
        insert_sql = f"INSERT INTO [{schema}].[{table_name}] ({cols_str}) VALUES ({placeholders})"
        
        with self._get_sql_server_connection() as conn:
            cursor = conn.cursor()
            
            for i in range(0, len(df_converted), batch_size):
                batch = df_converted.iloc[i:i+batch_size]
                
                for _, row in batch.iterrows():
                    try:
                        # Clean values
                        row_vals = {c: self._clean_param_value(row[c]) for c in columns}
                        
                        # Try Update first
                        # Update params: values for update_cols + unique_column value
                        update_params = tuple(row_vals[c] for c in update_cols) + (row_vals.get(unique_column),)
                        cursor.execute(update_sql, update_params)
                        
                        if cursor.rowcount == 0:
                            # Insert if no row updated
                            insert_params = tuple(row_vals[c] for c in insert_cols)
                            cursor.execute(insert_sql, insert_params)
                            
                    except Exception as e:
                        logging.warning(f"SQL Server row sync error: {e}")
                        # Continue to next row
                
                conn.commit()

    def bulk_insert(self, df: pd.DataFrame, table_name: str, if_exists: str = "append") -> int:
        """Bulk insert to SQLite (primary) and best-effort insert to SQL Server (secondary)."""
        sqlite_rows = self.sqlite_manager.bulk_insert(df, table_name, if_exists=if_exists)

        try:
            if df.empty:
                return sqlite_rows

            df_converted = df.copy()
            for col in df_converted.columns:
                if pd.api.types.is_datetime64_any_dtype(df_converted[col]):
                    df_converted[col] = df_converted[col].apply(
                        lambda x: x.isoformat() if pd.notna(x) else None
                    )

            schema = "dbo"
            self._ensure_sqlserver_table_for_df(table_name, df_converted, schema=schema)
            columns = list(df_converted.columns)
            insert_cols = [c for c in columns if c.lower() != 'id']
            sql_cols = self._get_sqlserver_columns(table_name, schema=schema)
            insert_cols = [c for c in insert_cols if c in sql_cols]

            cols_sql = ", ".join(f"[{c}]" for c in insert_cols)
            placeholders = ", ".join(["?" for _ in insert_cols])
            insert_sql = f"INSERT INTO [{schema}].[{table_name}] ({cols_sql}) VALUES ({placeholders})"

            with self._get_sql_server_connection() as conn:
                cur = conn.cursor()

                if if_exists == "replace":
                    try:
                        cur.execute(f"DELETE FROM [{schema}].[{table_name}]")
                        conn.commit()
                    except Exception as e:
                        logging.warning(f"SQL Server table clear failed for {table_name}: {e}")

                try:
                    cur.fast_executemany = True
                except Exception:
                    pass

                if not insert_cols:
                    return sqlite_rows

                # Insert in chunks; on failure, fall back to per-row insert to avoid losing whole batch
                chunk_size = 1000
                all_rows = []
                for _, row in df_converted.iterrows():
                    row_vals = tuple(self._clean_param_value(row[c]) for c in insert_cols)
                    all_rows.append(row_vals)

                for i in range(0, len(all_rows), chunk_size):
                    chunk = all_rows[i:i + chunk_size]
                    try:
                        cur.executemany(insert_sql, chunk)
                        conn.commit()
                    except Exception as e:
                        logging.warning(f"SQL Server batch insert failed for {table_name} rows {i}-{i+len(chunk)-1}: {e}")
                        conn.rollback()
                        for row_vals in chunk:
                            try:
                                cur.execute(insert_sql, row_vals)
                            except Exception as row_e:
                                logging.warning(f"SQL Server row insert failed for {table_name}: {row_e}")
                        conn.commit()

        except Exception as e:
            logging.error(f"SQL Server write failed (bulk_insert) for {table_name}: {e}")

        return sqlite_rows
                
    # Delegate other methods to sqlite_manager
    def read_table(self, *args, **kwargs):
        return self.sqlite_manager.read_table(*args, **kwargs)
        
    def get_table_count(self, *args, **kwargs):
        return self.sqlite_manager.get_table_count(*args, **kwargs)
        
    def get_existing_hashes(self, *args, **kwargs):
        return self.sqlite_manager.get_existing_hashes(*args, **kwargs)
    
    # File state management - sync to both
    def is_file_changed(self, *args, **kwargs):
        return self.sqlite_manager.is_file_changed(*args, **kwargs)
        
    def filter_changed_files(self, *args, **kwargs):
        return self.sqlite_manager.filter_changed_files(*args, **kwargs)
        
    def mark_file_processed(self, etl_name: str, file_path: str) -> None:
        self.sqlite_manager.mark_file_processed(etl_name, file_path)
        # Also sync to SQL Server (best effort)
        try:
             # Logic is bit complex to duplicate exactly due to SQL differences
             # For now, rely on SQLite as state of truth for ETL state
             pass
        except Exception:
            pass
            
    def mark_files_processed(self, *args, **kwargs):
        return self.sqlite_manager.mark_files_processed(*args, **kwargs)
        
    def log_etl_run(self, *args, **kwargs):
        return self.sqlite_manager.log_etl_run(*args, **kwargs)
        
    def update_etl_run(self, *args, **kwargs):
        return self.sqlite_manager.update_etl_run(*args, **kwargs)

def get_dual_db_manager() -> DualDatabaseManager:
    """Get the standard dual DB manager instance"""
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    db_path = os.path.join(project_root, "data_pipelines", "database", "mddap_v2.db")
    return DualDatabaseManager(db_path)
