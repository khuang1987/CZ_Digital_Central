"""
SQL Server Only Database Manager
Writes only to SQL Server, no SQLite dependency.
"""

import logging
import math
import numbers
import os
import pyodbc
import pandas as pd
import re
from typing import Dict, List, Any, Optional, Tuple, Iterable, Set
from datetime import datetime

# logging.basicConfig removed to allow consumer scripts to configure logging

class SQLServerOnlyManager:
    """Database Manager that writes only to SQL Server"""
    
    def __init__(self, 
                 sql_server: str = None, 
                 sql_db: str = None,
                 driver: str = None,
                 sql_user: str = None,
                 sql_password: str = None):
        
        # Load defaults from environment if not provided
        self.sql_server = sql_server or os.getenv("MDDAP_SQL_SERVER", r"localhost\SQLEXPRESS")
        self.sql_db = sql_db or os.getenv("MDDAP_SQL_DATABASE", "mddap_v2")
        self.driver = driver or os.getenv("MDDAP_SQL_DRIVER", "ODBC Driver 17 for SQL Server")
        self.sql_user = sql_user or os.getenv("MDDAP_SQL_USER", "")
        self.sql_password = sql_password or os.getenv("MDDAP_SQL_PASSWORD", "")
        
        self._columns_cache: Dict[Tuple[str, str], set] = {}
        self._build_connection_string()

    def _build_connection_string(self):
        base_conn = (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.sql_server};"
            f"DATABASE={self.sql_db};"
        )
        
        if self.sql_user and self.sql_password:
            # SQL Authentication
            auth_part = f"UID={self.sql_user};PWD={self.sql_password};"
        else:
            # Windows Authentication
            auth_part = "Trusted_Connection=yes;"
            
        self.connection_string = (
            f"{base_conn}"
            f"{auth_part}"
            "Encrypt=no;"
            "TrustServerCertificate=no;"
        )

    def get_connection(self):
        """Get SQL Server connection"""
        try:
            return pyodbc.connect(self.connection_string, autocommit=False, timeout=30)
        except pyodbc.Error as e:
            msg = str(e)
            # Only try fallback for localhost strings
            fallback_candidates = {
                r"localhost\SQLEXPRESS",
                r".\SQLEXPRESS",
                r"(local)\SQLEXPRESS",
            }
            # Also check normalized versions (remove double backslashes for check)
            server_check = self.sql_server.replace("\\\\", "\\")
            
            if ("08001" in msg or "login timeout" in msg.lower() or "登录超时" in msg) and server_check in fallback_candidates:
                alt_server = "(local)"
                self.sql_server = alt_server
                self._build_connection_string() # Rebuild with new server
                return pyodbc.connect(self.connection_string, autocommit=False, timeout=30)
            raise

    def _clean_string(self, value: Any) -> Any:
        """Clean strings for SQL Server"""
        if isinstance(value, str):
            return ''.join(ch for ch in value if ord(ch) >= 32 or ch in '\t\n\r')
        return value

    def _clean_param_value(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            v = value.strip()
            if v == "" or v.lower() in {"null", "none", "nan"}:
                return None
            return self._clean_string(v)
        # Handle float values: check for NaN, Inf, and convert to None if invalid
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return int(value)
        if isinstance(value, float):
            try:
                if not math.isfinite(float(value)):
                    return None
                return float(value)
            except (ValueError, TypeError, OverflowError):
                return None
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        return self._clean_string(value)

    def init_database(self, schema_path: str) -> bool:
        """Initialize SQL Server database schema"""
        if not os.path.exists(schema_path):
            logging.error(f"Schema file not found: {schema_path}")
            return False
        
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema_sql = f.read()
        
        # Convert SQLite schema to SQL Server (basic conversion)
        # This is simplified - you may need to adjust based on actual schema
        schema_sql = schema_sql.replace('INTEGER PRIMARY KEY AUTOINCREMENT', 'INT IDENTITY(1,1) PRIMARY KEY')
        schema_sql = schema_sql.replace('AUTOINCREMENT', 'IDENTITY(1,1)')
        schema_sql = schema_sql.replace('INTEGER PRIMARY KEY', 'INT PRIMARY KEY')
        schema_sql = schema_sql.replace('DATETIME DEFAULT CURRENT_TIMESTAMP', 'DATETIME DEFAULT GETDATE()')
        
        def _split_sql_statements(sql_text: str) -> List[str]:
            """Split SQL by semicolons, but keep semicolons inside string literals and comments."""
            statements: List[str] = []
            buf: List[str] = []

            in_single_quote = False
            in_double_quote = False
            in_line_comment = False
            in_block_comment = False

            i = 0
            n = len(sql_text)
            while i < n:
                ch = sql_text[i]
                nxt = sql_text[i + 1] if i + 1 < n else ''

                if in_line_comment:
                    buf.append(ch)
                    if ch == '\n':
                        in_line_comment = False
                    i += 1
                    continue

                if in_block_comment:
                    buf.append(ch)
                    if ch == '*' and nxt == '/':
                        buf.append(nxt)
                        i += 2
                        in_block_comment = False
                        continue
                    i += 1
                    continue

                if in_single_quote:
                    buf.append(ch)
                    if ch == "'":
                        if nxt == "'":
                            # Escaped quote
                            buf.append(nxt)
                            i += 2
                            continue
                        in_single_quote = False
                    i += 1
                    continue

                if in_double_quote:
                    buf.append(ch)
                    if ch == '"':
                        in_double_quote = False
                    i += 1
                    continue

                if ch == '-' and nxt == '-':
                    in_line_comment = True
                    buf.append(ch)
                    buf.append(nxt)
                    i += 2
                    continue

                if ch == '/' and nxt == '*':
                    in_block_comment = True
                    buf.append(ch)
                    buf.append(nxt)
                    i += 2
                    continue

                if ch == "'":
                    in_single_quote = True
                    buf.append(ch)
                    i += 1
                    continue

                if ch == '"':
                    in_double_quote = True
                    buf.append(ch)
                    i += 1
                    continue

                if ch == ';':
                    buf.append(ch)
                    stmt = ''.join(buf).strip()
                    if stmt:
                        statements.append(stmt)
                    buf = []
                    i += 1
                    continue

                buf.append(ch)
                i += 1

            tail = ''.join(buf).strip()
            if tail:
                statements.append(tail)
            return statements

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                for statement in _split_sql_statements(schema_sql):
                    statement = statement.strip()
                    # Filter out comment-only lines to ensure startswith checks against actual SQL
                    lines = [line for line in statement.split('\n') if not line.strip().startswith('--')]
                    statement_clean = '\n'.join(lines).strip()

                    if statement_clean:
                        try:
                            cursor.execute(statement_clean)
                        except Exception as e:
                            # Ignore "already exists" errors
                            if 'already exists' not in str(e).lower():
                                logging.warning(f"Schema statement warning: {e}")
                conn.commit()
                logging.info(f"SQL Server database initialized: {self.sql_db}")
                return True
        except Exception as e:
            logging.error(f"Failed to initialize SQL Server: {e}")
            return False

    def bulk_insert_pandas(self, df: pd.DataFrame, table_name: str, if_exists: str = "append") -> int:
        """Use pandas to_sql for bulk insert (handles type conversion automatically)"""
        if df is None or df.empty:
            return 0
        
        from sqlalchemy import create_engine
        from urllib.parse import quote_plus
        
        # Create SQLAlchemy engine
        conn_str = (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.sql_server};"
            f"DATABASE={self.sql_db};"
            "Trusted_Connection=yes;"
            "Encrypt=no;"
        )
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}")
        
        try:
            # Use pandas to_sql which handles type conversion
            rows_inserted = df.to_sql(
                table_name, 
                engine, 
                if_exists=if_exists, 
                index=False,
                method='multi',
                chunksize=1000
            )
            logging.info(f"Inserted {len(df)} rows into {table_name}")
            return len(df)
        except Exception as e:
            logging.error(f"Pandas bulk insert failed for {table_name}: {e}")
            raise
        finally:
            engine.dispose()
    
    def bulk_insert(self, df: pd.DataFrame, table_name: str, if_exists: str = "append") -> int:
        """Bulk insert DataFrame into SQL Server table"""
        if df is None or df.empty:
            return 0
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                try:
                    cursor.fast_executemany = True
                except Exception:
                    pass
                
                # Get table columns in order
                cursor.execute(f"""
                    SELECT COLUMN_NAME, DATA_TYPE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = 'dbo'
                    AND COLUMN_NAME != 'id'
                    ORDER BY ORDINAL_POSITION
                """)
                table_cols = [(row[0], row[1]) for row in cursor.fetchall()]
                
                # Filter DataFrame to only include columns that exist in table
                valid_cols = [col_name for col_name, _ in table_cols if col_name in df.columns]
                df_insert = df[valid_cols].copy()
                
                # Clean data and convert numpy types to Python native types
                for col_name, sql_type in table_cols:
                    if col_name not in df_insert.columns:
                        continue

                    sql_type_norm = (sql_type or '').lower()
                    
                    if sql_type_norm in ['float', 'real', 'decimal', 'numeric', 'money', 'smallmoney']:
                        # Convert to Python float, replace NaN/Inf/invalid strings with None
                        def _to_float(v: Any) -> Optional[float]:
                            if v is None:
                                return None
                            if isinstance(v, str) and v.strip() == "":
                                return None
                            try:
                                if pd.isna(v):
                                    return None
                            except Exception:
                                pass
                            try:
                                fv = float(v)
                            except Exception:
                                return None
                            if not math.isfinite(fv):
                                return None
                            return fv

                        df_insert[col_name] = df_insert[col_name].apply(_to_float)
                    elif sql_type_norm in ['int', 'bigint', 'smallint']:
                        # Convert to Python int, replace NaN/blank with None
                        def _to_int(v: Any) -> Optional[int]:
                            if v is None:
                                return None
                            if isinstance(v, str) and v.strip() == "":
                                return None
                            try:
                                if pd.isna(v):
                                    return None
                            except Exception:
                                pass
                            try:
                                # allow values like 12345.0 / '12345.0'
                                return int(float(v))
                            except Exception:
                                return None

                        df_insert[col_name] = df_insert[col_name].apply(_to_int)
                    elif sql_type_norm in ['bit']:
                         # Convert to 0 or 1
                        def _to_bit(v: Any) -> Optional[int]:
                            if v is None:
                                return None
                            if isinstance(v, str) and v.strip() == "":
                                return None
                            try:
                                if pd.isna(v):
                                    return None
                            except Exception:
                                pass
                            try:
                                fv = float(v)
                                return 1 if fv != 0 else 0
                            except Exception:
                                pass
                            if isinstance(v, str):
                                if v.lower() == 'true': return 1
                                if v.lower() == 'false': return 0
                            return None
                            
                        df_insert[col_name] = df_insert[col_name].apply(_to_bit)
                    elif sql_type_norm in ['datetime', 'datetime2']:
                        # Keep datetime as is
                        pass
                    else:
                        # For string types, convert to Python str, replace NaN with None
                        key_cols = {"Operation", "Plant", "Group"}

                        def _to_str(v: Any) -> Optional[str]:
                            try:
                                if pd.isna(v):
                                    return None
                            except Exception:
                                pass

                            if v is None:
                                return None

                            # Special-case numeric join keys so they never become '80.0' in NVARCHAR columns.
                            if col_name in key_cols and isinstance(v, numbers.Number) and not isinstance(v, bool):
                                try:
                                    fv = float(v)
                                    if math.isfinite(fv) and fv == round(fv):
                                        return str(int(round(fv)))
                                except Exception:
                                    pass

                            if isinstance(v, str):
                                s = v.strip()
                                if s == "" or s.lower() in {"null", "none", "nan"}:
                                    return None
                                if col_name in key_cols:
                                    s = re.sub(r"\\.0$", "", s)
                                return s

                            return str(v)

                        df_insert[col_name] = df_insert[col_name].apply(_to_str)
                
                # Build INSERT statement
                placeholders = ','.join(['?' for _ in valid_cols])
                col_names = ','.join([f'[{c}]' for c in valid_cols])
                insert_sql = f"INSERT INTO dbo.{table_name} ({col_names}) VALUES ({placeholders})"
                
                # Insert in smaller batches with error handling
                batch_size = 5000
                commit_every_rows = 50000
                rows_inserted = 0
                rows_since_commit = 0
                failed_batches = 0
                
                for i in range(0, len(df_insert), batch_size):
                    batch = df_insert.iloc[i:i+batch_size]
                    
                    try:
                        batch_rows = [
                            [self._clean_param_value(v) for v in row]
                            for row in batch.itertuples(index=False, name=None)
                        ]
                        cursor.executemany(insert_sql, batch_rows)
                        rows_inserted += len(batch)
                        rows_since_commit += len(batch)

                        if rows_since_commit >= commit_every_rows:
                            conn.commit()
                            rows_since_commit = 0
                        
                        if rows_inserted % 10000 == 0:
                            logging.info(f"Inserted {rows_inserted}/{len(df_insert)} rows...")
                    
                    except Exception as batch_error:
                        # Log error and try inserting rows one by one
                        logging.warning(f"Batch insert failed at row {i}, trying row-by-row: {batch_error}")
                        try:
                            err_s = str(batch_error)
                            m = re.search(r"(?:参数|Parameter)\s+(\d+)", err_s, flags=re.IGNORECASE)
                            if m:
                                param_idx = int(m.group(1)) - 1
                                if 0 <= param_idx < len(valid_cols):
                                    bad_col = valid_cols[param_idx]
                                    logging.warning(
                                        f"Batch insert error references param {param_idx+1}; mapped column: {bad_col}"
                                    )
                                    try:
                                        sample_vals = (
                                            batch[bad_col]
                                            .dropna()
                                            .astype(str)
                                            .unique()
                                            .tolist()[:10]
                                        )
                                        logging.warning(f"Sample values for {bad_col}: {sample_vals}")
                                    except Exception:
                                        pass
                                else:
                                    logging.warning(
                                        f"Batch insert error references param {param_idx+1}, but only {len(valid_cols)} columns are being inserted."
                                    )
                                    logging.warning(f"Insert columns (order): {valid_cols[:40]}")
                        except Exception:
                            pass
                        failed_batches += 1
                        # Reset transaction state to avoid later commit being rolled back
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        
                        for j, row in batch.iterrows():
                            try:
                                row_params = [self._clean_param_value(v) for v in row.tolist()]
                                cursor.execute(insert_sql, row_params)
                                rows_inserted += 1
                                rows_since_commit += 1
                            except Exception as row_error:
                                logging.error(f"Failed to insert row {j}: {row_error}")
                                try:
                                    err_s2 = str(row_error)
                                    m2 = re.search(r"(?:参数|Parameter)\s+(\d+)", err_s2, flags=re.IGNORECASE)
                                    if m2:
                                        param_idx2 = int(m2.group(1)) - 1
                                        if 0 <= param_idx2 < len(valid_cols):
                                            bad_col2 = valid_cols[param_idx2]
                                            bad_val2 = row.get(bad_col2, None)
                                            logging.error(f"Row error references param {param_idx2+1}; column: {bad_col2}; value: {bad_val2}")
                                        else:
                                            logging.error(
                                                f"Row error references param {param_idx2+1}, but only {len(valid_cols)} columns are being inserted."
                                            )
                                            logging.error(f"Insert columns (order): {valid_cols[:40]}")
                                except Exception:
                                    pass
                                logging.debug(f"Problematic row data: {row.to_dict()}")

                        # Commit what we managed to insert in this fallback batch
                        try:
                            conn.commit()
                            rows_since_commit = 0
                        except Exception as commit_error:
                            logging.error(f"Commit failed after row-by-row fallback: {commit_error}")
                            raise
                
                # Final commit
                if rows_since_commit > 0:
                    conn.commit()
                    rows_since_commit = 0
                logging.info(f"Inserted {rows_inserted} rows into {table_name} (failed batches: {failed_batches})")
                return rows_inserted
                
        except Exception as e:
            logging.error(f"Bulk insert failed for {table_name}: {e}")
            raise

    def mark_file_processed(self, etl_name: str, file_path: str) -> None:
        """Mark file as processed in etl_file_state table"""
        # Normalize path
        file_path = os.path.normpath(os.path.abspath(file_path))
        
        file_mtime = os.path.getmtime(file_path)
        file_size = os.path.getsize(file_path)
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Check if record exists
                cursor.execute("""
                    SELECT COUNT(*) FROM dbo.etl_file_state 
                    WHERE etl_name = ? AND file_path = ?
                """, (etl_name, file_path))
                
                exists = cursor.fetchone()[0] > 0
                
                if exists:
                    cursor.execute("""
                        UPDATE dbo.etl_file_state 
                        SET file_mtime = ?, file_size = ?, processed_time = GETDATE(), updated_at = GETDATE()
                        WHERE etl_name = ? AND file_path = ?
                    """, (file_mtime, file_size, etl_name, file_path))
                else:
                    cursor.execute("""
                        INSERT INTO dbo.etl_file_state 
                        (etl_name, file_path, file_mtime, file_size, processed_time, created_at, updated_at)
                        VALUES (?, ?, ?, ?, GETDATE(), GETDATE(), GETDATE())
                    """, (etl_name, file_path, file_mtime, file_size))
                
                conn.commit()
                logging.info(f"Marked processed: {os.path.basename(file_path)} (mtime={file_mtime}, size={file_size})")
        except Exception as e:
            logging.error(f"Failed to mark file processed: {e}")
            raise

    def filter_changed_files(self, etl_name: str, file_paths: List[str]) -> List[str]:
        """Filter to only files that have changed since last processing"""
        changed_files = []
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                for raw_path in file_paths:
                    if not os.path.exists(raw_path):
                        logging.warning(f"File not found: {raw_path}")
                        continue
                    
                    # Normalize path
                    file_path = os.path.normpath(os.path.abspath(raw_path))
                    
                    current_mtime = os.path.getmtime(file_path)
                    current_size = os.path.getsize(file_path)
                    
                    cursor.execute("""
                        SELECT file_mtime, file_size 
                        FROM dbo.etl_file_state 
                        WHERE etl_name = ? AND file_path = ?
                    """, (etl_name, file_path))
                    
                    row = cursor.fetchone()
                    
                    if row:
                        db_mtime, db_size = row[0], row[1]
                        # Check if file has changed (different mtime or size)
                        # Note: db_mtime is float (was real), check tolerance
                        mtime_diff = abs(current_mtime - (db_mtime or 0))
                        size_match = (current_size == db_size)
                        
                        if db_mtime is not None and db_size is not None:
                            if mtime_diff < 1.0 and size_match:
                                # logging.debug(f"Skipping unchanged file: {os.path.basename(file_path)}")
                                continue
                            else:
                                logging.info(f"File changed: {os.path.basename(file_path)} | DB: mtime={db_mtime}, size={db_size} | FS: mtime={current_mtime}, size={current_size} | Diff: {mtime_diff}")
                        else:
                             logging.info(f"File state incomplete in DB: {os.path.basename(file_path)}")
                    else:
                        logging.info(f"New file found: {os.path.basename(file_path)}")
                    
                    changed_files.append(raw_path) # Return original path to avoid confusion in caller
                    
        except Exception as e:
            logging.error(f"Failed to filter changed files: {e}")
            return file_paths  # Return all files on error
        
        return changed_files

    def get_table_count(self, table_name: str, schema: str = "dbo") -> int:
        """Get row count for a table."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(1) FROM {schema}.[{table_name}]")
            row = cursor.fetchone()
            return int(row[0]) if row and row[0] is not None else 0

    def get_existing_hashes(
        self,
        table_name: str,
        hash_column: str = "record_hash",
        hashes: Optional[Iterable[str]] = None,
        schema: str = "dbo",
        batch_size: int = 800,
    ) -> Set[str]:
        """Get existing hashes from a table.

        If `hashes` is provided, performs batched `IN (...)` queries to avoid scanning
        the whole table.
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()

            if hashes is None:
                cursor.execute(f"SELECT [{hash_column}] FROM {schema}.[{table_name}] WHERE [{hash_column}] IS NOT NULL")
                return {str(r[0]) for r in cursor.fetchall() if r and r[0] is not None}

            uniq = [h for h in dict.fromkeys([str(x) for x in hashes if x is not None and str(x) != ""]).keys()]
            if not uniq:
                return set()

            existing: Set[str] = set()
            for i in range(0, len(uniq), batch_size):
                batch = uniq[i : i + batch_size]
                placeholders = ",".join(["?"] * len(batch))
                sql = (
                    f"SELECT [{hash_column}] FROM {schema}.[{table_name}] "
                    f"WHERE [{hash_column}] IN ({placeholders})"
                )
                cursor.execute(sql, batch)
                existing.update({str(r[0]) for r in cursor.fetchall() if r and r[0] is not None})
            return existing

    def merge_insert_by_hash(
        self,
        df: pd.DataFrame,
        table_name: str,
        hash_column: str = "record_hash",
        schema: str = "dbo",
        staging_table_name: Optional[str] = None,
    ) -> int:
        if df is None or df.empty:
            return 0

        if hash_column not in df.columns:
            raise ValueError(f"Missing required hash column: {hash_column}")

        staging_table_name = staging_table_name or f"_stg_{table_name}"

        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Optional optimization: create an index on the hash column when possible.
            # In SQL Server, types like NVARCHAR(MAX)/TEXT cannot be indexed as key columns.
            try:
                cursor.execute(
                    """
                    SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?
                    """,
                    (schema, table_name, hash_column),
                )
                col_info = cursor.fetchone()
                data_type = (col_info[0] or "").lower() if col_info else ""
                max_len = col_info[1] if col_info else None

                indexable = True
                if data_type in {"text", "ntext", "image"}:
                    indexable = False
                if data_type in {"varchar", "nvarchar", "varbinary"} and (max_len == -1 or max_len is None):
                    indexable = False

                if indexable:
                    cursor.execute(
                        """
                        SELECT 1
                        FROM sys.indexes i
                        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                        WHERE i.object_id = OBJECT_ID(?)
                          AND c.name = ?
                        """,
                        (f"{schema}.{table_name}", hash_column),
                    )
                    has_any_index = cursor.fetchone() is not None
                    if not has_any_index:
                        idx_name = f"idx_{table_name}_{hash_column}"
                        cursor.execute(
                            f"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = ? AND object_id = OBJECT_ID(?)) "
                            f"CREATE INDEX [{idx_name}] ON {schema}.[{table_name}] ([{hash_column}]);",
                            (idx_name, f"{schema}.{table_name}"),
                        )
                        conn.commit()
            except Exception as e:
                logging.warning(f"Skip creating index on {schema}.{table_name}.{hash_column}: {e}")

            cursor.execute(
                f"""
                IF OBJECT_ID(?, 'U') IS NULL
                BEGIN
                    SELECT TOP 0 *
                    INTO {schema}.[{staging_table_name}]
                    FROM {schema}.[{table_name}] WHERE 1=0;
                END
                """,
                (f"{schema}.{staging_table_name}",),
            )
            conn.commit()

            cursor.execute(f"TRUNCATE TABLE {schema}.[{staging_table_name}]")
            conn.commit()

        self.bulk_insert(df, staging_table_name, if_exists="append")

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?
                AND COLUMN_NAME != 'id'
                ORDER BY ORDINAL_POSITION
                """,
                (table_name, schema),
            )
            table_cols = [r[0] for r in cursor.fetchall()]
            insert_cols = [c for c in table_cols if c in df.columns]
            if hash_column not in insert_cols:
                insert_cols.append(hash_column)

            col_list = ",".join([f"[{c}]" for c in insert_cols])
            src_list = ",".join([f"S.[{c}]" for c in insert_cols])

            insert_sql = f"""
            SET NOCOUNT ON;

            INSERT INTO {schema}.[{table_name}] ({col_list})
            SELECT {src_list}
            FROM {schema}.[{staging_table_name}] AS S
            WHERE NOT EXISTS (
                SELECT 1
                FROM {schema}.[{table_name}] AS T
                WHERE T.[{hash_column}] = S.[{hash_column}]
            );

            SELECT @@ROWCOUNT AS inserted_count;
            """

            cursor.execute(insert_sql)

            inserted = 0
            while True:
                try:
                    row = cursor.fetchone()
                    if row is not None:
                        inserted = int(row[0]) if row[0] is not None else 0
                        break
                except pyodbc.ProgrammingError:
                    pass

                try:
                    has_next = cursor.nextset()
                except Exception:
                    has_next = False
                if not has_next:
                    break

            cursor.execute(f"TRUNCATE TABLE {schema}.[{staging_table_name}]")
            conn.commit()
            return inserted

    def log_etl_run(
        self,
        etl_name: str,
        status: str,
        records_read: int = 0,
        records_inserted: int = 0,
        records_updated: int = 0,
        records_skipped: int = 0,
        error_message: Optional[str] = None,
        schema: str = "dbo",
    ) -> None:
        """Insert an ETL run log record into dbo.etl_run_log."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                INSERT INTO {schema}.etl_run_log
                    (etl_name, run_start, run_end, status,
                     records_read, records_inserted, records_updated, records_skipped, error_message, created_at)
                VALUES
                    (?, GETDATE(), GETDATE(), ?, ?, ?, ?, ?, ?, GETDATE())
                """,
                (
                    etl_name,
                    status,
                    int(records_read or 0),
                    int(records_inserted or 0),
                    int(records_updated or 0),
                    int(records_skipped or 0),
                    error_message,
                ),
            )
            conn.commit()
