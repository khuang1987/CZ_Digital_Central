"""
数据库工具模块
提供 SQLite 数据库连接、初始化、CRUD 操作
设计为可迁移到 PostgreSQL
"""

import sqlite3
import logging
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
from contextlib import contextmanager

import pandas as pd

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class DatabaseManager:
    """数据库管理器 - 支持 SQLite，可扩展到 PostgreSQL"""
    
    def __init__(self, db_path: str):
        """
        初始化数据库管理器
        
        Args:
            db_path: SQLite 数据库文件路径
        """
        self.db_path = db_path
        self.db_dir = os.path.dirname(db_path)
        
        # 确保目录存在
        if self.db_dir and not os.path.exists(self.db_dir):
            os.makedirs(self.db_dir)
    
    @contextmanager
    def get_connection(self):
        """获取数据库连接（上下文管理器）"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # 支持字典式访问
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def init_database(self, schema_file: str) -> bool:
        """
        初始化数据库（执行 schema SQL）
        
        Args:
            schema_file: SQL schema 文件路径
            
        Returns:
            是否成功
        """
        try:
            with open(schema_file, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            
            with self.get_connection() as conn:
                conn.executescript(schema_sql)
            
            logging.info(f"数据库初始化成功: {self.db_path}")
            return True
            
        except Exception as e:
            logging.error(f"数据库初始化失败: {e}")
            return False
    
    def execute_query(self, sql: str, params: tuple = ()) -> List[Dict]:
        """
        执行查询并返回结果
        
        Args:
            sql: SQL 查询语句
            params: 查询参数
            
        Returns:
            查询结果列表
        """
        with self.get_connection() as conn:
            cursor = conn.execute(sql, params)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
    
    def execute_sql(self, sql: str, params: tuple = ()) -> int:
        """
        执行 SQL（INSERT/UPDATE/DELETE）
        
        Args:
            sql: SQL 语句
            params: 参数
            
        Returns:
            影响的行数
        """
        with self.get_connection() as conn:
            cursor = conn.execute(sql, params)
            return cursor.rowcount
    
    def read_table(self, table_name: str, where: str = "", params: tuple = (), limit: int = None) -> pd.DataFrame:
        """
        读取表数据到 DataFrame
        
        Args:
            table_name: 表名
            where: WHERE 条件（不含 WHERE 关键字）
            params: 查询参数
            limit: 限制行数
            
        Returns:
            DataFrame
        """
        sql = f"SELECT * FROM {table_name}"
        if where:
            sql += f" WHERE {where}"
        if limit:
            sql += f" LIMIT {limit}"
        
        with self.get_connection() as conn:
            return pd.read_sql(sql, conn, params=params)
    
    def upsert_dataframe(self, df: pd.DataFrame, table_name: str, 
                         unique_column: str = "record_hash",
                         batch_size: int = 1000) -> Dict[str, int]:
        """
        将 DataFrame 数据 UPSERT 到数据库
        
        Args:
            df: 数据 DataFrame
            table_name: 目标表名
            unique_column: 唯一键列名（用于冲突检测）
            batch_size: 批量插入大小
            
        Returns:
            统计信息 {inserted, updated, skipped}
        """
        if df.empty:
            return {"inserted": 0, "updated": 0, "skipped": 0}
        
        stats = {"inserted": 0, "updated": 0, "skipped": 0}
        
        # 转换 pandas Timestamp 为 Python 原生类型（SQLite 兼容）
        df_converted = df.copy()
        for col in df_converted.columns:
            if pd.api.types.is_datetime64_any_dtype(df_converted[col]):
                # 转换为 ISO 格式字符串
                df_converted[col] = df_converted[col].apply(
                    lambda x: x.isoformat() if pd.notna(x) else None
                )
        
        # 获取 DataFrame 列名
        columns = list(df_converted.columns)
        placeholders = ", ".join(["?" for _ in columns])
        columns_str = ", ".join(columns)
        
        # 构建 UPSERT SQL（SQLite 语法）
        update_columns = [c for c in columns if c != unique_column and c != "id"]
        update_set = ", ".join([f"{c} = excluded.{c}" for c in update_columns])
        
        sql = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT({unique_column}) DO UPDATE SET
                {update_set},
                updated_at = CURRENT_TIMESTAMP
        """
        
        with self.get_connection() as conn:
            for i in range(0, len(df_converted), batch_size):
                batch = df_converted.iloc[i:i+batch_size]
                
                for _, row in batch.iterrows():
                    try:
                        # 检查是否存在
                        check_sql = f"SELECT id FROM {table_name} WHERE {unique_column} = ?"
                        existing = conn.execute(check_sql, (row[unique_column],)).fetchone()
                        
                        values = tuple(row[c] for c in columns)
                        conn.execute(sql, values)
                        
                        if existing:
                            stats["updated"] += 1
                        else:
                            stats["inserted"] += 1
                            
                    except Exception as e:
                        logging.warning(f"记录处理失败: {e}")
                        stats["skipped"] += 1
                
                conn.commit()
                logging.info(f"已处理 {min(i + batch_size, len(df_converted))}/{len(df_converted)} 条记录")
        
        return stats
    
    def bulk_insert(self, df: pd.DataFrame, table_name: str, 
                    if_exists: str = "append") -> int:
        """
        批量插入数据（不检查冲突，速度更快）
        
        Args:
            df: 数据 DataFrame
            table_name: 目标表名
            if_exists: 'append' 或 'replace'
            
        Returns:
            插入的行数
        """
        if df.empty:
            return 0
        
        # 转换 pandas Timestamp 为 Python 原生类型（SQLite 兼容）
        df_converted = df.copy()
        for col in df_converted.columns:
            if pd.api.types.is_datetime64_any_dtype(df_converted[col]):
                df_converted[col] = df_converted[col].apply(
                    lambda x: x.isoformat() if pd.notna(x) else None
                )
        
        with self.get_connection() as conn:
            df_converted.to_sql(table_name, conn, if_exists=if_exists, index=False)
            return len(df_converted)
    
    def get_table_count(self, table_name: str) -> int:
        """获取表的记录数"""
        result = self.execute_query(f"SELECT COUNT(*) as cnt FROM {table_name}")
        return result[0]["cnt"] if result else 0
    
    def get_existing_hashes(self, table_name: str, hash_column: str = "record_hash") -> set:
        """
        获取表中已存在的所有 hash 值
        
        Args:
            table_name: 表名
            hash_column: hash 列名
            
        Returns:
            hash 值集合
        """
        sql = f"SELECT {hash_column} FROM {table_name} WHERE {hash_column} IS NOT NULL"
        with self.get_connection() as conn:
            cursor = conn.execute(sql)
            return {row[0] for row in cursor.fetchall()}
    
    # ============================================================
    # 文件状态管理（替代 JSON 文件）
    # ============================================================
    
    def is_file_changed(self, etl_name: str, file_path: str) -> bool:
        """
        检查文件是否有变化
        
        Args:
            etl_name: ETL 名称
            file_path: 文件路径
            
        Returns:
            True: 文件有变化或是新文件，需要处理
            False: 文件未变化，可以跳过
        """
        if not os.path.exists(file_path):
            return False
        
        current_mtime = os.path.getmtime(file_path)
        current_size = os.path.getsize(file_path)
        
        # 查询数据库中的记录
        sql = "SELECT file_mtime, file_size FROM etl_file_state WHERE etl_name = ? AND file_path = ?"
        result = self.execute_query(sql, (etl_name, file_path))
        
        if not result:
            return True  # 新文件
        
        stored_mtime = result[0].get("file_mtime")
        stored_size = result[0].get("file_size")
        
        # 如果修改时间或大小变化，认为文件有变化
        if stored_mtime != current_mtime or stored_size != current_size:
            return True
        
        return False
    
    def filter_changed_files(self, etl_name: str, file_paths: List[str]) -> List[str]:
        """
        过滤出有变化的文件
        
        Args:
            etl_name: ETL 名称
            file_paths: 文件路径列表
            
        Returns:
            有变化的文件路径列表
        """
        changed_files = []
        skipped_count = 0
        
        for file_path in file_paths:
            if self.is_file_changed(etl_name, file_path):
                changed_files.append(file_path)
            else:
                skipped_count += 1
        
        if skipped_count > 0:
            logging.info(f"文件级去重：跳过 {skipped_count} 个未变化的文件，剩余 {len(changed_files)} 个待处理")
        
        return changed_files
    
    def mark_file_processed(self, etl_name: str, file_path: str) -> None:
        """标记文件为已处理"""
        if not os.path.exists(file_path):
            return
        
        current_mtime = os.path.getmtime(file_path)
        current_size = os.path.getsize(file_path)
        processed_time = datetime.now().isoformat()
        
        sql = """
            INSERT INTO etl_file_state (etl_name, file_path, file_mtime, file_size, processed_time)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(etl_name, file_path) DO UPDATE SET
                file_mtime = excluded.file_mtime,
                file_size = excluded.file_size,
                processed_time = excluded.processed_time,
                updated_at = CURRENT_TIMESTAMP
        """
        
        self.execute_sql(sql, (etl_name, file_path, current_mtime, current_size, processed_time))
    
    def mark_files_processed(self, etl_name: str, file_paths: List[str]) -> None:
        """批量标记文件为已处理"""
        for file_path in file_paths:
            self.mark_file_processed(etl_name, file_path)
    
    def get_processed_files(self, etl_name: str) -> List[Dict]:
        """获取已处理的文件列表"""
        sql = "SELECT file_path, file_mtime, file_size, processed_time FROM etl_file_state WHERE etl_name = ?"
        return self.execute_query(sql, (etl_name,))
    
    def log_etl_run(self, etl_name: str, status: str, 
                    records_read: int = 0, records_inserted: int = 0,
                    records_updated: int = 0, records_skipped: int = 0,
                    error_message: str = None, run_start: datetime = None) -> int:
        """
        记录 ETL 运行日志
        
        Returns:
            日志记录 ID
        """
        sql = """
            INSERT INTO etl_run_log 
            (etl_name, run_start, run_end, status, records_read, 
             records_inserted, records_updated, records_skipped, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        run_start = run_start or datetime.now()
        run_end = datetime.now() if status != "running" else None
        
        with self.get_connection() as conn:
            cursor = conn.execute(sql, (
                etl_name, run_start, run_end, status,
                records_read, records_inserted, records_updated, records_skipped,
                error_message
            ))
            return cursor.lastrowid
    
    def update_etl_run(self, log_id: int, status: str,
                       records_read: int = None, records_inserted: int = None,
                       records_updated: int = None, records_skipped: int = None,
                       error_message: str = None):
        """更新 ETL 运行日志"""
        updates = ["run_end = CURRENT_TIMESTAMP", "status = ?"]
        params = [status]
        
        if records_read is not None:
            updates.append("records_read = ?")
            params.append(records_read)
        if records_inserted is not None:
            updates.append("records_inserted = ?")
            params.append(records_inserted)
        if records_updated is not None:
            updates.append("records_updated = ?")
            params.append(records_updated)
        if records_skipped is not None:
            updates.append("records_skipped = ?")
            params.append(records_skipped)
        if error_message is not None:
            updates.append("error_message = ?")
            params.append(error_message)
        
        params.append(log_id)
        sql = f"UPDATE etl_run_log SET {', '.join(updates)} WHERE id = ?"
        
        self.execute_sql(sql, tuple(params))


def get_project_root() -> str:
    """获取项目根目录"""
    # db_utils.py 位于 shared_infrastructure/utils/
    # 向上2层到达项目根目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.dirname(os.path.dirname(current_dir))


def get_default_db_manager() -> DatabaseManager:
    """获取默认的数据库管理器"""
    project_root = get_project_root()
    db_path = os.path.join(project_root, "data_pipelines", "database", "mddap_v2.db")
    return DatabaseManager(db_path)


def init_default_database() -> bool:
    """初始化默认数据库"""
    project_root = get_project_root()
    
    db_path = os.path.join(project_root, "data_pipelines", "database", "mddap_v2.db")
    schema_path = os.path.join(project_root, "data_pipelines", "database", "schema", "init_schema.sql")
    
    logging.info(f"项目根目录: {project_root}")
    logging.info(f"数据库路径: {db_path}")
    logging.info(f"Schema路径: {schema_path}")
    
    db = DatabaseManager(db_path)
    return db.init_database(schema_path)


if __name__ == "__main__":
    # 测试初始化数据库
    success = init_default_database()
    if success:
        db = get_default_db_manager()
        print(f"数据库已创建: {db.db_path}")
        
        # 测试查询
        tables = db.execute_query("SELECT name FROM sqlite_master WHERE type='table'")
        print(f"已创建的表: {[t['name'] for t in tables]}")
