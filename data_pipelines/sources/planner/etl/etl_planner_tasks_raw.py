"""
Planner 任务数据 ETL 脚本 (V2 架构)
功能：从 Excel 读取 Planner 导出数据，清洗后存入数据库
"""

import os
import sys
import time
import logging
import glob
import yaml
import re
from datetime import datetime
from typing import Dict, List, Any
import pandas as pd
from pathlib import Path

# 数据库配置
PROJECT_ROOT = Path(__file__).resolve().parents[4]
project_root = str(PROJECT_ROOT)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from shared_infrastructure.utils.etl_utils import (
    setup_logging,
    load_config,
    read_sharepoint_excel,
    update_etl_state,
)
from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

# 配置路径
current_dir = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(current_dir, "..", "config", "config_planner_tasks.yaml")
SCHEMA_PATH = os.path.join(current_dir, "..", "config", "init_schema_planner_tasks.sql")
LABEL_CONFIG_PATH = os.path.join(current_dir, "..", "config", "label_cleaning.yaml")

# 设置日志 (使用工具函数或自定义)
# 这里先加载配置以获取日志路径
try:
    _cfg = load_config(CONFIG_PATH)
    _log_file = _cfg.get("logging", {}).get("file", "logs/etl_planner.log")
    if not os.path.isabs(_log_file):
        _log_file = os.path.join(project_root, _log_file)
    os.makedirs(os.path.dirname(_log_file), exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, _cfg.get("logging", {}).get("level", "INFO")),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(_log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
except Exception as e:
    # Fallback logging
    logging.basicConfig(level=logging.INFO)
    logging.warning(f"日志配置加载失败，使用默认设置: {e}")

logger = logging.getLogger(__name__)

def get_db_manager() -> SQLServerOnlyManager:
    """获取 SQL Server 数据库管理器（只写 SQL Server）"""
    return SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server",
    )

def init_database():
    """初始化数据库表"""
    logger.info("SQL Server 模式：假设 dbo.planner_tasks / dbo.planner_task_labels 已存在")
    return True


def _sqlserver_table_has_column(conn, table_name: str, column_name: str, schema: str = "dbo") -> bool:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?
        """,
        (schema, table_name, column_name),
    )
    return cur.fetchone() is not None


def _get_sqlserver_columns(conn, table_name: str, schema: str = "dbo") -> List[str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """,
        (schema, table_name),
    )
    return [str(r[0]) for r in cur.fetchall() if r and r[0]]


def _ensure_planner_sqlserver_columns(conn) -> None:
    """Ensure Planner tables have required columns; add them if missing."""
    ddl = []

    required_task_cols = {
        "IsDeleted": "BIT NULL",
        "DeletedAt": "DATETIME2 NULL",
        "LastSeenAt": "DATETIME2 NULL",
        "LastSeenSourceMtime": "NVARCHAR(64) NULL",
        "LastSeenSourceFile": "NVARCHAR(512) NULL",
    }

    for col, col_def in required_task_cols.items():
        if not _sqlserver_table_has_column(conn, "planner_tasks", col, schema="dbo"):
            ddl.append(f"ALTER TABLE dbo.planner_tasks ADD [{col}] {col_def}")

    for stmt in ddl:
        conn.cursor().execute(stmt)

    if ddl:
        conn.commit()


def _create_temp_table_for_planner_tasks(cur, temp_name: str = "#PlannerTasks") -> None:
    cur.execute(f"IF OBJECT_ID('tempdb..{temp_name}') IS NOT NULL DROP TABLE {temp_name};")
    cur.execute(
        f"""
        CREATE TABLE {temp_name} (
            TaskId NVARCHAR(255) NOT NULL,
            TaskName NVARCHAR(MAX) NULL,
            BucketName NVARCHAR(MAX) NULL,
            Status NVARCHAR(MAX) NULL,
            Priority NVARCHAR(MAX) NULL,
            Assignees NVARCHAR(MAX) NULL,
            CreatedBy NVARCHAR(MAX) NULL,
            CreatedDate DATE NULL,
            StartDate DATE NULL,
            DueDate DATE NULL,
            IsRecurring BIT NULL,
            IsLate BIT NULL,
            CompletedDate DATE NULL,
            CompletedBy NVARCHAR(MAX) NULL,
            CompletedChecklistItemCount INT NULL,
            ChecklistItemCount INT NULL,
            Labels NVARCHAR(MAX) NULL,
            Description NVARCHAR(MAX) NULL,
            SourceFile NVARCHAR(MAX) NULL,
            TeamName NVARCHAR(MAX) NULL,
            ImportedAt DATETIME2 NULL,
            IsDeleted BIT NULL,
            DeletedAt DATETIME2 NULL,
            LastSeenAt DATETIME2 NULL,
            LastSeenSourceMtime NVARCHAR(64) NULL,
            LastSeenSourceFile NVARCHAR(MAX) NULL
        );
        """
    )


def _bulk_insert_temp_planner_tasks(cur, df: pd.DataFrame, temp_name: str = "#PlannerTasks") -> None:
    insert_cols = [
        "TaskId",
        "TaskName",
        "BucketName",
        "Status",
        "Priority",
        "Assignees",
        "CreatedBy",
        "CreatedDate",
        "StartDate",
        "DueDate",
        "IsRecurring",
        "IsLate",
        "CompletedDate",
        "CompletedBy",
        "CompletedChecklistItemCount",
        "ChecklistItemCount",
        "Labels",
        "Description",
        "SourceFile",
        "TeamName",
        "ImportedAt",
        "IsDeleted",
        "DeletedAt",
        "LastSeenAt",
        "LastSeenSourceMtime",
        "LastSeenSourceFile",
    ]
    insert_cols = [c for c in insert_cols if c in df.columns]
    if not insert_cols:
        return

    placeholders = ",".join(["?"] * len(insert_cols))
    cols_sql = ",".join([f"[{c}]" for c in insert_cols])
    sql = f"INSERT INTO {temp_name} ({cols_sql}) VALUES ({placeholders})"

    df2 = df[insert_cols].copy()
    for bcol in ["IsRecurring", "IsLate", "IsDeleted"]:
        if bcol in df2.columns:
            df2[bcol] = df2[bcol].apply(lambda x: None if pd.isna(x) else (1 if bool(x) else 0))

    def _to_py(v):
        if v is None:
            return None
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        if hasattr(v, "to_pydatetime"):
            try:
                return v.to_pydatetime()
            except Exception:
                return None
        return v

    rows = [tuple(_to_py(v) for v in r) for r in df2.itertuples(index=False, name=None)]
    can_fast = True
    try:
        cur.fast_executemany = True
    except Exception:
        can_fast = False

    try:
        cur.executemany(sql, rows)
    except Exception as e:
        # pyodbc/ODBC 在 fast_executemany 模式下会为 NVARCHAR 参数绑定固定长度缓冲区，
        # 遇到超长字符串时可能抛出 "String data, right truncation"。
        if can_fast and "right truncation" in str(e).lower():
            try:
                cur.fast_executemany = False
            except Exception:
                pass
            cur.execute(f"DELETE FROM {temp_name};")
            cur.executemany(sql, rows)
        else:
            raise


def _merge_planner_tasks(cur, temp_name: str = "#PlannerTasks") -> None:
    cur.execute(
        f"""
        MERGE dbo.planner_tasks AS tgt
        USING {temp_name} AS src
            ON tgt.TaskId = src.TaskId
        WHEN MATCHED THEN
            UPDATE SET
                tgt.TaskName = src.TaskName,
                tgt.BucketName = src.BucketName,
                tgt.Status = src.Status,
                tgt.Priority = src.Priority,
                tgt.Assignees = src.Assignees,
                tgt.CreatedBy = src.CreatedBy,
                tgt.CreatedDate = src.CreatedDate,
                tgt.StartDate = src.StartDate,
                tgt.DueDate = src.DueDate,
                tgt.IsRecurring = src.IsRecurring,
                tgt.IsLate = src.IsLate,
                tgt.CompletedDate = src.CompletedDate,
                tgt.CompletedBy = src.CompletedBy,
                tgt.CompletedChecklistItemCount = src.CompletedChecklistItemCount,
                tgt.ChecklistItemCount = src.ChecklistItemCount,
                tgt.Labels = src.Labels,
                tgt.Description = src.Description,
                tgt.SourceFile = src.SourceFile,
                tgt.TeamName = src.TeamName,
                tgt.ImportedAt = src.ImportedAt,
                tgt.IsDeleted = src.IsDeleted,
                tgt.DeletedAt = src.DeletedAt,
                tgt.LastSeenAt = src.LastSeenAt,
                tgt.LastSeenSourceMtime = src.LastSeenSourceMtime,
                tgt.LastSeenSourceFile = src.LastSeenSourceFile
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                TaskId, TaskName, BucketName, Status, Priority, Assignees, CreatedBy,
                CreatedDate, StartDate, DueDate, IsRecurring, IsLate,
                CompletedDate, CompletedBy, CompletedChecklistItemCount, ChecklistItemCount,
                Labels, Description, SourceFile, TeamName, ImportedAt,
                IsDeleted, DeletedAt, LastSeenAt, LastSeenSourceMtime, LastSeenSourceFile
            )
            VALUES (
                src.TaskId, src.TaskName, src.BucketName, src.Status, src.Priority, src.Assignees, src.CreatedBy,
                src.CreatedDate, src.StartDate, src.DueDate, src.IsRecurring, src.IsLate,
                src.CompletedDate, src.CompletedBy, src.CompletedChecklistItemCount, src.ChecklistItemCount,
                src.Labels, src.Description, src.SourceFile, src.TeamName, src.ImportedAt,
                src.IsDeleted, src.DeletedAt, src.LastSeenAt, src.LastSeenSourceMtime, src.LastSeenSourceFile
            );
        """
    )

def extract_team_name(filename):
    """从文件名提取团队名称"""
    name = filename.replace('.xlsx', '')
    if '-TIER' in name:
        name = name.split('-TIER')[0]
    elif '-Tier' in name:
        name = name.split('-Tier')[0]
    return name

def load_label_cleaning_config(cfg: Dict[str, Any]):
    """从主配置文件加载标签清洗配置"""
    try:
        label_cfg = cfg.get("label_cleaning", {})
        return label_cfg
    except Exception as e:
        logger.warning(f"Failed to load label cleaning config: {e}. Using defaults.")
        return {"mappings": {}, "exclusions": []}

def read_planner_files(cfg: Dict[str, Any], test_mode: bool = False, max_rows: int = 1000) -> pd.DataFrame:
    """读取 Planner Excel 文件"""
    source_cfg = cfg.get("source", {})
    planner_path = source_cfg.get("planner_path")
    file_pattern = source_cfg.get("file_pattern", "*.xlsx")

    if not planner_path or not os.path.exists(planner_path):
        logger.error(f"Planner 数据路径不存在: {planner_path}")
        return pd.DataFrame()

    search_path = os.path.join(planner_path, file_pattern)
    files = glob.glob(search_path)
    
    if not files:
        logger.warning(f"未找到匹配的文件: {search_path}")
        return pd.DataFrame()

    # Filter changed files
    db = get_db_manager()
    # Normalize paths to absolute
    files = [os.path.normpath(os.path.abspath(f)) for f in files]
    
    # Check for changes
    changed_files = db.filter_changed_files("planner_tasks_raw", files)
    
    if not changed_files:
        logger.info("所有文件均未变化，无需读取。")
        return pd.DataFrame()
        
    logger.info(f"发现 {len(changed_files)} 个文件有更新 (总数: {len(files)})")

    all_dfs = []
    
    for file_path in changed_files:
        # 跳过临时文件
        filename = os.path.basename(file_path)
        if filename.startswith('~$'):
            continue
            
        logger.info(f"读取文件: {filename}")
        try:
            # 尝试读取
            # Planner 导出通常在第一个Sheet '任务'，或者默认第一个
            # 使用 read_sharepoint_excel 工具函数处理重试和锁定
            df = read_sharepoint_excel(file_path, max_rows=max_rows if test_mode else None)
            
            if not df.empty:
                # 添加元数据
                df['SourceFile'] = filename
                df['SourceFilePath'] = file_path # Store full path for state tracking
                df['TeamName'] = extract_team_name(filename)
                df['ImportedAt'] = datetime.now()
                df['SourceFileMtime'] = datetime.fromtimestamp(os.path.getmtime(file_path))
                all_dfs.append(df)
                logger.info(f"  成功读取 {len(df)} 行")
            
        except Exception as e:
            logger.error(f"  读取失败 {filename}: {e}")

    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    
    return pd.DataFrame()

def clean_planner_data(df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    """清洗 Planner 数据"""
    if df.empty:
        return df
    
    mapping = cfg.get("mapping", {})
    
    # 1. 重命名列
    df_clean = df.rename(columns=mapping)
    
    # 2. 保留需要的列 (根据 mapping 的 values + 元数据列)
    target_cols = list(mapping.values()) + ['SourceFile', 'SourceFilePath', 'TeamName', 'ImportedAt', 'SourceFileMtime']
    existing_cols = [c for c in target_cols if c in df_clean.columns]
    df_clean = df_clean[existing_cols]
    
    # 3. 数据类型转换
    # 日期列
    date_cols = ['CreatedDate', 'StartDate', 'DueDate', 'CompletedDate']
    for col in date_cols:
        if col in df_clean.columns:
            # 先转为 datetime，再转为 date 对象，最后将 NaT 替换为 None
            dt_series = pd.to_datetime(df_clean[col], errors='coerce')
            # 使用 object 类型存储 date 对象和 None
            df_clean[col] = dt_series.dt.date.astype(object).where(dt_series.notna(), None)
            
    # 布尔列
    bool_cols = ['IsRecurring', 'IsLate']
    for col in bool_cols:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].fillna(False).astype(bool)

    # 数值列（避免空字符串/NaN 导致 SQL Server float/int 参数错误）
    num_cols = ['CompletedChecklistItemCount', 'ChecklistItemCount']
    for col in num_cols:
        if col in df_clean.columns:
            s = pd.to_numeric(df_clean[col], errors='coerce')
            df_clean[col] = s.astype('Int64').astype(object).where(s.notna(), None)

    logger.info(f"数据清洗完成: {len(df_clean)} 行")
    return df_clean

def save_to_database(df: pd.DataFrame, table_name: str = "planner_tasks") -> Dict[str, int]:
    """保存到数据库"""
    if df.empty:
        return {"inserted": 0, "updated": 0, "skipped": 0}
        
    db = get_db_manager()

    # 按 TaskId 以“最新导出文件”为准（避免旧文件覆盖新文件）
    df_to_save = df.copy()
    if 'TaskId' in df_to_save.columns:
        df_to_save = df_to_save[df_to_save['TaskId'].notna()]

    sort_cols = []
    if 'SourceFileMtime' in df_to_save.columns:
        sort_cols.append('SourceFileMtime')
    if 'ImportedAt' in df_to_save.columns:
        sort_cols.append('ImportedAt')

    if sort_cols and 'TaskId' in df_to_save.columns:
        df_to_save = df_to_save.sort_values(sort_cols).drop_duplicates(subset=['TaskId'], keep='last')
    elif 'TaskId' in df_to_save.columns:
        df_to_save = df_to_save.drop_duplicates(subset=['TaskId'], keep='last')

    now_ts = datetime.now()
    df_to_save['IsDeleted'] = 0
    df_to_save['DeletedAt'] = None
    df_to_save['LastSeenAt'] = df_to_save['ImportedAt'] if 'ImportedAt' in df_to_save.columns else now_ts
    if 'SourceFileMtime' in df_to_save.columns:
        df_to_save['LastSeenSourceMtime'] = df_to_save['SourceFileMtime'].apply(
            lambda x: x.isoformat() if hasattr(x, 'isoformat') else (str(x) if x is not None else None)
        )
    else:
        df_to_save['LastSeenSourceMtime'] = None
    df_to_save['LastSeenSourceFile'] = df_to_save['SourceFile'] if 'SourceFile' in df_to_save.columns else None

    # Helper column: not persisted
    if 'SourceFileMtime' in df_to_save.columns:
        df_to_save = df_to_save.drop(columns=['SourceFileMtime'])
    
    try:
        with db.get_connection() as conn:
            _ensure_planner_sqlserver_columns(conn)
            cur = conn.cursor()
            _create_temp_table_for_planner_tasks(cur, "#PlannerTasks")
            _bulk_insert_temp_planner_tasks(cur, df_to_save, "#PlannerTasks")
            _merge_planner_tasks(cur, "#PlannerTasks")
            conn.commit()
            
        # Mark files as processed
        if 'SourceFilePath' in df.columns:
            unique_files = df['SourceFilePath'].dropna().unique()
            for file_path in unique_files:
                try:
                    db.mark_file_processed("planner_tasks_raw", str(file_path))
                except Exception as e:
                    logger.warning(f"Failed to mark file processed {file_path}: {e}")

        return {"inserted": len(df_to_save), "updated": 0, "skipped": 0}
    except Exception as e:
        logger.error(f"保存数据库失败: {e}")
        return {"inserted": 0, "updated": 0, "skipped": len(df_to_save)}


def mark_missing_tasks_deleted(df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return

    if 'TaskId' not in df.columns or 'TeamName' not in df.columns or 'SourceFileMtime' not in df.columns:
        return

    db = get_db_manager()

    for team in df['TeamName'].dropna().unique().tolist():
        team_df = df[df['TeamName'] == team]
        if team_df.empty:
            continue

        latest_mtime = team_df['SourceFileMtime'].max()
        latest_mtime_str = latest_mtime.isoformat() if hasattr(latest_mtime, 'isoformat') else str(latest_mtime)
        current_ids = team_df[team_df['SourceFileMtime'] == latest_mtime]['TaskId'].dropna().astype(str).unique().tolist()
        if not current_ids:
            continue

        try:
            with db.get_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    "UPDATE dbo.planner_tasks SET IsDeleted=1, DeletedAt=GETDATE() "
                    "WHERE TeamName=? AND (IsDeleted IS NULL OR IsDeleted=0) AND (LastSeenSourceMtime IS NULL OR LastSeenSourceMtime < ?)",
                    (team, latest_mtime_str),
                )

                cur.execute("IF OBJECT_ID('tempdb..#CurrentTaskIds') IS NOT NULL DROP TABLE #CurrentTaskIds;")
                cur.execute("CREATE TABLE #CurrentTaskIds (TaskId NVARCHAR(255) PRIMARY KEY);")
                try:
                    cur.fast_executemany = True
                except Exception:
                    pass
                cur.executemany(
                    "INSERT INTO #CurrentTaskIds (TaskId) VALUES (?)",
                    [(tid,) for tid in current_ids],
                )

                cur.execute(
                    "UPDATE t SET IsDeleted=0, DeletedAt=NULL "
                    "FROM dbo.planner_tasks t "
                    "INNER JOIN #CurrentTaskIds c ON t.TaskId = c.TaskId "
                    "WHERE t.TeamName=?",
                    (team,),
                )
                conn.commit()
        except Exception as e:
            logger.warning(f"标记已删除任务失败: {e}")

def extract_and_clean_labels(df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    """提取并清洗标签数据"""
    if df.empty:
        return pd.DataFrame()

    config = load_label_cleaning_config(cfg)
    mappings = config.get('mappings', {})
    exclusions = set(config.get('exclusions', []))
    
    label_rows = []
    
    for _, row in df.iterrows():
        task_id = row.get('TaskId')
        if not task_id: continue
        
        raw_labels_str = row.get('Labels')
        if pd.isna(raw_labels_str) or not raw_labels_str:
            continue
            
        # Split by common delimiters
        raw_labels = str(raw_labels_str).replace(';', ',').split(',')
        
        for label in raw_labels:
            label = label.strip()
            if not label: continue
            
            cleaned_label = label
            # Apply mapping
            for standardized, variants in mappings.items():
                if label == standardized or label in variants:
                    cleaned_label = standardized
                    break
            
            is_excluded = 1 if cleaned_label in exclusions else 0
            
            label_rows.append({
                'TaskId': task_id,
                'OriginalLabel': label,
                'CleanedLabel': cleaned_label,
                'IsExcluded': is_excluded
            })
            
    return pd.DataFrame(label_rows)

def save_labels_to_database(df_labels: pd.DataFrame):
    """保存标签到数据库 (全量覆盖更新涉及的任务)"""
    if df_labels.empty:
        return
    
    db = get_db_manager()
    task_ids = df_labels['TaskId'].unique().tolist()
    
    try:
        with db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("IF OBJECT_ID('tempdb..#TempTaskIds') IS NOT NULL DROP TABLE #TempTaskIds;")
            cur.execute("CREATE TABLE #TempTaskIds (TaskId NVARCHAR(255) PRIMARY KEY);")
            try:
                cur.fast_executemany = True
            except Exception:
                pass
            cur.executemany(
                "INSERT INTO #TempTaskIds (TaskId) VALUES (?)",
                [(str(tid),) for tid in task_ids if tid is not None and str(tid).strip() != ""],
            )

            cur.execute(
                "DELETE l FROM dbo.planner_task_labels l INNER JOIN #TempTaskIds t ON l.TaskId = t.TaskId"
            )

            insert_cols = ["TaskId", "OriginalLabel", "CleanedLabel", "IsExcluded"]
            df_ins = df_labels.copy()
            df_ins = df_ins[[c for c in insert_cols if c in df_ins.columns]]
            if "IsExcluded" in df_ins.columns:
                df_ins["IsExcluded"] = df_ins["IsExcluded"].apply(lambda x: None if pd.isna(x) else (1 if bool(x) else 0))

            placeholders = ",".join(["?"] * len(df_ins.columns))
            cols_sql = ",".join([f"[{c}]" for c in df_ins.columns])
            sql = f"INSERT INTO dbo.planner_task_labels ({cols_sql}) VALUES ({placeholders})"
            rows = [tuple(None if (v is None or (hasattr(pd, 'isna') and pd.isna(v))) else v for v in r) for r in df_ins.itertuples(index=False, name=None)]
            if rows:
                cur.executemany(sql, rows)
            conn.commit()

        logger.info(f"已更新 {len(task_ids)} 个任务的标签信息，共插入 {len(df_labels)} 条标签记录。")
    except Exception as e:
        logger.error(f"保存标签失败: {e}")

def sync_planner_task_status():
    """同步已关闭的Planner任务状态到触发系统"""
    logger.info("开始同步 Planner 任务状态...")
    
    try:
        db = get_db_manager()
        
        with db.get_connection() as conn:
            # 获取已关闭的任务
            query = """
            SELECT 
                TaskId,
                TaskName,
                Status,
                CompletedDate,
                BucketName
            FROM dbo.planner_tasks 
            WHERE Status = 'Completed' 
               OR Status = 'Closed'
               OR (Status = 'Completed %' AND Status IS NOT NULL)
            ORDER BY CompletedDate DESC
            """
            
            closed_tasks = pd.read_sql(query, conn)
            logger.info(f"找到 {len(closed_tasks)} 个已关闭的Planner任务")
            
            if closed_tasks.empty:
                logger.info("没有需要更新的任务")
                return
            
            updated_count = 0
            
            for _, task in closed_tasks.iterrows():
                # 提取A3Id
                title = task['TaskName']
                if not title or pd.isna(title):
                    continue
                    
                # A3Id格式: A3-YYYYMMDD-NNNN
                pattern = r'A3-\d{8}-\d{4}'
                match = re.search(pattern, title)
                if not match:
                    continue
                    
                a3_id = match.group(0)
                
                # 更新TriggerCaseRegistry表中的状态
                update_query = """
                UPDATE dbo.TriggerCaseRegistry 
                SET Status = 'CLOSED',
                    ClosedAt = ?,
                    PlannerTaskId = ?,
                    UpdatedAt = ?
                WHERE A3Id = ? AND Status = 'OPEN'
                """
                
                completed_date = task['CompletedDate'] if task['CompletedDate'] else datetime.now().strftime('%Y-%m-%d')
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                cursor = conn.cursor()
                cursor.execute(update_query, (
                    completed_date,
                    task['TaskId'],
                    current_time,
                    a3_id
                ))
                
                if cursor.rowcount > 0:
                    updated_count += 1
                    logger.info(f"更新A3Id {a3_id} 状态为CLOSED")
            
            logger.info(f"成功更新 {updated_count} 个触发状态")
            
    except Exception as e:
        logger.error(f"同步Planner任务状态失败: {e}")

def main(test_mode: bool = False):
    logger.info("=" * 60)
    logger.info("Planner 任务数据 ETL (V2) 启动")
    if test_mode:
        logger.info("*** 测试模式 ***")
    logger.info("=" * 60)
    
    t0 = time.time()
    
    try:
        # 1. 初始化
        init_database()
        cfg = load_config(CONFIG_PATH)
        
        # 2. 读取
        max_rows = cfg.get("test", {}).get("max_rows", 1000)
        df = read_planner_files(cfg, test_mode=test_mode, max_rows=max_rows)
        
        # 3. 清洗任务数据
        df_clean = clean_planner_data(df, cfg)
        
        # 4. 保存任务数据
        stats = save_to_database(df_clean, "planner_tasks")

        # 4.1 标记缺失任务为已删除
        mark_missing_tasks_deleted(df_clean)
        
        # 5. 提取并清洗标签 (使用 df_clean 以确保只处理有效任务)
        df_labels = extract_and_clean_labels(df_clean, cfg)
        
        # 6. 保存标签数据
        save_labels_to_database(df_labels)
        
        # 7. 同步Planner任务状态到触发系统
        sync_planner_task_status()
        
        logger.info(f"处理完成: 插入 {stats['inserted']}, 更新 {stats['updated']}, 跳过/错误 {stats['skipped']}")
        
        # 8. 记录运行日志
        logger.info(
            f"运行完成: read={len(df)}, inserted={stats['inserted']}, updated={stats['updated']}, skipped={stats['skipped']}"
        )
        
    except Exception as e:
        logger.exception(f"ETL 运行失败: {e}")
            
    logger.info(f"总耗时: {time.time() - t0:.2f} 秒")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Planner Data ETL')
    parser.add_argument('--test', action='store_true', help='Test mode')
    args = parser.parse_args()
    
    main(test_mode=args.test)
