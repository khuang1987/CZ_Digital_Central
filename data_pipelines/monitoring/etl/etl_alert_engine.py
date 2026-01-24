"""
KPI 监控引擎 (ETL组件)
功能：扫描 KPI_Data，根据规则生成触发案例 (A3)，并导出 CSV 供 Power BI 展示。
原: monitoring/engine.py
"""
import csv
import pandas as pd
from datetime import datetime
import os
import re
import hashlib
import logging
import pyodbc
from pathlib import Path

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def _migrate_category_tags(conn: pyodbc.Connection) -> None:
    """Migrate legacy Category tag names to new ones to avoid duplicate cases."""
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT 1 WHERE OBJECT_ID('dbo.TriggerCaseRegistry','U') IS NOT NULL")
        if cursor.fetchone() is None:
            return
    except Exception:
        return

    cursor.execute(
        "UPDATE dbo.TriggerCaseRegistry "
        "SET Category = 'CZ_Campus', UpdatedAt = SYSUTCDATETIME() "
        "WHERE Category = 'GLOBAL' "
        "  AND TriggerType IN ('LT_GLOBAL_WARNING','LT_GLOBAL_CRITICAL','SA_GLOBAL_WARNING','SA_GLOBAL_CRITICAL')"
    )
    if cursor.rowcount:
        conn.commit()

# 路径配置 - 调整为基于当前文件位置 (data_pipelines/monitoring/etl/)
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent.parent.parent
OUTPUT_DIR = PROJECT_ROOT / 'data_pipelines' / 'monitoring' / 'output'

# 确保输出目录存在
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def get_db_connection():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        r"SERVER=localhost\SQLEXPRESS;"
        "DATABASE=mddap_v2;"
        "Trusted_Connection=yes;"
        "Encrypt=no;"
    )
    try:
        return pyodbc.connect(conn_str, autocommit=False)
    except Exception as e:
        logger.error(f"SQL Server 连接失败: {e}")
        return None


def _is_sqlserver_conn(conn) -> bool:
    return conn is not None


def _ensure_monitoring_schema(conn: pyodbc.Connection):
    cur = conn.cursor()
    cur.execute(
        "IF OBJECT_ID('dbo.TriggerCaseRegistry','U') IS NULL "
        "BEGIN "
        "CREATE TABLE dbo.TriggerCaseRegistry ("
        "  A3Id NVARCHAR(32) NOT NULL CONSTRAINT PK_TriggerCaseRegistry PRIMARY KEY,"
        "  Category NVARCHAR(128) NOT NULL,"
        "  TriggerType NVARCHAR(64) NOT NULL,"
        "  Source NVARCHAR(16) NULL,"
        "  Status NVARCHAR(16) NULL,"
        "  OpenedAt DATE NULL,"
        "  ClosedAt DATE NULL,"
        "  PlannerTaskId NVARCHAR(255) NULL,"
        "  Notes NVARCHAR(MAX) NULL,"
        "  OriginalLevel NVARCHAR(32) NULL,"
        "  OriginalDesc NVARCHAR(512) NULL,"
        "  OriginalDetails NVARCHAR(MAX) NULL,"
        "  OriginalValue NVARCHAR(128) NULL,"
        "  UpdatedAt DATETIME2 NOT NULL CONSTRAINT DF_TriggerCaseRegistry_UpdatedAt DEFAULT SYSUTCDATETIME()"
        ");"
        "CREATE INDEX IX_TriggerCaseRegistry_Status ON dbo.TriggerCaseRegistry(Status);"
        "CREATE INDEX IX_TriggerCaseRegistry_CategoryTypeStatus ON dbo.TriggerCaseRegistry(Category, TriggerType, Status);"
        "END"
    )
    cur.execute(
        "IF OBJECT_ID('dbo.TriggerCaseCutoff','U') IS NULL "
        "BEGIN "
        "CREATE TABLE dbo.TriggerCaseCutoff ("
        "  Tag NVARCHAR(128) NOT NULL,"
        "  TriggerType NVARCHAR(64) NOT NULL,"
        "  ClosedAt DATE NULL,"
        "  CONSTRAINT PK_TriggerCaseCutoff PRIMARY KEY (Tag, TriggerType)"
        ");"
        "END"
    )
    conn.commit()

def _get_int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == '':
        return default
    try:
        return int(value)
    except ValueError:
        return default

def _parse_date_yyyy_mm_dd(value):
    if value is None: return None
    s = str(value).strip()
    if not s: return None
    try:
        return datetime.strptime(s[:10], '%Y-%m-%d').date()
    except ValueError:
        return None

def _get_as_of_date():
    v = os.getenv('KPI_AS_OF_DATE')
    return _parse_date_yyyy_mm_dd(v)

def _sql_date_now(as_of_date):
    if as_of_date is None:
        return "CAST(GETDATE() AS date)"
    return f"CAST('{as_of_date.strftime('%Y-%m-%d')}' AS date)"

def _sql_datetime_now(as_of_date):
    if as_of_date is None:
        return "SYSUTCDATETIME()"
    return f"CAST('{as_of_date.strftime('%Y-%m-%d')} 12:00:00' AS datetime2)"

def _normalize_status(value: str) -> str:
    if value is None: return ''
    return str(value).strip().upper()

def _safe_strftime(dt, fmt='%Y-%m-%d'):
    """Safely format a date/datetime object or string."""
    if dt is None:
        return ""
    if isinstance(dt, str):
        # Already a string, return first 10 chars if it looks like a date
        s = dt.strip()
        if len(s) >= 10 and re.match(r'\d{4}-\d{2}-\d{2}', s):
            return s[:10]
        return s
    try:
        return dt.strftime(fmt)
    except AttributeError:
        return str(dt)

def _format_ordinal(n):
    """Format integer as English ordinal (1st, 2nd, 3rd, etc.)"""
    try:
        n = int(float(n))
        if 11 <= (n % 100) <= 13:
            suffix = 'th'
        else:
            suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
        return f"{n}{suffix}"
    except (ValueError, TypeError):
        return str(n)

def _effective_case_status(raw_status: str, opened_at, closed_at, as_of_date):
    status = _normalize_status(raw_status)
    if as_of_date is None:
        if status: return status
        return 'OPEN'

    def _coerce_to_date(v):
        if v is None:
            return None
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, str):
            return _parse_date_yyyy_mm_dd(v)
        d = getattr(v, 'date', None)
        if callable(d):
            try:
                return d()
            except Exception:
                pass
        return _parse_date_yyyy_mm_dd(v)

    opened_at = _coerce_to_date(opened_at)
    closed_at = _coerce_to_date(closed_at)

    if opened_at is not None and opened_at > as_of_date:
        return 'OPEN'

    if status == 'CLOSED':
        if closed_at is not None and closed_at > as_of_date:
            return 'OPEN'
        return 'CLOSED'

    if status == 'OPEN':
        return 'OPEN'

    return status or 'OPEN'

def _extract_min_date_from_details(details: str):
    if details is None: return None
    text = str(details)
    candidates = re.findall(r'\d{4}-\d{2}-\d{2}', text)
    dates = []
    for c in candidates:
        d = _parse_date_yyyy_mm_dd(c)
        if d is not None:
            dates.append(d)
    if not dates:
        return None
    return min(dates)

def _alloc_a3_id(date_str: str, conn, used_ids) -> str:
    """Allocate a new A3 ID in format A3-YYYYMMDD-0001."""
    prefix = f"A3-{date_str}-"

    cursor = conn.cursor()
    cursor.execute("SELECT A3Id FROM dbo.TriggerCaseRegistry WHERE A3Id LIKE ?", (f"{prefix}%",))
    max_seq = 0
    for (aid,) in cursor.fetchall():
        try:
            suffix = int(aid.split('-')[-1])
            if suffix > max_seq:
                max_seq = suffix
        except Exception:
            continue

    for aid in used_ids:
        if aid.startswith(prefix):
            try:
                suffix = int(aid.split('-')[-1])
                if suffix > max_seq:
                    max_seq = suffix
            except Exception:
                continue

    next_seq = max_seq + 1
    new_id = f"{prefix}{next_seq:04d}"
    while new_id in used_ids:
        next_seq += 1
        new_id = f"{prefix}{next_seq:04d}"
    used_ids.add(new_id)
    return new_id

def _load_case_registry_from_db(conn):
    cases = {}
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT "
            "  A3Id, Category, TriggerType, Source, Status, OpenedAt, ClosedAt, PlannerTaskId, Notes, "
            "  OriginalLevel, OriginalDesc, OriginalDetails, OriginalValue "
            "FROM dbo.TriggerCaseRegistry"
        )
    except Exception:
        return {}

    rows = cursor.fetchall()
    for (
        a3_id,
        category,
        trigger_type,
        source,
        status,
        opened_at,
        closed_at,
        planner_task_id,
        notes,
        orig_level,
        orig_desc,
        orig_details,
        orig_value,
    ) in rows:
        category = (category or '').strip()
        trigger_type = (trigger_type or '').strip()
        if not category or not trigger_type:
            continue

        try:
            opened_key = opened_at.strftime('%Y-%m-%d') if opened_at is not None else ''
        except Exception:
            opened_key = str(opened_at or '').strip()

        cases[(category, trigger_type, opened_key)] = {
            'A3Id': (str(a3_id) if a3_id is not None else '').strip(),
            'Source': (str(source) if source is not None else 'AUTO').strip().upper(),
            'Status': _normalize_status(status),
            'ClosedAt': closed_at,
            'OpenedAt': opened_at,
            'PlannerTaskId': (str(planner_task_id) if planner_task_id is not None else '').strip(),
            'Notes': (str(notes) if notes is not None else '').strip(),
            'OriginalLevel': (str(orig_level) if orig_level is not None else '').strip(),
            'OriginalDesc': (str(orig_desc) if orig_desc is not None else '').strip(),
            'OriginalDetails': (str(orig_details) if orig_details is not None else '').strip(),
            'OriginalValue': (str(orig_value) if orig_value is not None else '').strip(),
        }
    return cases

def _upsert_case_registry_row(conn, a3_id, category, trigger_type, status, opened_at, closed_at, planner_task_id, notes,
                               original_level='', original_desc='', original_details='', original_value='', source='AUTO'):
    cursor = conn.cursor()
    if _is_sqlserver_conn(conn):
        cursor.execute(
            "UPDATE dbo.TriggerCaseRegistry "
            "SET "
            "  Status=?, "
            "  Source=COALESCE(NULLIF(Source,''), ?), "
            "  Category=?, "
            "  TriggerType=?, "
            "  OpenedAt=COALESCE(?, OpenedAt), "
            "  ClosedAt=?, "
            "  PlannerTaskId=?, "
            "  Notes=?, "
            "  OriginalLevel=?, "
            "  OriginalDesc=?, "
            "  OriginalDetails=?, "
            "  OriginalValue=?, "
            "  UpdatedAt=SYSUTCDATETIME() "
            "WHERE A3Id=?",
            (
                status,
                (source or 'AUTO'),
                category,
                trigger_type,
                opened_at,
                closed_at,
                planner_task_id,
                notes,
                original_level,
                original_desc,
                original_details,
                original_value,
                a3_id,
            ),
        )
        if cursor.rowcount == 0:
            cursor.execute(
                "INSERT INTO dbo.TriggerCaseRegistry ("
                "  A3Id, Category, TriggerType, Source, Status, OpenedAt, ClosedAt, PlannerTaskId, Notes, "
                "  OriginalLevel, OriginalDesc, OriginalDetails, OriginalValue"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    a3_id,
                    category,
                    trigger_type,
                    (source or 'AUTO'),
                    status,
                    opened_at,
                    closed_at,
                    planner_task_id,
                    notes,
                    original_level,
                    original_desc,
                    original_details,
                    original_value,
                ),
            )
        conn.commit()
        return

def _load_rules_from_csv():
    rules_path = PROJECT_ROOT / 'data_pipelines' / 'monitoring' / 'config' / 'kpi_rules.csv'
    if not rules_path.exists():
        logger.error(f"找不到规则配置文件: {rules_path}")
        return []
    
    rules = []
    try:
        with open(rules_path, 'r', encoding='utf-8-sig') as f:
            # Read header first to strip whitespace
            reader = csv.reader(f)
            headers = next(reader, None)
            if not headers:
                return []
            
            headers = [h.strip() for h in headers]
            
            for row in reader:
                if not row: continue
                # Create dict manually and strip values
                row_dict = {
                    h: v.strip() for h, v in zip(headers, row) if len(v) > 0 # Handle potential trailing empty fields if any
                }
                
                # Basic validation
                if not row_dict.get('RuleCode') or not row_dict.get('KPI_Id'):
                    continue
                
                # Filter by Active column
                is_active = row_dict.get('Active', 'Yes').strip().lower() == 'yes'
                if not is_active:
                    continue
                    
                rules.append(row_dict)
                
        logger.info(f"Loaded {len(rules)} active rules.")
        for r in rules:
            logger.info(f"  Rule: {r['RuleCode']} | Op: {r['ComparisonOperator']} | Thr: {r['ThresholdValue']} | TagFilter: {r.get('TagFilter')}")
        return rules
    except Exception as e:
        logger.error(f"读取规则文件失败: {e}")
    return rules

def check_generic_rule(cursor, rule):
    """根据规则配置执行通用检查"""
    rule_code = rule['RuleCode']
    kpi_id = rule['KPI_Id']
    # kpi_name = rule['KPI_Name']
    # owner = rule['Owner']
    # data_source = rule['DataSource']
    monitor_start_date = rule.get('MonitorStartDate', '2000-01-01')
    op = rule['ComparisonOperator']
    threshold = rule['ThresholdValue']
    consecutive = int(rule.get('ConsecutiveOccurrences', 1))
    level = rule.get('TriggerLevel', 'Medium')
    lookback = int(rule.get('LookbackDays', 7))
    desc_template = rule.get('Description', '')
    tag_filter = rule.get('TagFilter', '')
    
    # Load MinVolume (Default 0)
    min_volume = 0
    val = rule.get('MinVolume', '0')
    if val and str(val).isdigit():
        min_volume = int(val)

    watermark_days = 60
    effective_lookback = min(max(lookback, 1), watermark_days)

    # 简单的防止SQL注入处理 (Op 只能是 > 或 <)
    if op not in ('>', '<', '>=', '<=', '=', '!='):
        logger.warning(f"规则 {rule_code} 的操作符非法: {op}")
        return []

    # 动态描述生成逻辑
    tag_condition = ""
    if tag_filter:
        tag_condition = f"AND d.Tag = '{tag_filter}'"

    # SQL Server path
    try:
        conn_obj = getattr(cursor, "connection", None)
    except Exception:
        conn_obj = None

    if _is_sqlserver_conn(conn_obj):
        # Get as_of_date (date) and current fiscal week
        as_of_date = _get_as_of_date()
        if as_of_date is None:
            as_of_date = datetime.now().date()

        # Determine current fiscal week (cy/cw)
        try:
            cursor.execute(
                "SELECT TOP 1 fiscal_year, fiscal_week FROM dbo.dim_calendar WHERE [date] = ?",
                (as_of_date.strftime('%Y-%m-%d'),),
            )
            row = cursor.fetchone()
            cy = int(row[0]) if row and row[0] is not None else None
            cw = int(row[1]) if row and row[1] is not None else None
        except Exception:
            cy, cw = None, None

        # Build query (note: op is already validated)
        sql_tag_condition = ""
        if tag_filter:
            sql_tag_condition = "AND d.Tag = ?"

        sql = f"""
        WITH Cutoffs AS (
            SELECT Category AS Tag, TriggerType, MAX(CAST(ClosedAt AS date)) AS ClosedAt
            FROM dbo.TriggerCaseRegistry
            WHERE Status = 'CLOSED'
            GROUP BY Category, TriggerType
        ),
        DailyData AS (
            SELECT
                d.Tag,
                CAST(d.CreatedDate AS date) AS CreatedDate,
                CONCAT(cal.fiscal_year, ' W', RIGHT('0' + CAST(cal.fiscal_week AS varchar(2)), 2)) AS FiscalWeek,
                d.Progress,
                d.Details,
                CASE 
                    WHEN d.Progress {op} ? 
                    AND (
                        ? = 0 
                        OR 
                        -- Parse 'Count: X' from Details using robust logic
                        TRY_CAST(LTRIM(SUBSTRING(d.Details, CHARINDEX('Count:', d.Details) + 6, LEN(d.Details))) AS INT) >= ?
                    )
                    THEN 1 
                    ELSE 0 
                END AS IsViolation,
                c.ClosedAt AS CutoffClosedAt
            FROM dbo.KPI_Data d
            LEFT JOIN Cutoffs c
                ON d.Tag = c.Tag AND c.TriggerType = ?
            LEFT JOIN dbo.dim_calendar cal
                ON CAST(d.CreatedDate AS date) = cal.[date]
            WHERE d.KPI_Id = ?
              {sql_tag_condition}
              AND CAST(d.CreatedDate AS date) >= ?
              AND CAST(d.CreatedDate AS date) >= DATEADD(day, -?, ?)
              AND (? IS NULL OR ? IS NULL OR cal.fiscal_year < ? OR (cal.fiscal_year = ? AND cal.fiscal_week < ?))
              AND (c.ClosedAt IS NULL OR CAST(d.CreatedDate AS date) > c.ClosedAt)
        ),
        Grouped AS (
            SELECT
                Tag,
                CreatedDate,
                FiscalWeek,
                Progress,
                Details,
                IsViolation,
                ROW_NUMBER() OVER (PARTITION BY Tag ORDER BY CreatedDate) -
                ROW_NUMBER() OVER (PARTITION BY Tag, IsViolation ORDER BY CreatedDate) AS Grp
            FROM DailyData
        ),
        Sequences AS (
            SELECT
                g.Tag,
                g.Grp,
                COUNT(*) AS ConsecCount,
                STRING_AGG(
                    CONCAT(
                        COALESCE(g.FiscalWeek, CONVERT(varchar(10), g.CreatedDate, 23)),
                        '(',
                        FORMAT(g.Progress, '0.0'),
                        CASE 
                            WHEN g.Details LIKE '%Count:%' THEN 
                                CONCAT(', ', LTRIM(RTRIM(SUBSTRING(g.Details, CHARINDEX('Count:', g.Details) + 6, LEN(g.Details)))))
                            ELSE '' 
                        END,
                        ')'
                    ),
                    ', '
                ) WITHIN GROUP (ORDER BY g.CreatedDate) AS Details,
                MAX(g.CreatedDate) AS LastViolationDate
            FROM Grouped g
            WHERE g.IsViolation = 1
            GROUP BY g.Tag, g.Grp
        ),
        LatestPerTag AS (
            SELECT
                s.Tag,
                s.ConsecCount,
                s.Details,
                s.LastViolationDate,
                ROW_NUMBER() OVER (PARTITION BY s.Tag ORDER BY s.LastViolationDate DESC) AS rn
            FROM Sequences s
        )
        SELECT
            l.Tag,
            ? AS TriggerType,
            ? AS TriggerLevel,
            CONCAT(?, ' (当前: ', FORMAT(d.Progress, '0.0'), ', 阈值: ', CAST(? AS varchar(64)), ')') AS TriggerDesc,
            d.Progress AS CurrentValue,
            l.ConsecCount AS ConsecutiveWeeks,
            l.Details AS WeeklyDetails,
            'TRIGGER' AS TriggerStatus,
            SYSUTCDATETIME() AS LastUpdate
        FROM LatestPerTag l
        JOIN DailyData d ON l.Tag = d.Tag AND l.LastViolationDate = d.CreatedDate
        WHERE l.rn = 1
          AND l.ConsecCount >= ?;
        """

        params = [float(threshold), int(min_volume), int(min_volume), str(rule_code), int(kpi_id)]
        if tag_filter:
            params.append(str(tag_filter))
        params.extend(
            [
                str(monitor_start_date),
                int(effective_lookback),
                as_of_date.strftime('%Y-%m-%d'),
                cy,
                cw,
                cy,
                cy,
                cw,
                str(rule_code),
                str(level),
                str(desc_template),
                float(threshold),
                int(consecutive),
            ]
        )

        try:
            cursor.execute(sql, params)
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"执行规则 {rule_code} 失败: {e}\nSQL: {sql}")
            return []

    return []

def suppress_redundant_triggers(triggers, rules_map):
    """
    抑制冗余触发: 同一个 Tag + KPI_Id 下，如果存在 Critical，则抑制 Warning。
    triggers: list of rows (Tag, TriggerType, Level, ...)
    """
    # Group by (Tag, KPI_Id)
    groups = {}
    for row in triggers:
        tag = row[0]
        trigger_type = row[1]
        # Level is at index 2
        
        rule = rules_map.get(trigger_type)
        if not rule: continue
        
        kpi_id = rule['KPI_Id']
        key = (tag, kpi_id)
        
        if key not in groups:
            groups[key] = []
        groups[key].append(row)
    
    final_triggers = []
    
    for key, rows in groups.items():
        # Check if any Critical exists
        # Ensure we compare normalized string
        has_critical = any(r[2].strip().lower() == 'critical' for r in rows)
        
        for row in rows:
            level = row[2].strip().lower()
            # If group has Critical, skip Warning
            if has_critical and level == 'warning':
                logger.info(f"抑制冗余触发: {row[1]} (因存在 Critical 触发)")
                continue
            final_triggers.append(row)
            
    return final_triggers

def _refine_trigger_row(row, rule_code, threshold_val):
    """
    Refine the trigger row for better display visibility.
    Original row: (Tag, TriggerType, Level, Desc, Value, ConsecutiveWeeks, Details, Status, Update)
    """
    tag, trigger_type, level, desc, val_raw, consec_weeks, details, status, update = row
    
    val_float = float(val_raw) if val_raw else 0.0
    
    if rule_code == 'SAFETY_RANK_CRITICAL':
        # Format "1.0" as "1st"
        current_rank_str = _format_ordinal(val_float)
        threshold_str = f"Top {int(float(threshold_val))}"
        
        # Refine Description
        refined_desc = f"Latest Week Rank: {current_rank_str}, Threshold: {threshold_str}"
        
        # Refine Details (FY26 W39(1.0) -> FY26 W39(1st))
        refined_details = details
        # Match digits with optional decimals
        matches = re.findall(r'(\d+(?:\.\d+)?)\)', details)
        for m in matches:
            refined_details = refined_details.replace(f"({m})", f"({_format_ordinal(m)})")
        
        # Truncate to last 4 weeks
        detail_parts = [p.strip() for p in refined_details.split(',') if p.strip()]
        if len(detail_parts) > 4:
            refined_details = ', '.join(detail_parts[-4:])
            
        return (tag, trigger_type, level, refined_desc, current_rank_str, consec_weeks, refined_details, status, update)
        
    elif 'SA_' in rule_code:
        # Format SA as percentage
        sa_val_str = f"{val_float:.1f}%"
        threshold_str = f"< {threshold_val}%"
        
        # Refine Description
        refined_desc = f"Latest Week SA: {sa_val_str}, Threshold: {threshold_str}"
        
        # Refine Details (FY26 W39(88.0, 4000) -> FY26 W39(88.0%, Count: 4000))
        refined_details = details
        # Pattern: (Progress, Count) e.g. (94.7, 4201)
        # We want to keep it simple. The SQL returns (Progress, Count) or (Progress).
        
        # Try to match (Progress, Count)
        # Use a callback function for substitution to be robust
        def repl(m):
            prog = m.group(1)
            cnt = m.group(2)
            return f"({prog}%, Count: {cnt})"

        refined_details = re.sub(r'\((\d+(?:\.\d+)?),\s*(\d+)\)', repl, refined_details)
        
        # Also handle case without count if any: (94.7) -> (94.7%)
        refined_details = re.sub(r'\((\d+(?:\.\d+)?)\)', r'(\1%)', refined_details)
        
        # Truncate to last 4 weeks
        detail_parts = [p.strip() for p in refined_details.split(', FY') if p.strip()] # Split by FY to avoid splitting internal commas if any, though standard format is ", "
        # Actually standard separator is ", ". splitting by ", " might be safe if dates don't have comma.
        # Let's rely on standard split but be careful.
        # Re-construct list properly
        parts = []
        current = ""
        # Simple split by ", F" helps? No.
        # Just use ") ," as delimiter?
        
        # Fallback to simple split by comma if no internal commas expected (current SQL doesn't add internal commas except the one we just formatted)
        # Our formatted string has comma: "94.7%, Count: 4201". 
        # So we CANNOT split by comma!
        
        # Use regex to find all "FY..(...)" blocks
        blocks = re.findall(r'(FY\d+ W\d+\([^)]+\))', refined_details)
        if len(blocks) > 4:
            refined_details = ', '.join(blocks[-4:])
            
        return (tag, trigger_type, level, refined_desc, sa_val_str, consec_weeks, refined_details, status, update)

    return row

def export_kpi_history(conn):
    """导出 KPI 历史数据 (Pivot Table: Fiscal Week x KPI)"""
    logger.info("正在导出 KPI 历史记录...")
    
    # 1. 获取 KPI 定义映射
    kpi_map = {}
    try:
        # KPI_Definition table schema: Id, Name, Description, ...
        kpi_df = pd.read_sql("SELECT Id, Name FROM dbo.KPI_Definition", conn)
        for _, row in kpi_df.iterrows():
            kpi_map[row['Id']] = row['Name']
    except Exception as e:
        logger.warning(f"无法读取 KPI_Definition: {e}")
    
    # Use zero-padded week for correct sorting
    # Include both GLOBAL tags and Safety_Issue_Rank tags
    query = """
    SELECT
        d.KPI_Id,
        d.Tag,
        CAST(d.CreatedDate AS date) AS CreatedDate,
        d.Progress,
        CONCAT(cal.fiscal_year, ' W', RIGHT('0' + CAST(cal.fiscal_week AS varchar(2)), 2)) AS FiscalWeek
    FROM dbo.KPI_Data d
    LEFT JOIN dbo.dim_calendar cal ON CAST(d.CreatedDate AS date) = cal.[date]
    WHERE (d.Tag = 'CZ_Campus' OR d.KPI_Id = (SELECT TOP 1 Id FROM dbo.KPI_Definition WHERE Name = 'Safety_Issue_Rank'))
    ORDER BY CAST(d.CreatedDate AS date)
    """
    
    try:
        df = pd.read_sql(query, conn)
        if df.empty:
            logger.info("KPI 历史数据为空。")
            return

        df['KPI_Name'] = df['KPI_Id'].apply(lambda x: kpi_map.get(x, f"KPI_{x}"))
        
        # Pivot
        pivot_df = df.pivot_table(
            index='FiscalWeek', 
            columns='KPI_Name', 
            values='Progress', 
            aggfunc='mean'
        )
        
        # Sort index (now W01, W02... so it works alphabetically)
        pivot_df.sort_index(inplace=True)
        
        output_path = OUTPUT_DIR / 'kpi_history_matrix.csv'
        pivot_df.to_csv(output_path, encoding='utf-8-sig')
        logger.info(f"KPI 历史矩阵已导出至: {output_path}")
        
    except Exception as e:
        logger.error(f"导出 KPI 历史失败: {e}", exc_info=True)

def run_alert_engine(conn):
    """主执行函数，供 etl_kpi_aggregation 调用"""
    logger.info("启动监控引擎 (ETL组件)...")
    
    try:
        _ensure_monitoring_schema(conn)
        cursor = conn.cursor()

        try:
            _migrate_category_tags(conn)
        except Exception as e:
            logger.warning(f"旧 Tag 迁移失败（将继续运行告警引擎）: {e}")
        
        # 加载规则
        rules = _load_rules_from_csv()
        rules_map = {r['RuleCode']: r for r in rules}
        logger.info(f"加载了 {len(rules)} 条监控规则")

        as_of_date = _get_as_of_date()
        if as_of_date is None:
            as_of_date = datetime.now().date()

        try:
            _sync_case_registry_with_planner_tasks(conn)
        except Exception as e:
            logger.warning(f"Planner 任务同步失败（将继续运行告警引擎）: {e}")

        try:
            _deduplicate_open_auto_cases(conn, as_of_date)
        except Exception as e:
            logger.warning(f"重复触发去重失败（将继续运行告警引擎）: {e}")

        cases = _load_case_registry_from_db(conn)
        used_a3_ids = {info.get('A3Id') for info in cases.values() if info.get('A3Id')}

        open_case_keys = set()
        open_case_map = {}
        for (category, trigger_type, _opened_key), info in cases.items():
            eff = _effective_case_status(info.get('Status'), info.get('OpenedAt'), info.get('ClosedAt'), as_of_date)
            if eff == 'OPEN':
                open_case_keys.add((category, trigger_type))
                prev = open_case_map.get((category, trigger_type))
                if prev is None:
                    open_case_map[(category, trigger_type)] = info
                else:
                    prev_has_planner = bool((prev.get('PlannerTaskId') or '').strip())
                    cur_has_planner = bool((info.get('PlannerTaskId') or '').strip())
                    if cur_has_planner and not prev_has_planner:
                        open_case_map[(category, trigger_type)] = info
                    elif cur_has_planner == prev_has_planner:
                        if str(info.get('A3Id') or '') > str(prev.get('A3Id') or ''):
                            open_case_map[(category, trigger_type)] = info

        # 执行检查 (配置驱动)
        raw_results = []
        for rule in rules:
            # logger.info(f"正在检查规则: {rule['RuleCode']} ({rule['KPI_Name']})")
            rows = check_generic_rule(cursor, rule)
            # Refine rows immediately
            refined_rows = []
            for r in rows:
                refined_rows.append(_refine_trigger_row(r, rule['RuleCode'], rule['ThresholdValue']))
            raw_results.extend(refined_rows)
        
        # 抑制冗余触发 (Critical supersedes Warning)
        results = suppress_redundant_triggers(raw_results, rules_map)

        # 分配 A3 ID 并准备数据
        results_with_id = []
        created_count = 0

        for row in results:
            category = row[0]
            trigger_type = row[1]
            details = row[6] # Shifted from 5 to 6
            trigger_start = _extract_min_date_from_details(details)

            # 获取规则配置
            rule_cfg = rules_map.get(trigger_type, {})
            action_type = rule_cfg.get('ActionType', 'Create_A3')

            existing = open_case_map.get((category, trigger_type))
            if existing is not None and existing.get('A3Id'):
                a3_id = str(existing.get('A3Id'))
                opened_at = existing.get('OpenedAt')
                opened_at_str = _safe_strftime(opened_at) if opened_at is not None else (_safe_strftime(trigger_start) if trigger_start else datetime.now().strftime('%Y-%m-%d'))
                planner_task_id = (existing.get('PlannerTaskId') or '').strip()
                notes = (existing.get('Notes') or '').strip() or f"Auto Triggered ({action_type})"
                _upsert_case_registry_row(
                    conn,
                    a3_id=a3_id,
                    category=category,
                    trigger_type=trigger_type,
                    status='OPEN',
                    opened_at=opened_at_str,
                    closed_at=None,
                    planner_task_id=planner_task_id,
                    notes=notes,
                    original_level=row[2],
                    original_desc=row[3],
                    original_details=row[6], # Shifted
                    original_value=str(row[4]), # Progress
                    source='AUTO'
                )
                results_with_id.append((a3_id,) + tuple(row))
                used_a3_ids.add(a3_id)
            else:
                # Fallback date for ID generation if trigger_start is missing
                id_date = _safe_strftime(trigger_start) if trigger_start else datetime.now().strftime('%Y%m%d').replace('-', '')
                if '-' in id_date: id_date = id_date.replace('-', '')
                
                a3_id = _alloc_a3_id(id_date, conn, used_a3_ids)
                results_with_id.append((a3_id,) + tuple(row))
                used_a3_ids.add(a3_id)
                created_count += 1

                opened_at = _safe_strftime(trigger_start) if trigger_start else datetime.now().strftime('%Y-%m-%d')

                cursor.execute(
                    "SELECT TOP 1 A3Id "
                    "FROM dbo.TriggerCaseRegistry "
                    "WHERE Category = ? AND TriggerType = ? AND Status = 'CLOSED' "
                    "ORDER BY ISNULL(ClosedAt, '1900-01-01') DESC, UpdatedAt DESC",
                    (str(category), str(trigger_type)),
                )
                prev_closed = cursor.fetchone()
                reopened_from = (prev_closed[0] if prev_closed else '')
                notes = f"Auto Triggered ({action_type})"
                if reopened_from:
                    notes = notes + f" | Reopened from {reopened_from}"

                _upsert_case_registry_row(
                    conn,
                    a3_id=a3_id,
                    category=category,
                    trigger_type=trigger_type,
                    status='OPEN',
                    opened_at=opened_at,
                    closed_at=None,
                    planner_task_id='',
                    notes=notes,
                    original_level=row[2],
                    original_desc=row[3],
                    original_details=row[6], # Shifted
                    original_value=str(row[4]), # Progress
                    source='AUTO'
                )
        
        conn.commit() # Commit all upserts
        logger.info(f"监控完成: 新增触发 {created_count} 个。")
        
        # 导出结果 CSV/TSV (兼容旧 guide 的字段顺序，且额外保留 Owner/ActionType/DataSource)
        export_csv_path = OUTPUT_DIR / 'kpi_trigger_results.csv'
        export_tsv_path = OUTPUT_DIR / 'kpi_trigger_results.tsv'

        current_a3_ids = {str(r[0]).strip() for r in results_with_id if r and r[0]}

        cursor.execute(
            "SELECT "
            "  A3Id, Category, TriggerType, Status, OpenedAt, ClosedAt, PlannerTaskId, Notes, "
            "  OriginalLevel, OriginalDesc, OriginalValue, OriginalDetails, UpdatedAt "
            "FROM dbo.TriggerCaseRegistry "
            "ORDER BY UpdatedAt DESC"
        )
        all_cases = cursor.fetchall()

        if all_cases:
            data_rows = []
            for (
                a3_id,
                category,
                trigger_type,
                case_status,
                opened_at,
                closed_at,
                planner_task_id,
                notes,
                original_level,
                original_desc,
                original_value,
                original_details,
                updated_at,
            ) in all_cases:
                a3_id = (a3_id or '').strip()
                if not a3_id:
                    continue

                trigger_type = (trigger_type or '').strip()
                rule_cfg = rules_map.get(trigger_type, {})

                trigger_name = (rule_cfg.get('KPI_Name') or '').strip() or trigger_type
                trigger_level = (original_level or '').strip() or (rule_cfg.get('TriggerLevel') or '').strip()
                trigger_desc = (original_desc or '').strip()
                
                weekly_details = (original_details or '').strip()
                # Calculate ConsecutiveWeeks from details count (number of ")" parentheses)
                # Fallback to original_value if details empty (though unlikely for valid trigger)
                c_weeks_count = weekly_details.count(')')
                consecutive_weeks = str(c_weeks_count) if c_weeks_count > 0 else (original_value or '').strip()
                last_update = str(updated_at or '').strip()

                case_status_norm = _normalize_status(case_status)
                is_current = 'Yes' if a3_id in current_a3_ids else 'No'

                reopened_flag = 'reopened from' in str(notes or '').strip().lower()
                has_task = bool(str(planner_task_id or '').strip())

                if case_status_norm == 'CLOSED':
                    trigger_status = 'CLOSED'
                elif has_task:
                    trigger_status = 'OPEN'
                elif reopened_flag:
                    trigger_status = 'REOPENED'
                else:
                    trigger_status = 'TRIGGER'

                data_rows.append(
                    [
                        a3_id,
                        (category or '').strip(),
                        trigger_type,
                        trigger_name,
                        trigger_level,
                        trigger_desc,
                        consecutive_weeks,
                        weekly_details,
                        trigger_status,
                        last_update,
                        case_status_norm,
                        is_current,
                        str(opened_at or '').strip(),
                        str(closed_at or '').strip(),
                        str(planner_task_id or '').strip(),
                        (rule_cfg.get('Owner') or '').strip(),
                        (rule_cfg.get('ActionType') or '').strip(),
                        (rule_cfg.get('DataSource') or '').strip(),
                    ]
                )

            header = [
                'A3Id',
                'Category',
                'TriggerType',
                'TriggerName',
                'TriggerLevel',
                'TriggerDesc',
                'ConsecutiveWeeks',
                'WeeklyDetails',
                'TriggerStatus',
                'LastUpdate',
                'CaseStatus',
                'IsCurrentTrigger',
                'OpenedAt',
                'ClosedAt',
                'PlannerTaskId',
                'Owner',
                'ActionType',
                'DataSource',
            ]

            with open(export_csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(header)
                writer.writerows(data_rows)
            logger.info(f"结果已导出至: {export_csv_path} (CSV)")

            with open(export_tsv_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f, delimiter='\t')
                writer.writerow(header)
                writer.writerows(data_rows)
            logger.info(f"结果已导出至: {export_tsv_path} (TSV)")
        
        # 导出 KPI 历史矩阵 (可选)
        # export_kpi_history(conn)

    except Exception as e:
        logger.error(f"监控引擎运行失败: {e}", exc_info=True)


def _sync_case_registry_with_planner_tasks(conn: pyodbc.Connection) -> None:
    """同步 Planner tasks 与 TriggerCaseRegistry（避免已建 Planner task 的 trigger 被重复创建）"""
    cursor = conn.cursor()
    if _is_sqlserver_conn(conn):
        try:
            cursor.execute("SELECT 1 WHERE OBJECT_ID('dbo.planner_tasks','U') IS NOT NULL")
            if cursor.fetchone() is None:
                return
            cursor.execute("SELECT 1 WHERE OBJECT_ID('dbo.TriggerCaseRegistry','U') IS NOT NULL")
            if cursor.fetchone() is None:
                return
        except Exception:
            return

        cursor.execute(
            "SELECT TaskId, TaskName, Status, CompletedDate "
            "FROM dbo.planner_tasks "
            "WHERE TaskName IS NOT NULL AND TaskName LIKE '%A3-%'"
        )
        rows = cursor.fetchall()

        updated = 0
        for task_id, task_name, status, completed_date in rows:
            if not task_name:
                continue
            m = re.search(r'A3-\d{8}-\d{4}', str(task_name))
            if not m:
                continue
            a3_id = m.group(0)

            s = (status or '').strip().lower()
            is_done = (s in ('completed', 'closed')) or (completed_date is not None and str(completed_date).strip() != '')

            if is_done:
                close_date = completed_date
                if close_date is None or str(close_date).strip() == '':
                    close_date = datetime.now().strftime('%Y-%m-%d')

                cursor.execute(
                    "UPDATE dbo.TriggerCaseRegistry "
                    "SET PlannerTaskId = COALESCE(NULLIF(PlannerTaskId,''), ?), "
                    "    Status = 'CLOSED', "
                    "    ClosedAt = COALESCE(ClosedAt, ?), "
                    "    UpdatedAt = SYSUTCDATETIME() "
                    "WHERE A3Id = ? "
                    "  AND (Source IS NULL OR LTRIM(RTRIM(Source)) = '' OR UPPER(Source) = 'AUTO')",
                    (str(task_id), str(close_date), str(a3_id)),
                )
            else:
                cursor.execute(
                    "UPDATE dbo.TriggerCaseRegistry "
                    "SET PlannerTaskId = COALESCE(NULLIF(PlannerTaskId,''), ?), "
                    "    Status = 'OPEN', "
                    "    UpdatedAt = SYSUTCDATETIME() "
                    "WHERE A3Id = ? "
                    "  AND (Source IS NULL OR LTRIM(RTRIM(Source)) = '' OR UPPER(Source) = 'AUTO')",
                    (str(task_id), str(a3_id)),
                )

            if cursor.rowcount > 0:
                updated += cursor.rowcount

        if updated:
            conn.commit()
        return

    return


def _deduplicate_open_auto_cases(conn: pyodbc.Connection, as_of_date) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT 1 WHERE OBJECT_ID('dbo.TriggerCaseRegistry','U') IS NOT NULL")
        if cursor.fetchone() is None:
            return
    except Exception:
        return

    cursor.execute(
        "SELECT A3Id, Category, TriggerType, PlannerTaskId "
        "FROM dbo.TriggerCaseRegistry "
        "WHERE Status='OPEN' AND (Source IS NULL OR LTRIM(RTRIM(Source))='' OR UPPER(Source)='AUTO')"
    )
    rows = cursor.fetchall()

    groups = {}
    for a3_id, category, trigger_type, planner_task_id in rows:
        key = ((category or '').strip(), (trigger_type or '').strip())
        if not key[0] or not key[1] or not a3_id:
            continue
        groups.setdefault(key, []).append((str(a3_id), (str(planner_task_id) if planner_task_id is not None else '').strip()))

    close_date = as_of_date.strftime('%Y-%m-%d') if as_of_date is not None else datetime.now().strftime('%Y-%m-%d')
    to_close = []
    for (category, trigger_type), items in groups.items():
        if len(items) <= 1:
            continue
        with_planner = [i for i in items if i[1]]
        keep = max(with_planner, key=lambda x: x[0])[0] if with_planner else max(items, key=lambda x: x[0])[0]
        for a3_id, _pt in items:
            if a3_id != keep:
                to_close.append((a3_id, keep))

    for a3_id, keep_id in to_close:
        cursor.execute(
            "UPDATE dbo.TriggerCaseRegistry "
            "SET Status='CLOSED', "
            "    ClosedAt=COALESCE(ClosedAt, ?), "
            "    Notes=COALESCE(NULLIF(Notes,''), '') + ' | Auto closed (superseded by ' + ? + ')', "
            "    UpdatedAt=SYSUTCDATETIME() "
            "WHERE A3Id=? AND Status='OPEN' "
            "  AND (Source IS NULL OR LTRIM(RTRIM(Source))='' OR UPPER(Source)='AUTO')",
            (close_date, str(keep_id), str(a3_id)),
        )

    if to_close:
        conn.commit()

if __name__ == "__main__":
    conn = get_db_connection()
    if conn:
        run_alert_engine(conn)
        conn.close()
