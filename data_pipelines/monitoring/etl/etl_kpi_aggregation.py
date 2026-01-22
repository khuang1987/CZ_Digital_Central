"""
KPI 数据聚合脚本
功能：从 raw_mes, raw_sfc, raw_sap_labor_hours 等原始表计算 KPI，并写入 KPI_Data 中间表。
目前包含:
1. KPI #1: Lead Time (制造周期) - 基于 SFC 批次
2. KPI #2: Schedule Attainment (达成率) - 基于 MES 工序 (v_mes_metrics: PT vs ST + 8h 容差)
"""
import pyodbc
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CAMPUS_TAG = 'CZ_Campus'
PLANT_TAGS = ('CKH', 'CZM')

def get_db_connection():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        r"SERVER=localhost\SQLEXPRESS;"
        "DATABASE=mddap_v2;"
        "Trusted_Connection=yes;"
        "Encrypt=no;"
    )
    return pyodbc.connect(conn_str, autocommit=False)


def _ensure_monitoring_schema(conn):
    # SQL Server schema is managed outside of this script.
    cursor = conn.cursor()
    cursor.execute("SELECT TOP 1 1 FROM dbo.KPI_Definition")
    cursor.fetchone()

def aggregate_lead_time(conn, run_date=None):
    """
    KPI #1: Lead Time (Average Manufacturing Lead Time)
    来源: raw_sfc
    逻辑: 
      1. 按 BatchNumber 分组
      2. 计算 StartTime (Min TrackIn) 和 EndTime (Max TrackOut)
      3. 筛选 EndTime 在计算日期范围内的批次
      4. 计算 Duration (Hours)
      5. 按 CFN (产品系列) 聚合取平均值
    """
    logger.info("正在聚合 KPI #1 (Lead Time)...")
    
    date_filter = ""
    if run_date:
        # 只计算指定日期完成的批次
        date_filter = f"HAVING CONVERT(varchar(10), MAX(TrackOutTime), 23) = '{run_date}'"
    
    query = f"""
    WITH BatchStats AS (
        SELECT 
            BatchNumber,
            CFN,
            MIN(TrackInTime) as StartTime,
            MAX(TrackOutTime) as EndTime
        FROM dbo.raw_sfc
        WHERE TrackInTime IS NOT NULL AND TrackOutTime IS NOT NULL
        GROUP BY BatchNumber, CFN
        {date_filter}
    )
    SELECT 
        CONVERT(varchar(10), EndTime, 23) as CreatedDate,
        CFN as Tag,
        AVG(CAST(DATEDIFF_BIG(SECOND, StartTime, EndTime) AS FLOAT) / 3600.0 / 24.0) as AvgDurationDays
    FROM BatchStats
    WHERE StartTime < EndTime
    GROUP BY CONVERT(varchar(10), EndTime, 23), CFN
    """
    
    try:
        df = pd.read_sql(query, conn)
        if df.empty:
            logger.info("  KPI #1 (Lead Time): 无数据。")
            return

        cursor = conn.cursor()
        count = 0
        for _, row in df.iterrows():
            if pd.isna(row['AvgDurationDays']): continue
            
            cursor.execute("""
                INSERT INTO dbo.KPI_Data (KPI_Id, Tag, CreatedDate, Progress)
                VALUES (1, ?, ?, ?)
            """, (row['Tag'], row['CreatedDate'], row['AvgDurationDays']))
            count += 1
        
        logger.info(f"  KPI #1 (Lead Time): 已插入 {count} 条记录。")
    except Exception as e:
        logger.error(f"  KPI #1 聚合失败: {e}")

def calculate_due_time(start_time, plan_hours, calendar_df):
    """
    按工作日逐天累加工作时间，跳过非工作日
    
    参数:
        start_time: 开始时间（datetime）
        plan_hours: 计划小时数（float）
        calendar_df: 工作日日历DataFrame
        
    返回:
        due_time: 应完工时间（datetime）
    """
    from datetime import timedelta
    
    current_time = start_time
    remaining_hours = plan_hours
    
    while remaining_hours > 0:
        current_date = current_time.date()
        
        # 查询日历表判断是否为工作日
        is_workday = calendar_df[
            calendar_df['date'] == current_date.strftime('%Y-%m-%d')
        ]['is_workday'].values
        
        if len(is_workday) > 0 and is_workday[0] == 1:
            # 工作日：减少剩余工时（每天24小时连续生产）
            daily_hours = min(remaining_hours, 24)
            remaining_hours -= daily_hours
        # 非工作日：不减少工时，但时间推进
        
        # 推进到下一天
        current_time += timedelta(days=1)
    
    return current_time


def aggregate_schedule_attainment(conn, run_date=None):
    """
    KPI #2: Schedule Attainment (达成率)
    来源: v_mes_metrics view (operation-level)
    逻辑:
      1. 仅统计具备标准时间和可计算PT的工序记录（IsOnTime 不为空）
      2. 工序级判断：PT(d) vs ST(d) + 8h 容差（该逻辑已在 v_mes_metrics 中计算为 IsOnTime/IsOverdue）
      3. 按工厂和日期聚合：SA = OnTime工序数 / 总工序数 × 100
    """
    logger.info("正在聚合 KPI #2 (Schedule Attainment)...")
    
    try:
        operation_query = """
        SELECT
            Plant,
            TrackOutDate as CreatedDate,
            IsOnTime
        FROM dbo.v_mes_metrics
        WHERE TrackOutDate IS NOT NULL
          AND IsOnTime IS NOT NULL
        """

        if run_date:
            operation_query += f" AND TrackOutDate = '{run_date}'"

        df_ops = pd.read_sql(operation_query, conn)

        if df_ops.empty:
            if run_date:
                logger.info(f"  KPI #2 (SA): {run_date} 无有效工序数据（可能缺少ST/PT）。")
            else:
                logger.info("  KPI #2 (SA): 无有效工序数据。")
            return

        logger.info(f"  找到 {len(df_ops)} 条可判定OnTime/Overdue的工序记录")

        # 按工厂和日期聚合
        logger.info("  按工厂和日期聚合SA数据...")
        sa_aggregated = df_ops.groupby(['Plant', 'CreatedDate']).agg(
            TotalOps=('IsOnTime', 'count'),
            OnTimeOps=('IsOnTime', 'sum')
        ).reset_index()
        
        # 计算SA百分比
        sa_aggregated['SA_Percent'] = (sa_aggregated['OnTimeOps'] / sa_aggregated['TotalOps'] * 100).round(2)
        
        # 保存到KPI_Data表
        cursor = conn.cursor()
        count = 0
        
        for _, row in sa_aggregated.iterrows():
            cursor.execute("""
                INSERT INTO dbo.KPI_Data (KPI_Id, Tag, CreatedDate, Progress)
                VALUES (2, ?, ?, ?)
            """, (row['Plant'], row['CreatedDate'], round(row['SA_Percent'], 2)))
            count += 1
            
        logger.info(f"  KPI #2 (SA): 已插入 {count} 条工厂记录。")
        
        # 计算并插入 GLOBAL 数据（按工序加权平均）
        logger.info("  计算全局SA数据...")
        global_sa = sa_aggregated.groupby('CreatedDate').apply(
            lambda x: pd.Series({
                'GlobalSA': (x['OnTimeOps'].sum() / x['TotalOps'].sum() * 100).round(2)
            })
        ).reset_index()
        
        global_count = 0
        for _, row in global_sa.iterrows():
            cursor.execute("""
                INSERT INTO dbo.KPI_Data (KPI_Id, Tag, CreatedDate, Progress)
                VALUES (2, 'GLOBAL_DAILY', ?, ?)
            """, (row['CreatedDate'], row['GlobalSA']))
            global_count += 1
            
        logger.info(f"  KPI #2 (SA): 已插入 {global_count} 条全局(日)记录。")
        
        # 输出统计信息
        total_ops = sa_aggregated['TotalOps'].sum()
        total_ontime = sa_aggregated['OnTimeOps'].sum()
        overall_sa = (total_ontime / total_ops * 100) if total_ops > 0 else 0
        logger.info(f"  总体SA达成率: {overall_sa:.2f}% ({total_ontime}/{total_ops})")
        
    except Exception as e:
        logger.error(f"  KPI #2 聚合失败: {e}")
        import traceback
        traceback.print_exc()

def aggregate_global_lead_time_weekly(conn):
    """
    KPI #1 Weekly: 园区（CZ_Campus）及工厂维度（CKH/CZM）的周平均制造周期
    Tag: 'CZ_Campus', 'CKH', 'CZM'
    Aggregation: Based on Fiscal Week from dim_calendar
    """
    logger.info("正在聚合 KPI #1 Global Weekly (Lead Time)...")
    
    query = f"""
    WITH BatchStats AS (
        SELECT 
            BatchNumber,
            MIN(TrackInTime) as StartTime,
            MAX(TrackOutTime) as EndTime
        FROM dbo.raw_sfc
        WHERE TrackInTime IS NOT NULL AND TrackOutTime IS NOT NULL
        GROUP BY BatchNumber
    ),
    BatchPlant AS (
        SELECT
            BatchNumber,
            MAX(NULLIF(LTRIM(RTRIM(Plant)), '')) as Plant
        FROM dbo.raw_mes
        WHERE Plant IS NOT NULL AND LTRIM(RTRIM(Plant)) <> ''
        GROUP BY BatchNumber
    ),
    BatchWithFiscal AS (
        SELECT
            bs.DurationDays,
            bs.BatchNumber,
            bp.Plant as Plant,
            cal.fiscal_year,
            cal.fiscal_week,
            cal.date as cal_date
        FROM (
            SELECT 
                BatchNumber,
                CAST(DATEDIFF_BIG(SECOND, StartTime, EndTime) AS FLOAT) / 3600.0 / 24.0 as DurationDays,
                CONVERT(varchar(10), EndTime, 23) as EndDate
            FROM BatchStats
            WHERE StartTime < EndTime
        ) bs
        JOIN dbo.dim_calendar cal ON bs.EndDate = cal.date
        LEFT JOIN BatchPlant bp ON bs.BatchNumber = bp.BatchNumber
    )
    SELECT
        MIN(cal_date) as CreatedDate,
        '{CAMPUS_TAG}' as Tag,
        AVG(DurationDays) as AvgDurationDays
    FROM BatchWithFiscal
    GROUP BY fiscal_year, fiscal_week

    UNION ALL

    SELECT
        MIN(cal_date) as CreatedDate,
        Plant as Tag,
        AVG(DurationDays) as AvgDurationDays
    FROM BatchWithFiscal
    WHERE Plant IN ('CKH', 'CZM')
    GROUP BY Plant, fiscal_year, fiscal_week
    """
    
    try:
        df = pd.read_sql(query, conn)
        if df.empty:
            logger.info("  KPI #1 Global: 无数据。")
            return

        cursor = conn.cursor()
        count = 0
        for _, row in df.iterrows():
            if pd.isna(row['AvgDurationDays']): continue
            cursor.execute("""
                INSERT INTO dbo.KPI_Data (KPI_Id, Tag, CreatedDate, Progress)
                VALUES (1, ?, ?, ?)
            """, (row['Tag'], row['CreatedDate'], row['AvgDurationDays']))
            count += 1
        logger.info(f"  KPI #1 Global: 已插入 {count} 条周记录。")
    except Exception as e:
        logger.error(f"  KPI #1 Global 聚合失败: {e}")

def aggregate_global_sa_weekly(conn):
    """
    KPI #2 Weekly: 园区（CZ_Campus）及工厂维度（CKH/CZM）的周平均达成率
    Tag: 'CZ_Campus', 'CKH', 'CZM'
    Aggregation: Based on Fiscal Week from dim_calendar
    """
    logger.info("正在聚合 KPI #2 Global Weekly (SA)...")
    
    query = f"""
    WITH ops AS (
        SELECT
            vm.Plant,
            CONVERT(varchar(10), vm.TrackOutDate, 23) AS TrackOutDate,
            CASE
                WHEN vm.CompletionStatus = 'OnTime' THEN 1
                WHEN vm.CompletionStatus = 'Overdue' THEN 0
                ELSE NULL
            END AS IsOnTime
        FROM dbo.v_mes_metrics vm
        WHERE vm.TrackOutDate IS NOT NULL
          AND vm.CompletionStatus IN ('OnTime', 'Overdue')
    )
    SELECT
        MIN(cal.date) as CreatedDate,
        '{CAMPUS_TAG}' as Tag,
        COUNT(ops.IsOnTime) as TotalOps,
        SUM(ops.IsOnTime) as OnTimeOps
    FROM ops
    JOIN dbo.dim_calendar cal ON ops.TrackOutDate = cal.date
    GROUP BY cal.fiscal_year, cal.fiscal_week

    UNION ALL

    SELECT
        MIN(cal.date) as CreatedDate,
        ops.Plant as Tag,
        COUNT(ops.IsOnTime) as TotalOps,
        SUM(ops.IsOnTime) as OnTimeOps
    FROM ops
    JOIN dbo.dim_calendar cal ON ops.TrackOutDate = cal.date
    WHERE ops.Plant IN ('CKH', 'CZM')
    GROUP BY ops.Plant, cal.fiscal_year, cal.fiscal_week
    """
    
    try:
        df = pd.read_sql(query, conn)
        if df.empty:
            logger.info("  KPI #2 Global: 无数据。")
            return

        cursor = conn.cursor()
        count = 0
        for _, row in df.iterrows():
            total_ops = row['TotalOps'] or 0
            ontime_ops = row['OnTimeOps'] or 0

            if total_ops > 0:
                sa_pct = (ontime_ops / total_ops) * 100
            else:
                sa_pct = 0

            sa_pct = min(max(sa_pct, 0), 100)

            cursor.execute("""
                INSERT INTO dbo.KPI_Data (KPI_Id, Tag, CreatedDate, Progress)
                VALUES (2, ?, ?, ?)
            """, (row['Tag'], row['CreatedDate'], sa_pct))
            count += 1
        logger.info(f"  KPI #2 Global: 已插入 {count} 条周记录。")
    except Exception as e:
        logger.error(f"  KPI #2 Global 聚合失败: {e}")

import yaml

# ... (Previous imports)

def load_label_cleaning_config():
    """Load label cleaning config"""
    config_path = Path(__file__).parent.parent.parent / 'sources' / 'planner' / 'config' / 'label_cleaning.yaml'
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.warning(f"Failed to load label cleaning config: {e}. Using defaults.")
        return {"mappings": {}, "exclusions": []}

def aggregate_safety_issue_rank_weekly(conn):
    """
    KPI #3 Global Weekly: Safety Issue Label Ranks
    KPI_Name: 'Safety_Issue_Rank'
    Tag: Label Name (e.g., 'Safety_Electrical')
    Progress: Rank (1, 2, 3...)
    
    Logic:
    1. Use cleaned labels from planner_task_labels table
    2. Filter tasks where BucketName = '安全' and IsExcluded = 0
    3. Group by Fiscal Week and CleanedLabel
    4. Calculate Rank based on Count desc
    5. Insert Rank into KPI_Data
    """
    logger.info("正在聚合 KPI #3 Global Weekly (Safety Issue Rank)...")
    
    # Query cleaned labels directly from planner_task_labels
    # Filter by fiscal week start date >= 2026-01-01, not task creation date
    query = """
    WITH WeekStarts AS (
        SELECT fiscal_year, fiscal_week, MIN(date) as week_start
        FROM dbo.dim_calendar
        WHERE date >= '2026-01-01'
        GROUP BY fiscal_year, fiscal_week
    )
    SELECT 
        l.CleanedLabel as Label,
        cal.fiscal_year,
        cal.fiscal_week,
        ws.week_start as week_start_date
    FROM dbo.planner_task_labels l
    JOIN dbo.planner_tasks t ON l.TaskId = t.TaskId
    JOIN dbo.dim_calendar cal ON CONVERT(varchar(10), t.CreatedDate, 23) = cal.date
    JOIN WeekStarts ws ON cal.fiscal_year = ws.fiscal_year AND cal.fiscal_week = ws.fiscal_week
    WHERE t.BucketName = N'安全' 
      AND l.IsExcluded = 0
    """
    
    try:
        df = pd.read_sql(query, conn)
        if df.empty:
            logger.info("  KPI #3 Safety: 无数据。")
            return
            
        # Count and Rank
        # Group by Week and Label
        df_counts = df.groupby(['fiscal_year', 'fiscal_week', 'week_start_date', 'Label']).size().reset_index(name='Count')
        
        # Calculate Rank within each week
        df_counts['Rank'] = df_counts.groupby(['fiscal_year', 'fiscal_week'])['Count'].rank(method='min', ascending=False)
        
        # Get KPI ID for Safety_Issue_Rank
        kpi_id_row = None
        try:
            c2 = conn.cursor()
            c2.execute("SELECT TOP 1 Id FROM dbo.KPI_Definition WHERE Name = 'Safety_Issue_Rank'")
            kpi_id_row = c2.fetchone()
        except Exception:
            kpi_id_row = None
        if not kpi_id_row:
            logger.error("  KPI #3 Safety: KPI_Definition 'Safety_Issue_Rank' not found.")
            return
        kpi_id = kpi_id_row[0]
        
        # Insert into KPI_Data
        cursor = conn.cursor()
        count = 0
        for _, row in df_counts.iterrows():
            # Tag = Label Name
            # Progress = Rank
            cursor.execute("""
                INSERT INTO dbo.KPI_Data (KPI_Id, Tag, CreatedDate, Progress)
                VALUES (?, ?, ?, ?)
            """, (kpi_id, row['Label'], row['week_start_date'], round(row['Rank'], 2)))
            count += 1
            
        logger.info(f"  KPI #3 Safety: 已插入 {count} 条排名记录。")
        
    except Exception as e:
        logger.error(f"  KPI #3 Safety 聚合失败: {e}")

def generate_reports(conn):
    """
    Generate KPI reports:
    1. kpi_global_matrix.csv - Global KPI matrix (Lead Time & SA)
    2. pareto_top_matrix.csv - Safety Issue Rank matrix
    3. kpi_trigger_results.csv - Active triggers (CSV)
    4. kpi_trigger_results.tsv - Active triggers (TSV)
    """
    logger.info("正在生成 KPI 报表...")
    
    # 1. Lead Time 和 Schedule Attainment 矩阵（园区标签）
    query_global = """
    SELECT 
        CONCAT(cal.fiscal_year, ' W', RIGHT(CONCAT('0', cal.fiscal_week), 2)) as FiscalWeek,
        d.KPI_Id,
        d.Progress
    FROM dbo.KPI_Data d
    JOIN dbo.dim_calendar cal ON CONVERT(varchar(10), d.CreatedDate, 23) = cal.date
    WHERE d.Tag = 'CZ_Campus'
      AND d.KPI_Id IN (1, 2)
    ORDER BY cal.fiscal_year, cal.fiscal_week
    """
    df_global = pd.read_sql(query_global, conn)
    
    # 透视成矩阵
    global_matrix = df_global.pivot_table(
        index='FiscalWeek',
        columns='KPI_Id',
        values='Progress'
    ).rename(columns={
        1: 'Lead_Time (d)',
        2: 'Schedule_Attainment'
    }).reset_index()
    
    # 格式化
    if 'Lead_Time (d)' in global_matrix.columns:
        global_matrix['Lead_Time (d)'] = global_matrix['Lead_Time (d)'].round(2)
    if 'Schedule_Attainment' in global_matrix.columns:
        global_matrix['Schedule_Attainment'] = global_matrix['Schedule_Attainment'].round(2)
    
    # 保存全局 KPI 矩阵
    global_output = Path(__file__).parent.parent / 'output' / 'kpi_global_matrix.csv'
    global_output.parent.mkdir(parents=True, exist_ok=True)
    global_matrix.to_csv(global_output, index=False, encoding='utf-8-sig')
    logger.info(f"全局 KPI 矩阵已保存至: {global_output}")
    
    # 2. 安全标签矩阵 (使用 pareto_top_matrix.csv 文件名)
    query_safety = """
    SELECT 
        CONCAT(cal.fiscal_year, ' W', RIGHT(CONCAT('0', cal.fiscal_week), 2)) as FiscalWeek,
        d.Tag as SafetyLabel,
        d.Progress as Rank
    FROM dbo.KPI_Data d
    JOIN dbo.dim_calendar cal ON CONVERT(varchar(10), d.CreatedDate, 23) = cal.date
    WHERE d.KPI_Id = (SELECT TOP 1 Id FROM dbo.KPI_Definition WHERE Name = 'Safety_Issue_Rank')
    ORDER BY cal.fiscal_year, cal.fiscal_week, d.Progress
    """
    df_safety = pd.read_sql(query_safety, conn)
    
    safety_matrix = df_safety.pivot_table(
        index='FiscalWeek',
        columns='SafetyLabel',
        values='Rank',
        fill_value=''
    ).reset_index()
    
    safety_output = Path(__file__).parent.parent / 'output' / 'pareto_top_matrix.csv'
    safety_matrix.to_csv(safety_output, index=False, encoding='utf-8-sig')
    logger.info(f"安全排名矩阵已保存至: {safety_output}")
    
    # 3. 分析触发条件
    all_triggers = []
    
    # Lead Time 触发（连续超过24小时）
    lt_triggers = check_lead_time_triggers(global_matrix)
    all_triggers.extend(lt_triggers)
    
    # Schedule Attainment 触发（连续低于95%）
    sa_triggers = check_schedule_attainment_triggers(global_matrix)
    all_triggers.extend(sa_triggers)
    
    # 安全触发（连续3周前3）
    safety_triggers = check_safety_triggers(safety_matrix)
    all_triggers.extend(safety_triggers)
    
    # 保存所有触发结果
    if all_triggers:
        df_triggers = pd.DataFrame(all_triggers)
        
        # 保存为 CSV
        triggers_csv = Path(__file__).parent.parent / 'output' / 'kpi_trigger_results.csv'
        df_triggers.to_csv(triggers_csv, index=False, encoding='utf-8-sig')
        logger.info(f"触发结果已保存至: {triggers_csv} (CSV)")
        
        # 保存为 TSV
        triggers_tsv = Path(__file__).parent.parent / 'output' / 'kpi_trigger_results.tsv'
        df_triggers.to_csv(triggers_tsv, index=False, sep='\t', encoding='utf-8-sig')
        logger.info(f"触发结果已保存至: {triggers_tsv} (TSV)")
        
        # 显示当前活跃的触发
        logger.info("=== 当前活跃的触发警报 ===")
        for trigger in all_triggers:
            if trigger['Status'] == 'ACTIVE':
                logger.info(f"类型: {trigger['KPI_Type']} | 标签: {trigger['Tag']} | 描述: {trigger['Description']}")

def check_lead_time_triggers(matrix):
    """检查 Lead Time 触发（连续超过24小时）"""
    triggers = []
    if 'Lead_Time' not in matrix.columns:
        return triggers
    
    consecutive = 0
    for idx, row in matrix.iterrows():
        if pd.notna(row['Lead_Time']) and row['Lead_Time'] > 24:
            consecutive += 1
            if consecutive >= 3:
                triggers.append({
                    'KPI_Type': 'Lead_Time',
                    'Tag': 'CZ_Campus',
                    'Level': 'Critical',
                    'Description': '制造周期连续3周超过24小时',
                    'Details': f"当前值: {row['Lead_Time']:.1f}小时",
                    'Status': 'ACTIVE',
                    'TriggerWeek': row['FiscalWeek']
                })
                break
        else:
            consecutive = 0
    return triggers

def check_schedule_attainment_triggers(matrix):
    """检查 Schedule Attainment 触发（连续低于95%）"""
    triggers = []
    if 'Schedule_Attainment' not in matrix.columns:
        return triggers
    
    consecutive = 0
    for idx, row in matrix.iterrows():
        if pd.notna(row['Schedule_Attainment']) and row['Schedule_Attainment'] < 95:
            consecutive += 1
            if consecutive >= 3:
                triggers.append({
                    'KPI_Type': 'Schedule_Attainment',
                    'Tag': 'CZ_Campus',
                    'Level': 'Critical',
                    'Description': '达成率连续3周低于95%',
                    'Details': f"当前值: {row['Schedule_Attainment']:.1f}%",
                    'Status': 'ACTIVE',
                    'TriggerWeek': row['FiscalWeek']
                })
                break
        else:
            consecutive = 0
    return triggers

def extract_week_number(fiscal_week):
    """从财周字符串提取可排序的数字"""
    try:
        parts = fiscal_week.split()
        year = int(parts[0][2:])
        week = int(parts[1][1:])
        return year * 100 + week
    except:
        return 0

def check_safety_triggers(matrix):
    """检查安全触发（连续3周前3）"""
    triggers = []
    
    # 转换为可排序的格式
    matrix['WeekNum'] = matrix['FiscalWeek'].apply(extract_week_number)
    matrix_sorted = matrix.sort_values('WeekNum')
    
    for label in matrix.columns:
        if label in ['FiscalWeek', 'WeekNum']:
            continue
        
        ranks = matrix_sorted[label]
        consecutive = 0
        
        for idx, rank in enumerate(ranks):
            if pd.isna(rank) or rank == '':
                consecutive = 0
                continue
            
            if rank <= 3:
                consecutive += 1
                if consecutive >= 3:
                    week = matrix_sorted.iloc[idx]['FiscalWeek']
                    triggers.append({
                        'KPI_Type': 'Safety_Issue_Rank',
                        'Tag': label,
                        'Level': 'Critical',
                        'Description': '安全隐患类型连续3周排名前3',
                        'Details': f"当前排名: {rank}",
                        'Status': 'ACTIVE',
                        'TriggerWeek': week
                    })
                    break
            else:
                consecutive = 0
    return triggers

def main():
    conn = get_db_connection()
    try:
        _ensure_monitoring_schema(conn)
        # 默认计算当天，如果需要回补历史，可以修改逻辑
        # 这里为了测试方便，先不删数据，直接追加（生产环境应先删除当天数据）
        # today = datetime.now().strftime('%Y-%m-%d')
        # conn.execute(f"DELETE FROM KPI_Data WHERE CreatedDate = '{today}'")
        
        # 仅清理本脚本会重算的KPI，避免误删其他模块写入的数据
        cursor = conn.cursor()
        cursor.execute("DELETE FROM dbo.KPI_Data WHERE KPI_Id IN (1, 2)")
        cursor.execute("DELETE FROM dbo.KPI_Data WHERE KPI_Id = (SELECT TOP 1 Id FROM dbo.KPI_Definition WHERE Name = 'Safety_Issue_Rank')")
        conn.commit()

        # 2. 执行聚合
        # 用户要求: 暂时不考虑产品和区域级别，只计算整体
        # aggregate_lead_time(conn)
        # aggregate_schedule_attainment(conn)
        
        # 3. 执行 Global Weekly 聚合
        aggregate_global_lead_time_weekly(conn)
        aggregate_global_sa_weekly(conn)

        # Commit LT/SA first so downstream optional aggregations won't rollback them
        conn.commit()
        
        # New Aggregation
        aggregate_safety_issue_rank_weekly(conn)
        
        conn.commit()
        logger.info("KPI 聚合完成。")
        
        # 4. 生成报表
        generate_reports(conn)
        
        # 5. 运行监控警报引擎
        try:
            from etl_alert_engine import run_alert_engine
            run_alert_engine(conn)
        except ImportError:
            logger.error("无法导入 etl_alert_engine 模块")
        except Exception as e:
            logger.error(f"警报引擎运行失败: {e}")
        
    except Exception as e:
        logger.error(f"KPI 聚合流程失败: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
