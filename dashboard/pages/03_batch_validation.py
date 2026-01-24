"""
Batch Metrics Validation Tool
验证单批次的 LT/PT/ST 计算过程
"""
import streamlit as st
import pyodbc
import pandas as pd
import os

st.set_page_config(page_title="批次计算验证", page_icon="🔍", layout="wide")

# --- DB Connection ---
SQL_SERVER = os.getenv('MDDAP_SQL_SERVER', r'localhost\SQLEXPRESS')
SQL_DATABASE = os.getenv('MDDAP_SQL_DATABASE', 'mddap_v2')
SQL_DRIVER = os.getenv('MDDAP_SQL_DRIVER', 'ODBC Driver 17 for SQL Server')

def get_connection():
    try:
        conn_str = (
            f"DRIVER={{{SQL_DRIVER}}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            "Trusted_Connection=yes;"
            "Encrypt=no;"
        )
        return pyodbc.connect(conn_str)
    except Exception as e:
        st.error(f"数据库连接失败: {e}")
        return None


def fetch_view_result(conn, batch: str, operation: str):
    """从 v_mes_metrics 获取已计算结果"""
    sql = """
    SELECT * FROM dbo.v_mes_metrics
    WHERE BatchNumber = ? AND LTRIM(RTRIM(Operation)) = LTRIM(RTRIM(?))
    """
    return pd.read_sql(sql, conn, params=(batch.strip(), operation.strip()))



def fetch_raw_mes(conn, batch: str, operation: str):
    """从 raw_mes 获取原始 MES 数据"""
    sql = """
    SELECT 
        BatchNumber, Operation, Machine, CFN, ProductNumber, [Group],
        EnterStepTime, TrackInTime, TrackOutTime,
        StepInQuantity, TrackOutQuantity, TrackOutOperator
    FROM dbo.raw_mes
    WHERE BatchNumber = ? AND LTRIM(RTRIM(Operation)) = LTRIM(RTRIM(?))
    """
    return pd.read_sql(sql, conn, params=(batch.strip(), operation.strip()))


def fetch_raw_sfc(conn, batch: str, operation: str):
    """从 raw_sfc 获取原始 SFC 数据"""
    sql = """
    SELECT 
        BatchNumber, Operation, TrackInTime, ScrapQty
    FROM dbo.raw_sfc
    WHERE BatchNumber = ? AND LTRIM(RTRIM(Operation)) = LTRIM(RTRIM(?))
    """
    return pd.read_sql(sql, conn, params=(batch.strip(), operation.strip()))


def fetch_routing(conn, cfn: str, operation: str, group: str):
    """从 raw_sap_routing 获取工艺标准"""
    sql = """
    SELECT TOP 1
        CFN, Operation, [Group], StandardTime, EH_machine, EH_labor, Quantity, SetupTime, OEE
    FROM dbo.raw_sap_routing
    WHERE CFN = ? AND LTRIM(RTRIM(Operation)) = LTRIM(RTRIM(?)) AND [Group] = ?
    ORDER BY COALESCE(updated_at, created_at) DESC
    """
    return pd.read_sql(sql, conn, params=(cfn, operation.strip(), group))


def manual_calc_st(qty, scrap, eh_machine, eh_labor, setup_time, oee, is_setup):
    """手动计算 ST(d)"""
    if eh_machine is None and eh_labor is None:
        return None
    
    eh = eh_machine if eh_machine and eh_machine > 0 else eh_labor
    if eh is None:
        return None
    
    setup = setup_time if is_setup == 'Yes' and setup_time else 0
    oee_val = oee if oee and oee > 0 else 0.77
    qty_val = (qty or 0) + (scrap or 0)
    
    # ST = (Setup + Qty * EH/3600/OEE + 0.5) / 24
    st_hours = setup + (qty_val * eh / 3600 / oee_val) + 0.5
    st_days = st_hours / 24
    return round(st_days, 4)


# --- Calculation Verification Helpers ---
def get_non_work_days_deduction(conn, start_time, end_time):
    """
    Calculate non-working days deduction between two timestamps using dim_calendar_cumulative.
    Logic mimics the SQL view: (End_CumNW - Start_CumNW) * 86400 seconds approx (simplified)
    Actually, to be precise, we fetch the specific days.
    """
    if not start_time or not end_time:
        return 0.0
    
    start_date = start_time.date()
    end_date = end_time.date()
    
    if start_date == end_date:
        # Check if today is non-work
        cursor = conn.cursor()
        cursor.execute("SELECT IsWorkday FROM dbo.dim_calendar_cumulative WHERE CalendarDate = ?", (start_date,))
        row = cursor.fetchone()
        is_work = row[0] if row else 1
        return 0.0 if(is_work == 1) else (end_time - start_time).total_seconds() / 86400.0

    # For different days
    cursor = conn.cursor()
    # Get range stats
    cursor.execute("""
        SELECT 
            MIN(CalendarDate) as StartDate,
            MAX(CalendarDate) as EndDate,
            SUM(CASE WHEN IsWorkday = 0 THEN 1 ELSE 0 END) as TotalNonWorkDays
        FROM dbo.dim_calendar_cumulative 
        WHERE CalendarDate BETWEEN ? AND ?
    """, (start_date, end_date))
    row = cursor.fetchone()
    
    if not row:
        return 0.0
        
    total_nw_days = row[2] or 0
    
    # Correction for start/end partial days is complex in SQL.
    # Here we simplify: Just count full non-working days in the range.
    # The SQL view logic is robust:
    # (End_CumNW - Start_CumNW) - (If Start is NW then partial) - (If End is NW then partial) ...
    # Let's rely on the cumulative difference for the "days" part.
    
    cursor.execute("""
        SELECT 
            (SELECT CumulativeNonWorkDays FROM dbo.dim_calendar_cumulative WHERE CalendarDate = ?) as StartCum,
            (SELECT CumulativeNonWorkDays FROM dbo.dim_calendar_cumulative WHERE CalendarDate = ?) as EndCum,
            (SELECT IsWorkday FROM dbo.dim_calendar_cumulative WHERE CalendarDate = ?) as StartIsWork,
            (SELECT IsWorkday FROM dbo.dim_calendar_cumulative WHERE CalendarDate = ?) as EndIsWork
    """, (start_date, end_date, start_date, end_date))
    metrics = cursor.fetchone()
    
    if not metrics:
        return 0.0
        
    start_cum, end_cum, start_is_work, end_is_work = metrics
    start_cum = start_cum or 0
    end_cum = end_cum or 0
    
    # Calculate deduction in seconds effectively
    # If a day is non-work, the entire duration on that day should be deducted?
    # SQL Logic:
    # If Start is Work: no deduction for start day part
    # If Start is Non-Work: deduct (86400 - seconds_in_day) => time from start to midnight
    # Intermediate days: (Diff in CumNW) * 1 day
    # If End is Work: no deduction for end day part
    # If End is Non-Work: deduct time from midnight to end
    
    deduction_seconds = 0.0
    
    # Middle days deduction (full days between)
    # cum diff includes the end day if it is non-work
    # base diff = end_cum - start_cum
    
    # Careful implementation of SQL logic:
    # (CASE WHEN PT_Start_IsWork = 0 THEN (86400 - DATEDIFF(SECOND, CAST(PT_StartTime AS DATE), PT_StartTime)) ELSE 0 END)
    # + (CASE WHEN End_IsWork = 0 THEN DATEDIFF(SECOND, CAST(TrackOutTime AS DATE), TrackOutTime) ELSE 0 END)
    # + ((COALESCE(End_CumNW, 0) - (CASE WHEN End_IsWork = 0 THEN 1 ELSE 0 END)) - COALESCE(PT_Start_CumNW, 0)) * 86400
    
    # Part 1: Start Day Deduction
    if start_is_work == 0:
        midnight_next = datetime.combine(start_date + timedelta(days=1), datetime.min.time())
        deduction_seconds += (midnight_next - start_time).total_seconds()
        
    # Part 2: End Day Deduction
    if end_is_work == 0:
        midnight_end = datetime.combine(end_date, datetime.min.time())
        deduction_seconds += (end_time - midnight_end).total_seconds()
        
    # Part 3: Middle Days Deduction
    # (End_CumNW - (1 if End is NW) - Start_CumNW)
    correction = 1 if end_is_work == 0 else 0
    middle_days = (end_cum - correction - start_cum)
    if middle_days > 0:
        deduction_seconds += middle_days * 86400.0
        
    return deduction_seconds / 86400.0

def manual_calc_lt(track_out, enter_step, lnw_days):
    """手动计算 LT(d)"""
    if track_out is None or enter_step is None:
        return None
    
    gross_days = (track_out - enter_step).total_seconds() / 86400.0
    lt = gross_days - (lnw_days or 0)
    return round(max(lt, 0), 4)


# --- Helper Functions for Dropdowns ---
@st.cache_data(ttl=600)
def get_batch_list():
    """Fetch distinct batch numbers for dropdown (optimized)"""
    conn = get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        # Query raw table instead of complex view for performance
        cursor.execute("SELECT DISTINCT BatchNumber FROM dbo.raw_mes ORDER BY BatchNumber DESC")
        return [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()

@st.cache_data(ttl=600)
def get_operation_list(batch_number):
    """Fetch operations for a specific batch (optimized)"""
    if not batch_number:
        return []
    conn = get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT LTRIM(RTRIM(Operation)) FROM dbo.raw_mes WHERE BatchNumber = ?", (batch_number,))
        ops = [row[0] for row in cursor.fetchall() if row[0]]
        
        # Sort numerically in Python (handle potential non-numeric values gracefully)
        def sort_key(op):
            try:
                return float(op)
            except ValueError:
                return float('inf') # Put non-numeric at the end
                
        ops.sort(key=sort_key)
        return ops
    finally:
        conn.close()


# --- UI ---
st.title("🔍 批次 LT/PT/ST 计算验证工具")
st.markdown('选择批次号和工序号，点击 **计算** 查看原始数据和计算过程。')

col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    # Use selectbox with search capability (in Streamlit, selectbox has built-in search)
    batch_list = get_batch_list()
    batch_input = st.selectbox("批次号 (BatchNumber)", options=[""] + batch_list, index=0, placeholder="输入或选择批次号")

with col2:
    # Dynamic operation list based on selected batch
    if batch_input:
        op_list = get_operation_list(batch_input)
        op_input = st.selectbox("工序号 (Operation)", options=[""] + op_list, index=0, placeholder="输入或选择工序")
    else:
        op_input = st.selectbox("工序号 (Operation)", options=[], disabled=True)

with col3:
    st.write("")
    st.write("")
    calc_btn = st.button("🚀 计算", type="primary", use_container_width=True)

if calc_btn:
    if not batch_input or not op_input:
        st.warning("请输入批次号和工序号")
    else:
        conn = get_connection()
        if conn:
            with st.spinner("正在查询数据库..."):
                df_view = fetch_view_result(conn, batch_input, op_input)
                df_mes = fetch_raw_mes(conn, batch_input, op_input)
                df_sfc = fetch_raw_sfc(conn, batch_input, op_input)
            
            if df_view.empty and df_mes.empty:
                st.error(f"未找到批次 {batch_input} 工序 {op_input} 的数据")
            else:
                st.success(f"找到 {len(df_view)} 条视图记录, {len(df_mes)} 条 MES 原始记录, {len(df_sfc)} 条 SFC 原始记录")
                
                # ========== Section 1: Raw Data ==========
                st.markdown("---")
                st.subheader("📋 原始数据")
                
                tab1, tab2, tab3 = st.tabs(["MES 原始", "SFC 原始", "SAP Routing"])
                with tab1:
                    st.dataframe(df_mes, use_container_width=True)
                with tab2:
                    st.dataframe(df_sfc, use_container_width=True)
                with tab3:
                    if not df_mes.empty:
                        cfn = df_mes.iloc[0].get('CFN')
                        grp = df_mes.iloc[0].get('Group')
                        if cfn and grp:
                            df_routing = fetch_routing(conn, cfn, op_input, grp)
                            st.dataframe(df_routing, use_container_width=True)
                        else:
                            st.info("MES 记录中缺少 CFN 或 Group 信息")
                    else:
                        st.info("无 MES 数据")
                
                # ========== Section 2: Calculated Results ==========
                st.markdown("---")
                st.subheader("📊 计算结果 (来自 v_mes_metrics)")
                if not df_view.empty:
                    display_cols = ['BatchNumber', 'Operation', 'Machine', 'CFN', 
                                    'TrackInTime', 'TrackOutTime', 'EnterStepTime', 'TrackIn_SFC',
                                    'TrackOutQuantity', 'ScrapQty', 'IsSetup',
                                    'EH_machine', 'EH_labor', 'SetupTime', 'OEE',
                                    'LT(d)', 'PT(d)', 'ST(d)', 'LNW(d)', 'PNW(d)', 'CompletionStatus']
                    st.dataframe(df_view[[c for c in display_cols if c in df_view.columns]], use_container_width=True)
                    
                    # ========== Section 3: Step-by-Step Calculation ==========
                    st.markdown("---")
                    st.subheader("🧮 计算过程分解")
                    
                    row = df_view.iloc[0]
                    
                    # ST Calculation
                    st.markdown("#### ST(d) - 标准时间")
                    st.markdown(f"""
**公式**: `ST(d) = (调试时间 + (合格数+报废数) × 单件工时 / OEE + 0.5h) / 24`

| 参数 | 值 | 说明 |
|:---- |:---|:-----|
| 合格数 (Qty) | `{row.get('TrackOutQuantity')}` | 来自 MES |
| 报废数 (Scrap) | `{row.get('ScrapQty')}` | 来自 SFC |
| 单件工时 (EH) | `{row.get('EH_machine')}` 秒 | 机器工时优先 |
| OEE | `{row.get('OEE')}` | 来自 SAP Routing |
| 是否换型 | `{row.get('IsSetup')}` | 决定是否加调试时间 |
| 调试时间 | `{row.get('SetupTime')}` 小时 | 来自 SAP Routing |

**SQL 计算结果**: `{row.get('ST(d)')}` 天
""")
                    # Manual ST
                    manual_st = manual_calc_st(
                        row.get('TrackOutQuantity'), row.get('ScrapQty'),
                        row.get('EH_machine'), row.get('EH_labor'),
                        row.get('SetupTime'), row.get('OEE'), row.get('IsSetup')
                    )
                    st.markdown(f"**手动验算结果**: `{manual_st}` 天")
                    if manual_st and row.get('ST(d)'):
                        diff = abs(manual_st - float(row.get('ST(d)')))
                        if diff < 0.01:
                            st.success("✅ 手动计算与 SQL 结果一致")
                        else:
                            st.warning(f"⚠️ 存在差异: {diff:.4f} 天")
                    
                    # LT Calculation
                    st.markdown("#### LT(d) - 实际周期")
                    
                    # Get time sources
                    sfc_trackin = None
                    if not df_sfc.empty:
                        sfc_times = df_sfc['TrackInTime'].dropna()
                        if not sfc_times.empty:
                            sfc_trackin = sfc_times.min()
                    
                    mes_enterstep = row.get('EnterStepTime')
                    mes_trackin = row.get('TrackInTime')
                    trackout = row.get('TrackOutTime')
                    operation_str = str(row.get('Operation', '')).strip()
                    
                    # Determine which time is used based on SQL logic
                    is_first_op = operation_str in ['10', '0010']
                    if is_first_op:
                        actual_start = sfc_trackin or mes_enterstep or mes_trackin
                        selected_source = "SFC TrackIn" if sfc_trackin else ("MES EnterStep" if mes_enterstep else "MES TrackIn")
                        reason = "工序10为首道工序，优先使用 SFC 精确打卡时间"
                    else:
                        actual_start = mes_enterstep
                        selected_source = "MES EnterStep"
                        reason = "非首道工序，使用上道工序结束时间 (EnterStep)"
                    
                    st.markdown(f"""
**公式**: `LT(d) = (TrackOutTime - LT_StartTime) - 非工作日时间`

**时间源选择 (COALESCE 优先级)**:
| 优先级 | 时间源 | 值 | 是否被选中 |
|:-------|:-------|:---|:-----------|
| 1 | SFC TrackIn (精确打卡) | `{sfc_trackin}` | {'✅ **已选中**' if selected_source == 'SFC TrackIn' else '❌'} |
| 2 | MES EnterStep (进入工位) | `{mes_enterstep}` | {'✅ **已选中**' if selected_source == 'MES EnterStep' else '❌'} |
| 3 | MES TrackIn (MES开始) | `{mes_trackin}` | {'✅ **已选中**' if selected_source == 'MES TrackIn' else '❌'} |

**选择逻辑**: {reason}

| 参数 | 值 |
|:---- |:---|
| 结束时间 (TrackOut) | `{trackout}` |
| **实际使用的开始时间** | `{actual_start}` ({selected_source}) |
| 非工作日扣除 LNW(d) | `{row.get('LNW(d)')}` 天 |

**SQL 计算结果**: `{row.get('LT(d)')}` 天
""")
                    
                    # PT Calculation
                    st.markdown("#### PT(d) - 实际加工时间")
                    
                    # Get PT-related data from view
                    prev_batch_end = row.get('PreviousBatchEndTime')
                    machine = row.get('Machine')
                    
                    # PT start time logic is complex:
                    # If there's a gap between EnterStepTime and PreviousBatchEndTime -> use TrackInTime
                    # If continuous -> use PreviousBatchEndTime
                    has_gap = False
                    if mes_enterstep and prev_batch_end:
                        try:
                            gap = (mes_enterstep - prev_batch_end).total_seconds()
                            has_gap = gap > 0
                        except:
                            pass
                    
                    if has_gap:
                        pt_start = mes_trackin or prev_batch_end
                        pt_source = "MES TrackIn" if mes_trackin else "PreviousBatchEndTime"
                        pt_reason = "存在间隙 (EnterStep > PreviousBatchEnd)，使用 TrackInTime"
                    else:
                        pt_start = prev_batch_end or mes_trackin
                        pt_source = "PreviousBatchEndTime" if prev_batch_end else "MES TrackIn"
                        pt_reason = "连续生产，使用上一批次结束时间作为本批次开始时间"
                    
                    st.markdown(f"""
**公式**: `PT(d) = (TrackOutTime - PT_StartTime) - 非工作日时间`

**机台信息**: `{machine}`

**PT 开始时间逻辑** (基于同一机台连续性判断):
| 参数 | 值 | 说明 |
|:-----|:---|:-----|
| 上一批结束时间 | `{prev_batch_end}` | 同机台上一批次的 TrackOutTime |
| 本批进入时间 | `{mes_enterstep}` | EnterStepTime |
| 是否有间隙 | `{'是 (有等待)' if has_gap else '否 (连续生产)'}` | EnterStep > PrevBatchEnd? |

**时间源选择**:
| 优先级 | 时间源 | 值 | 是否被选中 |
|:-------|:-------|:---|:-----------|
| 1 | PreviousBatchEndTime (上批结束) | `{prev_batch_end}` | {'✅ **已选中**' if pt_source == 'PreviousBatchEndTime' else '❌'} |
| 2 | MES TrackIn (进入加工) | `{mes_trackin}` | {'✅ **已选中**' if pt_source == 'MES TrackIn' else '❌'} |

**选择逻辑**: {pt_reason}

| 计算参数 | 值 |
|:---------|:---|
| 结束时间 (TrackOut) | `{trackout}` |
| **实际使用的开始时间** | `{pt_start}` ({pt_source}) |
| 非工作日扣除 PNW(d) | `{row.get('PNW(d)')}` 天 |

**SQL 计算结果**: `{row.get('PT(d)')}` 天
""")
                    # --- Section 3: Python Real-time Verification ---
                    st.markdown("---")
                    st.subheader("🐍 Python 实时复算验证")
                    st.info("基于原始数据和 Python 逻辑实时重新计算，用于验证 SQL 结果的正确性。")
                    
                    # LT Recalculation
                    # Using the exact same logic as SQL but in Python
                    # 1. Calc non-work days between actual_start and trackout for LT
                    lt_nw_days = get_non_work_days_deduction(conn, pd.to_datetime(actual_start), pd.to_datetime(trackout))
                    lt_python_val = manual_calc_lt(pd.to_datetime(trackout), pd.to_datetime(actual_start), lt_nw_days)
                    
                    # PT Recalculation
                    pt_nw_days = get_non_work_days_deduction(conn, pd.to_datetime(pt_start), pd.to_datetime(trackout))
                    # Reuse manual_calc_lt as it's just (end-start) - deduction
                    pt_python_val = manual_calc_lt(pd.to_datetime(trackout), pd.to_datetime(pt_start), pt_nw_days)
                    
                    # ST Recalculation
                    st_python_val = manual_calc_st(
                        row.get('TrackOutQuantity'), row.get('ScrapQty'),
                        row.get('EH_machine'), row.get('EH_labor'),
                        row.get('SetupTime'), row.get('OEE'), row.get('IsSetup')
                    )

                    # Comparison Table
                    st.markdown("#### ✅ 结果对比")
                    
                    # Formatting helper
                    def fmt_val(v): return f"{v:.4f}" if v is not None else "N/A"
                    def diff_color(dev): return "background-color: #ffcccc" if abs(dev) > 0.0001 else ""

                    lt_sql = row.get('LT(d)')
                    pt_sql = row.get('PT(d)')
                    st_sql = row.get('ST(d)')
                    
                    lt_diff = (lt_sql or 0) - (lt_python_val or 0)
                    pt_diff = (pt_sql or 0) - (pt_python_val or 0)
                    st_diff = (st_sql or 0) - (st_python_val or 0)
                    
                    st.markdown(f"""
                    | 指标 | SQL 视图结果 | Python 实时计算 | 差异 (SQL-Py) | 状态 |
                    |:---|:---|:---|:---|:---|
                    | **LT(d)** | `{fmt_val(lt_sql)}` | `{fmt_val(lt_python_val)}` | `{fmt_val(lt_diff)}` | {'✅ 一致' if abs(lt_diff) < 0.001 else '❌ 差异'} |
                    | **PT(d)** | `{fmt_val(pt_sql)}` | `{fmt_val(pt_python_val)}` | `{fmt_val(pt_diff)}` | {'✅ 一致' if abs(pt_diff) < 0.001 else '❌ 差异'} |
                    | **ST(d)** | `{fmt_val(st_sql)}` | `{fmt_val(st_python_val)}` | `{fmt_val(st_diff)}` | {'✅ 一致' if abs(st_diff) < 0.001 else '❌ 差异'} |
                    
                    *注: 非工作日扣除计算 Python 侧为: LT={lt_nw_days:.4f}天, PT={pt_nw_days:.4f}天*
                    """)

                else:
                    st.warning("视图中无此批次数据，可能尚未同步或 JOIN 条件不满足")
            
            conn.close()
