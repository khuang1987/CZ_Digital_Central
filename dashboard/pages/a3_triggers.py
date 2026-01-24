import os
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

import pandas as pd
import plotly.graph_objects as go
import pyodbc
import streamlit as st


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
KPI_RULES_PATH = os.path.join(PROJECT_ROOT, 'data_pipelines', 'monitoring', 'config', 'kpi_rules.csv')


# SQL Server 连接配置（可用环境变量覆盖）
SQL_SERVER = os.getenv('MDDAP_SQL_SERVER', r'localhost\SQLEXPRESS')
SQL_DATABASE = os.getenv('MDDAP_SQL_DATABASE', 'mddap_v2')
SQL_DRIVER = os.getenv('MDDAP_SQL_DRIVER', 'ODBC Driver 17 for SQL Server')


def get_db_connection():
    try:
        conn_str = (
            f"DRIVER={{{SQL_DRIVER}}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            "Trusted_Connection=yes;"
            "Encrypt=no;"
        )
        return pyodbc.connect(conn_str, autocommit=False)
    except Exception as e:
        st.error(f"连接数据库失败: {str(e)}")
        return None


def _table_exists(conn: pyodbc.Connection, name: str) -> bool:
    cur = conn.cursor()
    row = cur.execute(
        "SELECT 1 WHERE OBJECT_ID(?, 'U') IS NOT NULL",
        (f"dbo.{name}",),
    ).fetchone()
    return row is not None


def _ensure_kpi_schema(conn: pyodbc.Connection) -> bool:
    try:
        if not _table_exists(conn, 'TriggerCaseRegistry'):
            return False
        cur = conn.cursor()
        row = cur.execute(
            "SELECT 1 "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='TriggerCaseRegistry' AND COLUMN_NAME='Source'"
        ).fetchone()
        if row is None:
            cur.execute("ALTER TABLE dbo.TriggerCaseRegistry ADD Source nvarchar(50) NULL")
            cur.execute("UPDATE dbo.TriggerCaseRegistry SET Source='AUTO' WHERE Source IS NULL OR LTRIM(RTRIM(Source))='' ")
            conn.commit()
        return True
    except Exception as e:
        st.error(f"执行 schema 检查失败: {e}")
        return False


def _alloc_a3_id(conn: pyodbc.Connection, opened_at: date) -> str:
    date_str = opened_at.strftime('%Y%m%d')
    prefix = f"A3-{date_str}-"
    cur = conn.cursor()
    rows = cur.execute("SELECT A3Id FROM dbo.TriggerCaseRegistry WHERE A3Id LIKE ?", (f"{prefix}%",)).fetchall()
    max_seq = 0
    for (aid,) in rows:
        try:
            suffix = int(str(aid).split('-')[-1])
            if suffix > max_seq:
                max_seq = suffix
        except Exception:
            continue
    return f"{prefix}{(max_seq + 1):04d}"


def _upsert_case(conn: pyodbc.Connection, row: dict) -> None:
    cur = conn.cursor()
    has_source = False
    try:
        c = cur.execute(
            "SELECT 1 "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='TriggerCaseRegistry' AND COLUMN_NAME='Source'"
        ).fetchone()
        has_source = c is not None
    except Exception:
        has_source = False

    if has_source:
        cur.execute(
            "UPDATE dbo.TriggerCaseRegistry SET "
            "Category=?, TriggerType=?, Source=?, Status=?, OpenedAt=?, ClosedAt=?, PlannerTaskId=?, Notes=?, "
            "OriginalLevel=COALESCE(NULLIF(OriginalLevel,''), ?), "
            "OriginalDesc=COALESCE(NULLIF(OriginalDesc,''), ?), "
            "OriginalDetails=COALESCE(NULLIF(OriginalDetails,''), ?), "
            "OriginalValue=COALESCE(NULLIF(OriginalValue,''), ?), "
            "UpdatedAt=SYSUTCDATETIME() "
            "WHERE A3Id=?",
            (
                row.get('Category'), row.get('TriggerType'), row.get('Source'), row.get('Status'),
                row.get('OpenedAt'), row.get('ClosedAt'), row.get('PlannerTaskId'), row.get('Notes'),
                row.get('OriginalLevel'), row.get('OriginalDesc'), row.get('OriginalDetails'), row.get('OriginalValue'),
                row.get('A3Id'),
            ),
        )
        if cur.rowcount == 0:
            cur.execute(
                "INSERT INTO dbo.TriggerCaseRegistry "
                "(A3Id, Category, TriggerType, Source, Status, OpenedAt, ClosedAt, PlannerTaskId, Notes, "
                " OriginalLevel, OriginalDesc, OriginalDetails, OriginalValue, UpdatedAt) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME())",
                (
                    row.get('A3Id'), row.get('Category'), row.get('TriggerType'), row.get('Source'), row.get('Status'),
                    row.get('OpenedAt'), row.get('ClosedAt'), row.get('PlannerTaskId'), row.get('Notes'),
                    row.get('OriginalLevel'), row.get('OriginalDesc'), row.get('OriginalDetails'), row.get('OriginalValue'),
                ),
            )
    else:
        cur.execute(
            "UPDATE dbo.TriggerCaseRegistry SET "
            "Category=?, TriggerType=?, Status=?, OpenedAt=?, ClosedAt=?, PlannerTaskId=?, Notes=?, "
            "UpdatedAt=SYSUTCDATETIME() "
            "WHERE A3Id=?",
            (
                row.get('Category'), row.get('TriggerType'), row.get('Status'),
                row.get('OpenedAt'), row.get('ClosedAt'), row.get('PlannerTaskId'), row.get('Notes'),
                row.get('A3Id'),
            ),
        )
        if cur.rowcount == 0:
            cur.execute(
                "INSERT INTO dbo.TriggerCaseRegistry "
                "(A3Id, Category, TriggerType, Status, OpenedAt, ClosedAt, PlannerTaskId, Notes, UpdatedAt) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME())",
                (
                    row.get('A3Id'), row.get('Category'), row.get('TriggerType'), row.get('Status'),
                    row.get('OpenedAt'), row.get('ClosedAt'), row.get('PlannerTaskId'), row.get('Notes'),
                ),
            )

    conn.commit()


@st.cache_data(ttl=60)
def load_kpi_rules() -> pd.DataFrame:
    if not os.path.exists(KPI_RULES_PATH):
        return pd.DataFrame()
    df = pd.read_csv(KPI_RULES_PATH)
    df.columns = [c.strip() for c in df.columns]
    for c in ['RuleCode', 'KPI_Name', 'Owner', 'DataSource', 'Description', 'TagFilter', 'ActionType', 'TriggerLevel', 'ComparisonOperator', 'Active']:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    
    # Filter by Active column
    if 'Active' in df.columns:
        df = df[df['Active'].str.lower() == 'yes'].copy()
        
    for c in ['KPI_Id', 'ThresholdValue', 'ConsecutiveOccurrences', 'LookbackDays']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


@st.cache_data(ttl=60)
def load_kpi_definition() -> pd.DataFrame:
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query('SELECT * FROM dbo.KPI_Definition', conn)
    finally:
        conn.close()


@st.cache_data(ttl=60)
def load_trigger_cases() -> pd.DataFrame:
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        df = pd.read_sql_query('SELECT * FROM dbo.TriggerCaseRegistry', conn)
        for col in ['OpenedAt', 'ClosedAt', 'UpdatedAt']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        return df
    finally:
        conn.close()


@st.cache_data(ttl=60)
def load_kpi_series(kpi_id: int, tag: str, start_date: str, end_date: str) -> pd.DataFrame:
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    q = """
    SELECT
      d.CreatedDate AS date,
      d.Progress AS value,
      cal.fiscal_year,
      cal.fiscal_week,
      cal.fiscal_week_label
    FROM dbo.KPI_Data d
    LEFT JOIN dbo.dim_calendar cal ON d.CreatedDate = cal.date
    WHERE d.KPI_Id = ?
      AND d.Tag = ?
      AND d.CreatedDate BETWEEN ? AND ?
    ORDER BY d.CreatedDate
    """
    try:
        df = pd.read_sql_query(q, conn, params=(kpi_id, tag, start_date, end_date))
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['fiscal_week_label'] = df['fiscal_week_label'].fillna('')
        return df
    finally:
        conn.close()


def _sa_target_value() -> float:
    return 90.0


def _default_date_range(days: int = 120) -> Tuple[date, date]:
    end = datetime.now().date()
    start = end - timedelta(days=days)
    return start, end


def _build_trend_chart(df: pd.DataFrame, title: str, target: Optional[float], thresholds: List[Tuple[str, float]]):
    fig = go.Figure()
    if not df.empty:
        x = df['date']
        fig.add_trace(go.Scatter(x=x, y=df['value'], mode='lines+markers', name='Actual'))

        if target is not None:
            fig.add_hline(y=target, line_width=2, line_dash='dash', line_color='green', annotation_text=f"Target {target}")

        for name, val in thresholds:
            fig.add_hline(y=val, line_width=2, line_dash='dot', line_color='red', annotation_text=f"{name} {val}")

    fig.update_layout(
        title=title,
        xaxis_title='Date',
        yaxis_title='Value',
        height=360,
        margin=dict(l=20, r=20, t=50, b=20),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
    )
    return fig


def main():
    st.title('A3 Trigger Dashboard')

    conn = get_db_connection()
    if conn is None:
        return

    required_tables = ['KPI_Definition', 'KPI_Data', 'TriggerCaseRegistry']
    missing = [t for t in required_tables if not _table_exists(conn, t)]

    if missing:
        st.warning(f"数据库缺少表: {', '.join(missing)}")
        st.info("可通过初始化脚本创建 KPI/Trigger 相关表。")
        if st.button('初始化 KPI/Trigger 表 (执行 kpi_schema.sql)'):
            ok = _ensure_kpi_schema(conn)
            if ok:
                st.success('初始化完成，请刷新页面。')
        conn.close()
        return

    rules_df = load_kpi_rules()
    kpi_def_df = load_kpi_definition()
    cases_df = load_trigger_cases()

    st.sidebar.header('Filters')

    source_options = []
    if not cases_df.empty and 'Source' in cases_df.columns:
        source_options = sorted([x for x in cases_df['Source'].dropna().astype(str).unique().tolist() if str(x).strip()])
    if source_options:
        selected_sources = st.sidebar.multiselect('Source', options=source_options, default=source_options)
    else:
        selected_sources = []

    status_options = ['OPEN', 'CLOSED']
    status_default = ['OPEN']
    selected_status = st.sidebar.multiselect('Status', options=status_options, default=status_default)

    level_options = []
    if not rules_df.empty and 'TriggerLevel' in rules_df.columns:
        level_options = sorted([x for x in rules_df['TriggerLevel'].dropna().unique().tolist() if str(x).strip()])
    if not level_options:
        level_options = ['Critical', 'Warning']
    selected_levels = st.sidebar.multiselect('Trigger Level', options=level_options, default=level_options)

    trigger_type_options = []
    if not cases_df.empty and 'TriggerType' in cases_df.columns:
        trigger_type_options = sorted([x for x in cases_df['TriggerType'].dropna().astype(str).unique().tolist() if str(x).strip()])
    selected_trigger_types = st.sidebar.multiselect('Trigger Type', options=trigger_type_options, default=trigger_type_options)

    start_d, end_d = _default_date_range(180)
    date_range = st.sidebar.date_input('OpenedAt Range', value=(start_d, end_d))
    if isinstance(date_range, tuple) and len(date_range) == 2:
        opened_start, opened_end = date_range
    else:
        opened_start, opened_end = start_d, end_d

    filtered = cases_df.copy()
    if selected_sources and 'Source' in filtered.columns:
        filtered = filtered[filtered['Source'].astype(str).str.upper().isin([s.upper() for s in selected_sources])]
    if 'Status' in filtered.columns and selected_status:
        filtered = filtered[filtered['Status'].astype(str).str.upper().isin([s.upper() for s in selected_status])]

    if 'TriggerType' in filtered.columns and selected_trigger_types:
        filtered = filtered[filtered['TriggerType'].astype(str).isin(selected_trigger_types)]

    if 'OpenedAt' in filtered.columns:
        filtered = filtered[(filtered['OpenedAt'] >= pd.to_datetime(opened_start)) & (filtered['OpenedAt'] <= pd.to_datetime(opened_end) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1))]

    if not rules_df.empty and 'RuleCode' in rules_df.columns:
        filtered = filtered.merge(
            rules_df,
            how='left',
            left_on='TriggerType',
            right_on='RuleCode',
            suffixes=('', '_rule'),
        )

    if 'TriggerLevel' in filtered.columns and selected_levels:
        filtered = filtered[filtered['TriggerLevel'].astype(str).isin(selected_levels)]

    today = datetime.now().date()
    if 'OpenedAt' in filtered.columns:
        filtered['AgeDays'] = (pd.Timestamp(today) - filtered['OpenedAt']).dt.days
    else:
        filtered['AgeDays'] = None

    open_df = filtered
    if 'Status' in open_df.columns:
        open_df = open_df[open_df['Status'].astype(str).str.upper() == 'OPEN']

    critical_open = open_df
    if 'TriggerLevel' in critical_open.columns:
        critical_open = critical_open[critical_open['TriggerLevel'].astype(str).str.lower() == 'critical']

    aging_open = open_df
    if 'AgeDays' in aging_open.columns:
        aging_open = aging_open[aging_open['AgeDays'].fillna(-1) > 14]

    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric('Open A3/Triggers', int(len(open_df)))
    kpi2.metric('Critical Open', int(len(critical_open)))
    kpi3.metric('Aging > 14 days', int(len(aging_open)))

    st.subheader('Trigger Cases')

    show_cols = []
    preferred = [
        'A3Id', 'Category', 'TriggerType', 'Source', 'TriggerLevel', 'Status', 'OpenedAt', 'ClosedAt', 'AgeDays',
        'Owner', 'ActionType', 'ThresholdValue', 'ComparisonOperator', 'ConsecutiveOccurrences', 'Description',
        'OriginalDesc', 'OriginalDetails', 'OriginalValue', 'UpdatedAt',
    ]
    for c in preferred:
        if c in filtered.columns:
            show_cols.append(c)

    if filtered.empty:
        st.info('当前筛选条件下没有触发案例。')
    else:
        st.dataframe(
            filtered[show_cols].sort_values(['Status', 'TriggerLevel', 'OpenedAt'], ascending=[True, True, False]) if show_cols else filtered,
            use_container_width=True,
            height=360,
        )

    rule_code_options: List[str] = []
    if not rules_df.empty and 'RuleCode' in rules_df.columns:
        rule_code_options = sorted([x for x in rules_df['RuleCode'].dropna().astype(str).unique().tolist()])

    st.subheader('Manual Maintenance')
    with st.expander('Create / Update A3 Case (MANUAL)', expanded=False):
        tab_create, tab_update = st.tabs(['Create MANUAL Case', 'Update / Close Case'])

        with tab_create:
            with st.form('manual_create'):
                opened_at = st.date_input('OpenedAt', value=datetime.now().date())
                category = st.text_input('Category (e.g., GLOBAL)', value='GLOBAL')
                trigger_type = st.selectbox('TriggerType', options=(rule_code_options if rule_code_options else ['SA_GLOBAL_CRITICAL']))
                status = st.selectbox('Status', options=['OPEN', 'CLOSED'], index=0)
                closed_at = st.date_input('ClosedAt (if CLOSED)', value=datetime.now().date())
                planner_task_id = st.text_input('PlannerTaskId', value='')
                notes = st.text_area('Notes', value='')
                original_level = st.text_input('OriginalLevel', value='MANUAL')
                original_desc = st.text_input('OriginalDesc', value='Manual created case')
                original_details = st.text_area('OriginalDetails', value='')
                original_value = st.text_input('OriginalValue', value='')
                submit = st.form_submit_button('Create')

            if submit:
                c = get_db_connection()
                if c is not None:
                    try:
                        _ensure_kpi_schema(c)
                        a3_id = _alloc_a3_id(c, opened_at)
                        row = {
                            'A3Id': a3_id,
                            'Category': category.strip(),
                            'TriggerType': str(trigger_type).strip(),
                            'Source': 'MANUAL',
                            'Status': str(status).strip(),
                            'OpenedAt': opened_at.strftime('%Y-%m-%d'),
                            'ClosedAt': (closed_at.strftime('%Y-%m-%d') if status == 'CLOSED' else None),
                            'PlannerTaskId': planner_task_id.strip(),
                            'Notes': notes,
                            'OriginalLevel': original_level,
                            'OriginalDesc': original_desc,
                            'OriginalDetails': original_details,
                            'OriginalValue': original_value,
                        }
                        _upsert_case(c, row)
                        st.success(f'Created MANUAL case: {a3_id}')
                        st.cache_data.clear()
                    finally:
                        c.close()

        with tab_update:
            cids = []
            if not cases_df.empty and 'A3Id' in cases_df.columns:
                cids = cases_df['A3Id'].dropna().astype(str).tolist()
            selected_a3 = st.selectbox('Select A3Id', options=cids) if cids else None
            if selected_a3:
                current = cases_df[cases_df['A3Id'].astype(str) == str(selected_a3)].head(1)
                cur_status = current['Status'].iloc[0] if (not current.empty and 'Status' in current.columns) else 'OPEN'
                cur_notes = current['Notes'].iloc[0] if (not current.empty and 'Notes' in current.columns) else ''
                cur_planner = current['PlannerTaskId'].iloc[0] if (not current.empty and 'PlannerTaskId' in current.columns) else ''
                with st.form('manual_update'):
                    new_status = st.selectbox('New Status', options=['OPEN', 'CLOSED'], index=(1 if str(cur_status).upper() == 'CLOSED' else 0))
                    new_closed_at = st.date_input('ClosedAt (if CLOSED)', value=datetime.now().date())
                    new_planner_task_id = st.text_input('PlannerTaskId', value=str(cur_planner) if cur_planner is not None else '')
                    new_notes = st.text_area('Notes', value=str(cur_notes) if cur_notes is not None else '')
                    submit2 = st.form_submit_button('Update')
                if submit2:
                    c = get_db_connection()
                    if c is not None:
                        try:
                            _ensure_kpi_schema(c)
                            row = {
                                'A3Id': str(selected_a3),
                                'Category': (current['Category'].iloc[0] if (not current.empty and 'Category' in current.columns) else 'GLOBAL'),
                                'TriggerType': (current['TriggerType'].iloc[0] if (not current.empty and 'TriggerType' in current.columns) else ''),
                                'Source': (current['Source'].iloc[0] if (not current.empty and 'Source' in current.columns) else 'MANUAL'),
                                'Status': str(new_status).strip(),
                                'OpenedAt': (pd.to_datetime(current['OpenedAt'].iloc[0]).strftime('%Y-%m-%d') if (not current.empty and 'OpenedAt' in current.columns and pd.notna(current['OpenedAt'].iloc[0])) else datetime.now().strftime('%Y-%m-%d')),
                                'ClosedAt': (new_closed_at.strftime('%Y-%m-%d') if new_status == 'CLOSED' else None),
                                'PlannerTaskId': new_planner_task_id.strip(),
                                'Notes': new_notes,
                                'OriginalLevel': (current['OriginalLevel'].iloc[0] if (not current.empty and 'OriginalLevel' in current.columns) else ''),
                                'OriginalDesc': (current['OriginalDesc'].iloc[0] if (not current.empty and 'OriginalDesc' in current.columns) else ''),
                                'OriginalDetails': (current['OriginalDetails'].iloc[0] if (not current.empty and 'OriginalDetails' in current.columns) else ''),
                                'OriginalValue': (current['OriginalValue'].iloc[0] if (not current.empty and 'OriginalValue' in current.columns) else ''),
                            }
                            _upsert_case(c, row)
                            st.success('Updated case')
                            st.cache_data.clear()
                        finally:
                            c.close()

    st.subheader('KPI Trend')

    if rule_code_options:
        default_rule = 'SA_GLOBAL_CRITICAL' if 'SA_GLOBAL_CRITICAL' in rule_code_options else rule_code_options[0]
        selected_rule = st.selectbox('Select Rule (for trend)', options=rule_code_options, index=rule_code_options.index(default_rule))
        rule_row = rules_df[rules_df['RuleCode'] == selected_rule].head(1)
    else:
        selected_rule = None
        rule_row = pd.DataFrame()

    trend_days = st.slider('Trend window (days)', min_value=30, max_value=365, value=180, step=30)
    trend_start, trend_end = _default_date_range(trend_days)

    if rule_row.empty:
        st.info('无法从规则表加载 KPI 配置，无法绘制趋势。')
        conn.close()
        return

    kpi_id = int(rule_row['KPI_Id'].iloc[0]) if pd.notna(rule_row['KPI_Id'].iloc[0]) else None
    tag = rule_row['TagFilter'].iloc[0] if 'TagFilter' in rule_row.columns else 'CZ_Campus'

    if kpi_id is None or not tag:
        st.info('规则缺少 KPI_Id 或 TagFilter，无法绘制趋势。')
        conn.close()
        return

    series = load_kpi_series(kpi_id, tag, trend_start.strftime('%Y-%m-%d'), trend_end.strftime('%Y-%m-%d'))

    kpi_name = rule_row['KPI_Name'].iloc[0] if 'KPI_Name' in rule_row.columns else str(kpi_id)
    threshold_val = float(rule_row['ThresholdValue'].iloc[0]) if pd.notna(rule_row['ThresholdValue'].iloc[0]) else None

    target_val = None
    if str(kpi_name).strip().lower() in ['schedule attainment', 'schedule_attainment', 'sa']:
        target_val = _sa_target_value()

    thresholds = []
    if threshold_val is not None:
        thresholds.append(('Trigger Threshold', threshold_val))

    if not kpi_def_df.empty and 'Id' in kpi_def_df.columns:
        def_row = kpi_def_df[kpi_def_df['Id'] == kpi_id].head(1)
        if not def_row.empty:
            if target_val is None and 'TargetValue' in def_row.columns and pd.notna(def_row['TargetValue'].iloc[0]):
                target_val = float(def_row['TargetValue'].iloc[0])

    title = f"{kpi_name} Trend (Tag={tag})"
    fig = _build_trend_chart(series, title=title, target=target_val, thresholds=thresholds)
    st.plotly_chart(fig, use_container_width=True)

    conn.close()


if __name__ == '__main__':
    main()
