import os
from datetime import date

import pandas as pd
import plotly.express as px
import pyodbc
import streamlit as st


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


def apply_dark_theme():
    st.markdown(
        """
        <style>
        .block-container { padding-top: 1.2rem; padding-bottom: 2rem; }
        [data-testid="stSidebar"] { background: #0B1220; }
        html, body, [class*="css"]  { background-color: #070B14; color: #E6EEF8; }
        .stApp { background: linear-gradient(180deg, #070B14 0%, #070B14 100%); }
        div[data-testid="stMetric"] { background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.08); padding: 14px 16px; border-radius: 14px; }
        div[data-testid="stDataFrame"] { background: rgba(255,255,255,0.02); }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(ttl=120)
def load_tables(snapshot_date: str) -> pd.DataFrame:
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()

    df = pd.read_sql_query(
        """
        SELECT table_name
        FROM dbo.meta_table_stats_daily
        WHERE snapshot_date = ? AND table_schema='dbo'
        ORDER BY table_name
        """,
        conn,
        params=(snapshot_date,),
    )
    conn.close()
    return df


@st.cache_data(ttl=120)
def load_meta_for_table(snapshot_date: str, table_name: str):
    conn = get_db_connection()
    if conn is None:
        return None, None

    s = pd.read_sql_query(
        """
        SELECT TOP 1 *
        FROM dbo.meta_table_stats_daily
        WHERE snapshot_date = ? AND table_schema='dbo' AND table_name = ?
        """,
        conn,
        params=(snapshot_date, table_name),
    )

    d = pd.read_sql_query(
        """
        SELECT TOP 1 *
        FROM dbo.meta_table_dq_daily
        WHERE snapshot_date = ? AND table_schema='dbo' AND table_name = ?
        """,
        conn,
        params=(snapshot_date, table_name),
    )

    conn.close()
    return (s.iloc[0].to_dict() if not s.empty else None), (d.iloc[0].to_dict() if not d.empty else None)


@st.cache_data(ttl=120)
def load_trend(table_name: str, days: int = 30) -> pd.DataFrame:
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()

    df = pd.read_sql_query(
        """
        SELECT
            s.snapshot_date,
            s.today_inserted,
            s.today_updated,
            s.row_count,
            d.dq_score,
            d.issues_summary
        FROM dbo.meta_table_stats_daily s
        LEFT JOIN dbo.meta_table_dq_daily d
          ON d.snapshot_date = s.snapshot_date
         AND d.table_schema = s.table_schema
         AND d.table_name = s.table_name
        WHERE s.table_schema='dbo'
          AND s.table_name=?
          AND s.snapshot_date >= DATEADD(day, -?, CAST(GETDATE() AS date))
        ORDER BY s.snapshot_date
        """,
        conn,
        params=(table_name, int(days)),
    )

    conn.close()

    if not df.empty:
        for c in ["today_inserted", "today_updated", "row_count", "dq_score"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


@st.cache_data(ttl=60)
def load_columns(table_name: str) -> pd.DataFrame:
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()

    df = pd.read_sql_query(
        """
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=?
        ORDER BY ORDINAL_POSITION
        """,
        conn,
        params=(table_name,),
    )
    conn.close()
    return df


@st.cache_data(ttl=60)
def load_sample(table_name: str, limit: int = 50) -> pd.DataFrame:
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()

    df = pd.read_sql_query(f"SELECT TOP {int(limit)} * FROM dbo.[{table_name}]", conn)
    conn.close()
    return df


def main():
    st.set_page_config(page_title="单表详情", layout="wide")
    apply_dark_theme()

    st.title("单表详情 (Table Detail)")

    today = date.today()
    snapshot = st.sidebar.date_input("选择日期", value=today)
    snapshot_str = snapshot.isoformat()

    tables_df = load_tables(snapshot_str)
    if tables_df.empty:
        st.warning("未找到 meta 统计数据。请先运行 meta 汇总 ETL：data_pipelines/monitoring/etl/etl_meta_table_health.py")
        return

    options = tables_df["table_name"].astype(str).tolist()

    default_table = None
    try:
        qp = st.query_params
        default_table = qp.get("table", None)
        if isinstance(default_table, list):
            default_table = default_table[0] if default_table else None
    except Exception:
        default_table = None

    idx = 0
    if default_table in options:
        idx = options.index(default_table)

    table_name = st.sidebar.selectbox("选择表", options=options, index=idx)

    s, d = load_meta_for_table(snapshot_str, table_name)

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("行数", int((s or {}).get("row_count") or 0))
    with col2:
        st.metric("今日新增", int((s or {}).get("today_inserted") or 0))
    with col3:
        st.metric("今日更新", int((s or {}).get("today_updated") or 0))
    with col4:
        dq_score = (d or {}).get("dq_score")
        st.metric("DQ Score", "N/A" if dq_score is None else f"{float(dq_score):.1f}")
    with col5:
        freshness = (d or {}).get("freshness_hours")
        st.metric("Freshness(h)", "N/A" if freshness is None else f"{float(freshness):.1f}")

    if d is not None:
        st.caption(f"Issues: {(d.get('issues_summary') or 'OK')}")

    trend_days = st.sidebar.selectbox("趋势范围(天)", options=[7, 14, 30, 60], index=2)
    trend = load_trend(table_name, days=int(trend_days))

    if not trend.empty:
        left, right = st.columns(2)
        with left:
            fig = px.line(
                trend,
                x="snapshot_date",
                y=["today_inserted", "today_updated"],
                title="每日新增/更新趋势",
            )
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="#E6EEF8",
                margin=dict(l=10, r=10, t=40, b=10),
            )
            st.plotly_chart(fig, use_container_width=True)

        with right:
            fig2 = px.line(trend, x="snapshot_date", y="dq_score", title="DQ Score 趋势")
            fig2.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="#E6EEF8",
                margin=dict(l=10, r=10, t=40, b=10),
            )
            st.plotly_chart(fig2, use_container_width=True)

    st.markdown("---")

    tabs = st.tabs(["字段结构", "样例数据", "近30天汇总"])

    with tabs[0]:
        cols = load_columns(table_name)
        st.dataframe(cols, use_container_width=True, height=460)

    with tabs[1]:
        limit = st.slider("样例行数", 10, 200, 50, 10)
        sample = load_sample(table_name, limit=int(limit))
        st.dataframe(sample, use_container_width=True, height=520)

    with tabs[2]:
        if trend.empty:
            st.info("暂无趋势数据")
        else:
            st.dataframe(trend.tail(60), use_container_width=True, height=520)


if __name__ == "__main__":
    main()
