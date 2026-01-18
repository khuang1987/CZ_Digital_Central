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
def load_catalog(snapshot_date: str) -> pd.DataFrame:
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()

    sql = """
    SELECT
        s.snapshot_date,
        s.table_schema,
        s.table_name,
        s.row_count,
        s.today_inserted,
        s.today_updated,
        s.last_updated_at,
        d.dq_score,
        d.issues_summary
    FROM dbo.meta_table_stats_daily s
    LEFT JOIN dbo.meta_table_dq_daily d
      ON d.snapshot_date = s.snapshot_date
     AND d.table_schema = s.table_schema
     AND d.table_name = s.table_name
    WHERE s.snapshot_date = ?
    ORDER BY ISNULL(s.today_inserted, 0) DESC, ISNULL(s.today_updated, 0) DESC, ISNULL(s.row_count, 0) DESC
    """

    df = pd.read_sql_query(sql, conn, params=(snapshot_date,))
    conn.close()

    if not df.empty:
        for c in ["row_count", "today_inserted", "today_updated", "dq_score"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def main():
    st.set_page_config(page_title="数据表目录", layout="wide")
    apply_dark_theme()

    st.title("数据表目录 (Data Catalog)")

    today = date.today()
    snapshot = st.sidebar.date_input("选择日期", value=today)
    snapshot_str = snapshot.isoformat()

    only_active = st.sidebar.checkbox("仅显示今日有变更的表", value=True)
    dq_threshold = st.sidebar.slider("DQ 风险阈值", min_value=0, max_value=100, value=85)
    q = st.sidebar.text_input("搜索表名", value="")

    with st.spinner("正在加载目录..."):
        df = load_catalog(snapshot_str)

    if df.empty:
        st.warning("未找到 meta 统计数据。请先运行 meta 汇总 ETL：data_pipelines/monitoring/etl/etl_meta_table_health.py")
        return

    if only_active:
        df = df[(df["today_inserted"].fillna(0) > 0) | (df["today_updated"].fillna(0) > 0)].copy()

    if q.strip():
        df = df[df["table_name"].astype(str).str.contains(q.strip(), case=False, na=False)].copy()

    df["risk_flag"] = (
        (df["dq_score"].fillna(0) < float(dq_threshold))
        | (df["issues_summary"].fillna("").astype(str).str.startswith("ERROR:"))
    )

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("表数量", int(df.shape[0]))
    with col2:
        st.metric("今日新增合计", int(df["today_inserted"].fillna(0).sum()))
    with col3:
        st.metric("今日更新合计", int(df["today_updated"].fillna(0).sum()))
    with col4:
        st.metric("风险表数", int(df["risk_flag"].sum()))

    st.markdown("---")

    left, right = st.columns([2, 1])
    with left:
        view_cols = [
            "table_name",
            "row_count",
            "today_inserted",
            "today_updated",
            "last_updated_at",
            "dq_score",
            "issues_summary",
        ]
        show_df = df[view_cols].copy()
        show_df = show_df.rename(
            columns={
                "table_name": "table_name",
                "row_count": "row_count",
                "today_inserted": "today_inserted",
                "today_updated": "today_updated",
                "last_updated_at": "last_updated_at",
                "dq_score": "dq_score",
                "issues_summary": "issues_summary",
            }
        )
        st.dataframe(show_df, use_container_width=True, height=520)

    with right:
        top = df.sort_values(["today_inserted", "today_updated"], ascending=False).head(10)
        if not top.empty:
            fig = px.bar(
                top,
                x="today_inserted",
                y="table_name",
                orientation="h",
                title="今日新增 Top10",
                height=420,
            )
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="#E6EEF8",
                margin=dict(l=10, r=10, t=40, b=10),
            )
            st.plotly_chart(fig, use_container_width=True)

        risk = df[df["risk_flag"]].sort_values(["dq_score"], ascending=True).head(10)
        if not risk.empty:
            st.write("风险表 Top10")
            st.dataframe(risk[["table_name", "dq_score", "issues_summary"]], use_container_width=True, height=260)


if __name__ == "__main__":
    main()
