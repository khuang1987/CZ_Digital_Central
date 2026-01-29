import streamlit as st
import pyodbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date
import os
 

# 页面配置
st.set_page_config(
    page_title="MDDAP 数据监控面板",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# SQL Server 连接配置（可用环境变量覆盖）
SQL_SERVER = os.getenv('MDDAP_SQL_SERVER', r'localhost\SQLEXPRESS')
SQL_DATABASE = os.getenv('MDDAP_SQL_DATABASE', 'mddap_v2')
SQL_DRIVER = os.getenv('MDDAP_SQL_DRIVER', 'ODBC Driver 17 for SQL Server')

# 获取数据库连接
def get_db_connection():
    """创建新的数据库连接"""
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


def apply_theme():
    """Apply minimal styling that works with both light and dark themes"""
    st.markdown(
        """
        <style>
        .block-container { padding-top: 1.2rem; padding-bottom: 2rem; }
        div[data-testid="stMetric"] { 
            background-color: rgba(128, 128, 128, 0.1); 
            border: 1px solid rgba(128, 128, 128, 0.2); 
            padding: 14px 16px; 
            border-radius: 14px; 
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(ttl=120)
def load_overview(snapshot_date: str) -> pd.DataFrame:
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
      AND s.table_schema = 'dbo'
    """

    df = pd.read_sql_query(sql, conn, params=(snapshot_date,))
    conn.close()

    if not df.empty:
        for c in ["row_count", "today_inserted", "today_updated", "dq_score"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

    return df

# 主界面
def main():
    apply_theme()

    st.title("数据库数据健康总览")
    st.markdown("**[📚 访问技术说明文档 (Technical Docs)](http://localhost:8000)**")

    snapshot = st.sidebar.date_input("选择日期", value=date.today())
    snapshot_str = snapshot.isoformat()
    only_active = st.sidebar.checkbox("仅显示今日有变更的表", value=True)
    dq_threshold = st.sidebar.slider("DQ 风险阈值", min_value=0, max_value=100, value=85)

    with st.spinner("正在加载 meta 汇总..."):
        df = load_overview(snapshot_str)

    if df.empty:
        st.warning("未找到 meta 统计数据。请先运行 meta 汇总 ETL：data_pipelines/monitoring/etl/etl_meta_table_health.py")
        return

    df["dq_score"] = df["dq_score"].fillna(0)
    df["today_inserted"] = df["today_inserted"].fillna(0)
    df["today_updated"] = df["today_updated"].fillna(0)
    df["row_count"] = df["row_count"].fillna(0)

    if only_active:
        df = df[(df["today_inserted"] > 0) | (df["today_updated"] > 0)].copy()

    df["risk_flag"] = (df["dq_score"] < float(dq_threshold)) | df["issues_summary"].fillna("").astype(str).str.startswith("ERROR:")

    total_tables = int(df.shape[0])
    total_rows = int(df["row_count"].sum())
    total_ins = int(df["today_inserted"].sum())
    total_upd = int(df["today_updated"].sum())
    risk_tables = int(df["risk_flag"].sum())
    healthy_tables = int((~df["risk_flag"]).sum())

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        st.metric("表数量", total_tables)
    with c2:
        st.metric("总行数", f"{total_rows:,}")
    with c3:
        st.metric("今日新增", f"{total_ins:,}")
    with c4:
        st.metric("今日更新", f"{total_upd:,}")
    with c5:
        ratio = 0 if total_tables == 0 else (healthy_tables / total_tables) * 100
        st.metric("健康表占比", f"{ratio:.1f}%")
    with c6:
        st.metric("风险表数", risk_tables)

    st.markdown("---")

    left, right = st.columns([1.6, 1])

    with left:
        top_ins = df.sort_values(["today_inserted", "today_updated"], ascending=False).head(12)
        fig = px.bar(top_ins, x="today_inserted", y="table_name", orientation="h", title="今日新增 Top12")
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#E6EEF8",
            margin=dict(l=10, r=10, t=40, b=10),
        )
        st.plotly_chart(fig, use_container_width=True)

        top_size = df.sort_values(["row_count"], ascending=False).head(12)
        fig2 = px.bar(top_size, x="row_count", y="table_name", orientation="h", title="表数据量 Top12")
        fig2.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#E6EEF8",
            margin=dict(l=10, r=10, t=40, b=10),
        )
        st.plotly_chart(fig2, use_container_width=True)

    with right:
        fig3 = px.histogram(df, x="dq_score", nbins=20, title="DQ Score 分布")
        fig3.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#E6EEF8",
            margin=dict(l=10, r=10, t=40, b=10),
        )
        st.plotly_chart(fig3, use_container_width=True)

        risk = df[df["risk_flag"]].sort_values(["dq_score"], ascending=True)
        st.write("风险表列表")
        st.dataframe(risk[["table_name", "dq_score", "issues_summary"]].head(15), use_container_width=True, height=320)

    with st.sidebar:
        st.markdown("---")
        st.markdown("### 🔗 快速链接")
        st.markdown("[📚 访问说明文档 (Docs)](http://localhost:8000)")
        st.markdown("*双击链接在新窗口打开文档*")

if __name__ == "__main__":
    main()
