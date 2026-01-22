"""
Calendar Visualization Page
显示日历表中的工作日/非工作日标记
"""
import streamlit as st
import pyodbc
import pandas as pd
import os
from datetime import date

st.set_page_config(page_title="日历管理", page_icon="📅", layout="wide")

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


def fetch_calendar(conn, year: int, month: int):
    """获取指定年月的日历数据"""
    sql = """
    SELECT 
        CalendarDate, IsWorkday, CumulativeNonWorkDays
    FROM dbo.dim_calendar_cumulative
    WHERE YEAR(CalendarDate) = ? AND MONTH(CalendarDate) = ?
    ORDER BY CalendarDate
    """
    return pd.read_sql(sql, conn, params=(year, month))


def update_workday_status(conn, date_str: str, is_workday: int):
    """更新某天的工作日状态，并重新计算累积非工作日"""
    cursor = conn.cursor()
    # Update the isWorkday flag
    cursor.execute(
        "UPDATE dbo.dim_calendar_cumulative SET IsWorkday = ? WHERE CalendarDate = ?",
        (is_workday, date_str)
    )
    updated = cursor.rowcount
    
    if updated > 0:
        # Recalculate all cumulative values (simple but effective)
        cursor.execute("""
        ;WITH cte AS (
            SELECT 
                CalendarDate,
                IsWorkday,
                SUM(CASE WHEN IsWorkday = 0 THEN 1 ELSE 0 END) OVER (ORDER BY CalendarDate) as NewCumNW
            FROM dbo.dim_calendar_cumulative
        )
        UPDATE c
        SET c.CumulativeNonWorkDays = cte.NewCumNW
        FROM dbo.dim_calendar_cumulative c
        INNER JOIN cte ON c.CalendarDate = cte.CalendarDate
        WHERE c.CumulativeNonWorkDays != cte.NewCumNW;
        """)
    
    conn.commit()
    return updated


def regenerate_calendar():
    """重新生成日历表（调用 ETL 脚本）"""
    import subprocess
    import sys
    
    # Get project root (dashboard is in project/dashboard)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # Run the calendar ETL script
    result = subprocess.run(
        [sys.executable, "-c", 
         "import sys; sys.path.insert(0, '.'); from data_pipelines.sources.dimension.etl.etl_calendar import main; main()"],
        cwd=project_root,
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        # Also regenerate dim_calendar_cumulative
        conn = get_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute(open(os.path.join(project_root, 'data_pipelines/database/schema/create_dim_calendar_cumulative.sql'), encoding='utf-8').read())
            conn.commit()
            conn.close()
        return True, result.stdout
    else:
        return False, result.stderr


# --- UI ---
st.title("📅 日历管理")
st.markdown("查看和编辑工作日/非工作日标记。")



# Date selection
col1, col2 = st.columns(2)
with col1:
    selected_year = st.selectbox("年份", list(range(2024, 2028)), index=2)  # Default 2026
with col2:
    selected_month = st.selectbox("月份", list(range(1, 13)), index=0)  # Default January

conn = get_connection()
if conn:
    df = fetch_calendar(conn, selected_year, selected_month)
    
    if df.empty:
        st.warning("该月份暂无日历数据")
    else:
        # Statistics
        total_days = len(df)
        working_days = df['IsWorkday'].sum()
        non_working_days = total_days - working_days
        
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("总天数", total_days)
        with c2:
            st.metric("工作日", int(working_days))
        with c3:
            st.metric("非工作日", int(non_working_days))
        
        st.markdown("---")
        
        # Calendar Grid View
        st.subheader("📆 日历视图")
        
        # Create a calendar-like grid
        # Get first day of month weekday
        first_day = df.iloc[0]['CalendarDate']
        if isinstance(first_day, str):
            first_day = pd.to_datetime(first_day).date()
        first_weekday = first_day.weekday()  # 0=Monday, 6=Sunday
        
        # Header row
        weekday_names = ['一', '二', '三', '四', '五', '六', '日']
        header_cols = st.columns(7)
        for i, name in enumerate(weekday_names):
            color = "🔴" if i >= 5 else ""
            header_cols[i].markdown(f"**{name}** {color}")
        
        # Build calendar using Streamlit columns instead of raw HTML
        # Add empty cells for days before first day
        all_cells = []
        for _ in range(first_weekday):
            all_cells.append(None)
        
        for _, row in df.iterrows():
            cal_date = row['CalendarDate']
            if isinstance(cal_date, str):
                cal_date = pd.to_datetime(cal_date).date()
            all_cells.append({
                'day': cal_date.day,
                'is_workday': row['IsWorkday']
            })
        
        # Render in rows of 7
        for week_start in range(0, len(all_cells), 7):
            week_cells = all_cells[week_start:week_start + 7]
            cols = st.columns(7)
            for i, cell in enumerate(week_cells):
                if cell is None:
                    cols[i].write("")
                else:
                    if cell['is_workday'] == 1:
                        cols[i].success(f"**{cell['day']}**")
                    else:
                        cols[i].error(f"**{cell['day']}** 🔴")
        
        st.markdown("---")
        
        # Legend
        st.markdown("""
**图例**: 
- 🟢 绿色背景 = 工作日
- 🔴 红色背景 = 非工作日 (周末/节假日)
""")
        
        # Detailed Table
        st.subheader("📋 详细列表")
        
        # Prepare display dataframe
        df_display = df.copy()
        df_display['日期'] = pd.to_datetime(df_display['CalendarDate']).dt.strftime('%Y-%m-%d')
        df_display['周几'] = pd.to_datetime(df_display['CalendarDate']).dt.day_name()
        df_display['状态'] = df_display['IsWorkday'].apply(lambda x: '✅ 工作日' if x == 1 else '🔴 非工作日')
        df_display['累计非工作日'] = df_display['CumulativeNonWorkDays']
        
        st.dataframe(
            df_display[['日期', '周几', '状态', '累计非工作日']],
            use_container_width=True,
            height=400
        )
        
        # Edit section
        st.markdown("---")
        st.subheader("✏️ 编辑工作日状态")
        
        edit_col1, edit_col2, edit_col3 = st.columns([2, 2, 1])
        with edit_col1:
            edit_date = st.date_input("选择日期", value=date(selected_year, selected_month, 1))
        with edit_col2:
            new_status = st.selectbox("新状态", ["工作日", "非工作日"])
        with edit_col3:
            st.write("")
            st.write("")
            if st.button("更新", type="primary"):
                new_is_workday = 1 if new_status == "工作日" else 0
                updated = update_workday_status(conn, edit_date.strftime('%Y-%m-%d'), new_is_workday)
                if updated > 0:
                    st.success(f"已更新 {edit_date} 为 {new_status}")
                    st.rerun()
                else:
                    st.warning("未找到该日期，请确认日期存在于日历表中")
        
        # Manual regeneration section (at bottom)
        st.markdown("---")
        st.subheader("🔄 重新生成日历")
        st.markdown("点击下方按钮可重新生成整个日历表（FY21-FY30），应用最新的法定节假日规则。")
        if st.button("🔄 重新生成日历表", type="secondary"):
            with st.spinner("正在重新生成日历表..."):
                success, output = regenerate_calendar()
                if success:
                    st.success("✅ 日历表已重新生成！")
                    st.code(output)
                    st.rerun()
                else:
                    st.error("❌ 生成失败")
                    st.code(output)
    
    conn.close()
