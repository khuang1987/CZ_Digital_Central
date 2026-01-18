"""
工时分析看板
Labor Hours Dashboard

基于行业最佳实践设计的工时分析看板，包含：
- KPI 卡片：总工时、当月工时、YTD工时、环比/同比
- 趋势图：每日/每周/每月工时趋势
- 分布图：按工作中心、工序、物料分布
- 明细表：可筛选的工时明细数据
"""

import streamlit as st
import pyodbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os

# SQL Server 连接配置（可用环境变量覆盖）
SQL_SERVER = os.getenv('MDDAP_SQL_SERVER', r'localhost\SQLEXPRESS')
SQL_DATABASE = os.getenv('MDDAP_SQL_DATABASE', 'mddap_v2')
SQL_DRIVER = os.getenv('MDDAP_SQL_DRIVER', 'ODBC Driver 17 for SQL Server')

def get_db_connection():
    """创建数据库连接"""
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

@st.cache_data(ttl=300)
def load_labor_hours_data():
    """加载工时数据"""
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    
    query = """
    SELECT 
        PostingDate as posting_date,
        Plant as plant,
        WorkCenter as work_center,
        WorkCenterDesc as work_center_desc,
        CostCenter as cost_center,
        Material as material,
        MaterialDesc as material_desc,
        MaterialType as material_type,
        OrderNumber as order_no,
        OrderType as order_type,
        Operation as operation,
        OperationDesc as operation_desc,
        ProductionScheduler as scheduler,
        ProductionSchedulerDesc as scheduler_desc,
        EarnedLaborTime as labor_hours,
        MachineTime as machine_hours,
        ActualQuantity as actual_qty,
        ActualScrapQty as scrap_qty,
        TargetQuantity as target_qty
    FROM dbo.raw_sap_labor_hours
    WHERE PostingDate IS NOT NULL
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if not df.empty:
        df['posting_date'] = pd.to_datetime(df['posting_date'])
        df['year'] = df['posting_date'].dt.year
        df['month'] = df['posting_date'].dt.month
        df['week'] = df['posting_date'].dt.isocalendar().week
        df['weekday'] = df['posting_date'].dt.dayofweek
        df['year_month'] = df['posting_date'].dt.to_period('M').astype(str)
    
    return df

@st.cache_data(ttl=300)
def load_calendar_data():
    """加载日历数据"""
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    
    query = """
    SELECT 
        date,
        fiscal_year,
        fiscal_month,
        fiscal_week,
        fiscal_quarter,
        is_workday,
        holiday_name
    FROM dbo.dim_calendar
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if not df.empty:
        df['date'] = pd.to_datetime(df['date'])
    
    return df

@st.cache_data(ttl=300)
def load_planned_hours_data():
    """加载计划工时数据"""
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    
    query = """
    SELECT 
        plan_date,
        cz_planned_hours,
        kh_planned_hours
    FROM dbo.planned_labor_hours
    WHERE plan_date IS NOT NULL
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if not df.empty:
        df['plan_date'] = pd.to_datetime(df['plan_date'])
    
    return df

def calculate_kpis(df, df_calendar, df_planned, selected_date_range):
    """计算 KPI 指标"""
    if df.empty:
        return {}
    
    # 筛选日期范围
    start_date, end_date = selected_date_range
    mask = (df['posting_date'] >= pd.Timestamp(start_date)) & (df['posting_date'] <= pd.Timestamp(end_date))
    df_filtered = df[mask]
    
    # 当前选择范围的工时
    total_labor = df_filtered['labor_hours'].sum()
    total_machine = df_filtered['machine_hours'].sum()
    total_qty = df_filtered['actual_qty'].sum()
    total_scrap = df_filtered['scrap_qty'].sum()
    
    # 计划工时
    if not df_planned.empty:
        plan_mask = (df_planned['plan_date'] >= pd.Timestamp(start_date)) & (df_planned['plan_date'] <= pd.Timestamp(end_date))
        df_plan_filtered = df_planned[plan_mask]
        planned_cz = df_plan_filtered['cz_planned_hours'].sum()
        planned_kh = df_plan_filtered['kh_planned_hours'].sum()
        total_planned = planned_cz + planned_kh
    else:
        planned_cz = planned_kh = total_planned = 0
    
    # 达成率
    achievement_rate = (total_labor / total_planned * 100) if total_planned > 0 else 0
    variance = total_labor - total_planned
    
    # 当月工时 (基于结束日期的月份)
    current_month = end_date.month
    current_year = end_date.year
    month_mask = (df['posting_date'].dt.month == current_month) & (df['posting_date'].dt.year == current_year)
    month_labor = df[month_mask]['labor_hours'].sum()
    
    # YTD 工时
    ytd_mask = (df['posting_date'].dt.year == current_year) & (df['posting_date'] <= pd.Timestamp(end_date))
    ytd_labor = df[ytd_mask]['labor_hours'].sum()
    
    # 上月工时 (环比)
    last_month = current_month - 1 if current_month > 1 else 12
    last_month_year = current_year if current_month > 1 else current_year - 1
    last_month_mask = (df['posting_date'].dt.month == last_month) & (df['posting_date'].dt.year == last_month_year)
    last_month_labor = df[last_month_mask]['labor_hours'].sum()
    
    # 去年同月 (同比)
    last_year_mask = (df['posting_date'].dt.month == current_month) & (df['posting_date'].dt.year == current_year - 1)
    last_year_labor = df[last_year_mask]['labor_hours'].sum()
    
    # 计算增长率
    mom_growth = ((month_labor - last_month_labor) / last_month_labor * 100) if last_month_labor > 0 else 0
    yoy_growth = ((month_labor - last_year_labor) / last_year_labor * 100) if last_year_labor > 0 else 0
    
    # 报废率
    scrap_rate = (total_scrap / (total_qty + total_scrap) * 100) if (total_qty + total_scrap) > 0 else 0
    
    # 工作日数 (从日历表)
    if not df_calendar.empty:
        cal_mask = (df_calendar['date'] >= pd.Timestamp(start_date)) & (df_calendar['date'] <= pd.Timestamp(end_date))
        workdays = df_calendar[cal_mask & (df_calendar['is_workday'] == 1)].shape[0]
    else:
        workdays = (end_date - start_date).days + 1
    
    avg_daily_hours = total_labor / workdays if workdays > 0 else 0
    
    return {
        'total_labor': total_labor,
        'total_machine': total_machine,
        'total_qty': total_qty,
        'total_scrap': total_scrap,
        'total_planned': total_planned,
        'planned_cz': planned_cz,
        'planned_kh': planned_kh,
        'achievement_rate': achievement_rate,
        'variance': variance,
        'month_labor': month_labor,
        'ytd_labor': ytd_labor,
        'mom_growth': mom_growth,
        'yoy_growth': yoy_growth,
        'scrap_rate': scrap_rate,
        'workdays': workdays,
        'avg_daily_hours': avg_daily_hours
    }

def render_kpi_cards(kpis):
    """渲染 KPI 卡片"""
    # 第一行 KPI - 实际与计划对比
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="📊 实际工时",
            value=f"{kpis.get('total_labor', 0):,.1f} 小时",
            delta=f"计划: {kpis.get('total_planned', 0):,.1f} 小时"
        )
    
    with col2:
        st.metric(
            label="🎯 达成率",
            value=f"{kpis.get('achievement_rate', 0):.1f}%",
            delta=f"{kpis.get('variance', 0):+.1f} 小时"
        )
    
    with col3:
        st.metric(
            label="📅 当月工时",
            value=f"{kpis.get('month_labor', 0):,.1f} 小时",
            delta=f"{kpis.get('mom_growth', 0):+.1f}% 环比"
        )
    
    with col4:
        st.metric(
            label="📈 YTD 工时",
            value=f"{kpis.get('ytd_labor', 0):,.1f} 小时",
            delta=f"{kpis.get('yoy_growth', 0):+.1f}% 同比"
        )
    
    # 第二行 KPI - 工厂分解
    col5, col6, col7, col8 = st.columns(4)
    
    with col5:
        st.metric(
            label="🏭 常州计划",
            value=f"{kpis.get('planned_cz', 0):,.1f} 小时",
            delta=None
        )
    
    with col6:
        st.metric(
            label="🏭 康辉计划",
            value=f"{kpis.get('planned_kh', 0):,.1f} 小时",
            delta=None
        )
    
    with col7:
        st.metric(
            label="⚙️ 机器工时",
            value=f"{kpis.get('total_machine', 0):,.1f} 小时",
            delta=None
        )
    
    with col8:
        st.metric(
            label="⏱️ 日均工时",
            value=f"{kpis.get('avg_daily_hours', 0):,.1f} 小时/天",
            delta=None
        )
    
    # 第三行 KPI - 其他指标
    col9, col10, col11, col12 = st.columns(4)
    
    with col9:
        st.metric(
            label="📦 实际产量",
            value=f"{kpis.get('total_qty', 0):,.0f}",
            delta=None
        )
    
    with col10:
        st.metric(
            label="🗑️ 报废数量",
            value=f"{kpis.get('total_scrap', 0):,.0f}",
            delta=f"{kpis.get('scrap_rate', 0):.2f}% 报废率"
        )
    
    with col11:
        st.metric(
            label="📆 工作日数",
            value=f"{kpis.get('workdays', 0)} 天",
            delta=None
        )
    
    with col12:
        st.metric(
            label="📊 效率指数",
            value=f"{kpis.get('achievement_rate', 0) / 100:.2f}",
            delta=None
        )

def render_trend_chart(df, df_planned, date_range, granularity='daily'):
    """渲染趋势图"""
    start_date, end_date = date_range
    mask = (df['posting_date'] >= pd.Timestamp(start_date)) & (df['posting_date'] <= pd.Timestamp(end_date))
    df_filtered = df[mask].copy()
    
    if df_filtered.empty:
        st.warning("选定日期范围内没有数据")
        return
    
    # 处理实际工时数据
    if granularity == 'daily':
        trend_df = df_filtered.groupby('posting_date').agg({
            'labor_hours': 'sum',
            'machine_hours': 'sum',
            'actual_qty': 'sum'
        }).reset_index()
        x_col = 'posting_date'
        x_title = '日期'
    elif granularity == 'weekly':
        df_filtered['year_week'] = df_filtered['posting_date'].dt.strftime('%Y-W%W')
        trend_df = df_filtered.groupby('year_week').agg({
            'labor_hours': 'sum',
            'machine_hours': 'sum',
            'actual_qty': 'sum'
        }).reset_index()
        x_col = 'year_week'
        x_title = '周'
    else:  # monthly
        trend_df = df_filtered.groupby('year_month').agg({
            'labor_hours': 'sum',
            'machine_hours': 'sum',
            'actual_qty': 'sum'
        }).reset_index()
        x_col = 'year_month'
        x_title = '月份'
    
    # 处理计划工时数据
    if not df_planned.empty:
        plan_mask = (df_planned['plan_date'] >= pd.Timestamp(start_date)) & (df_planned['plan_date'] <= pd.Timestamp(end_date))
        df_plan_filtered = df_planned[plan_mask].copy()
        
        if granularity == 'daily':
            df_plan_filtered = df_plan_filtered.set_index('plan_date')
            df_plan_filtered['total_planned'] = df_plan_filtered['cz_planned_hours'] + df_plan_filtered['kh_planned_hours']
            plan_df = df_plan_filtered[['total_planned']].reset_index()
            plan_df.columns = [x_col, 'planned_hours']
        elif granularity == 'weekly':
            df_plan_filtered['year_week'] = df_plan_filtered['plan_date'].dt.strftime('%Y-W%W')
            df_plan_filtered['total_planned'] = df_plan_filtered['cz_planned_hours'] + df_plan_filtered['kh_planned_hours']
            plan_df = df_plan_filtered.groupby('year_week')['total_planned'].sum().reset_index()
            plan_df.columns = [x_col, 'planned_hours']
        else:  # monthly
            df_plan_filtered['year_month'] = df_plan_filtered['plan_date'].dt.to_period('M').astype(str)
            df_plan_filtered['total_planned'] = df_plan_filtered['cz_planned_hours'] + df_plan_filtered['kh_planned_hours']
            plan_df = df_plan_filtered.groupby('year_month')['total_planned'].sum().reset_index()
            plan_df.columns = [x_col, 'planned_hours']
    else:
        plan_df = pd.DataFrame(columns=[x_col, 'planned_hours'])
    
    # 合并实际和计划数据
    merged_df = pd.merge(trend_df, plan_df, on=x_col, how='left')
    
    fig = go.Figure()
    
    # 实际工时（柱状图）
    fig.add_trace(go.Bar(
        x=merged_df[x_col],
        y=merged_df['labor_hours'],
        name='实际人工工时',
        marker_color='#1f77b4'
    ))
    
    # 计划工时（折线图）
    fig.add_trace(go.Scatter(
        x=merged_df[x_col],
        y=merged_df['planned_hours'],
        name='计划工时',
        mode='lines+markers',
        yaxis='y2',
        line=dict(color='#2ca02c', width=2)
    ))
    
    # 机器工时（折线图）
    fig.add_trace(go.Scatter(
        x=merged_df[x_col],
        y=merged_df['machine_hours'],
        name='机器工时',
        mode='lines+markers',
        yaxis='y3',
        line=dict(color='#ff7f0e', width=2)
    ))
    
    fig.update_layout(
        title=f'工时趋势对比 ({granularity})',
        xaxis_title=x_title,
        yaxis=dict(title='实际人工工时 (小时)'),
        yaxis2=dict(title='计划工时 (小时)', overlaying='y', side='right'),
        yaxis3=dict(title='机器工时 (小时)', overlaying='y', side='right', position=0.85),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        height=400,
        hovermode='x unified'
    )
    
    st.plotly_chart(fig, use_container_width=True)

def render_distribution_charts(df, date_range):
    """渲染分布图"""
    start_date, end_date = date_range
    mask = (df['posting_date'] >= pd.Timestamp(start_date)) & (df['posting_date'] <= pd.Timestamp(end_date))
    df_filtered = df[mask]
    
    if df_filtered.empty:
        st.warning("选定日期范围内没有数据")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        # 按工作中心分布
        wc_df = df_filtered.groupby('work_center_desc')['labor_hours'].sum().reset_index()
        wc_df = wc_df.sort_values('labor_hours', ascending=False).head(10)
        
        fig1 = px.bar(
            wc_df,
            x='labor_hours',
            y='work_center_desc',
            orientation='h',
            title='Top 10 工作中心工时分布',
            labels={'labor_hours': '工时 (小时)', 'work_center_desc': '工作中心'}
        )
        fig1.update_layout(height=400, yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        # 按工序分布
        op_df = df_filtered.groupby('operation_desc')['labor_hours'].sum().reset_index()
        op_df = op_df.sort_values('labor_hours', ascending=False).head(10)
        
        fig2 = px.bar(
            op_df,
            x='labor_hours',
            y='operation_desc',
            orientation='h',
            title='Top 10 工序工时分布',
            labels={'labor_hours': '工时 (小时)', 'operation_desc': '工序'}
        )
        fig2.update_layout(height=400, yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig2, use_container_width=True)
    
    col3, col4 = st.columns(2)
    
    with col3:
        # 按物料类型分布 (饼图)
        mt_df = df_filtered.groupby('material_type')['labor_hours'].sum().reset_index()
        mt_df = mt_df[mt_df['labor_hours'] > 0]
        
        fig3 = px.pie(
            mt_df,
            values='labor_hours',
            names='material_type',
            title='物料类型工时占比',
            hole=0.4
        )
        fig3.update_layout(height=400)
        st.plotly_chart(fig3, use_container_width=True)
    
    with col4:
        # 按订单类型分布
        ot_df = df_filtered.groupby('order_type')['labor_hours'].sum().reset_index()
        ot_df = ot_df[ot_df['labor_hours'] > 0]
        
        fig4 = px.pie(
            ot_df,
            values='labor_hours',
            names='order_type',
            title='订单类型工时占比',
            hole=0.4
        )
        fig4.update_layout(height=400)
        st.plotly_chart(fig4, use_container_width=True)

def render_heatmap(df, date_range):
    """渲染热力图 - 按星期和工作中心"""
    start_date, end_date = date_range
    mask = (df['posting_date'] >= pd.Timestamp(start_date)) & (df['posting_date'] <= pd.Timestamp(end_date))
    df_filtered = df[mask]
    
    if df_filtered.empty:
        return
    
    # 按星期几和工作中心汇总
    weekday_names = ['周一', '周二', '周三', '周四', '周五', '周六', '周日']
    df_filtered['weekday_name'] = df_filtered['weekday'].map(lambda x: weekday_names[x])
    
    # 获取 Top 10 工作中心
    top_wc = df_filtered.groupby('work_center_desc')['labor_hours'].sum().nlargest(10).index.tolist()
    df_top = df_filtered[df_filtered['work_center_desc'].isin(top_wc)]
    
    heatmap_df = df_top.pivot_table(
        values='labor_hours',
        index='work_center_desc',
        columns='weekday_name',
        aggfunc='sum',
        fill_value=0
    )
    
    # 重新排序列
    heatmap_df = heatmap_df.reindex(columns=weekday_names, fill_value=0)
    
    fig = px.imshow(
        heatmap_df,
        labels=dict(x='星期', y='工作中心', color='工时'),
        title='工作中心 × 星期 工时热力图',
        color_continuous_scale='Blues'
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

def render_detail_table(df, date_range):
    """渲染明细表"""
    start_date, end_date = date_range
    mask = (df['posting_date'] >= pd.Timestamp(start_date)) & (df['posting_date'] <= pd.Timestamp(end_date))
    df_filtered = df[mask].copy()
    
    if df_filtered.empty:
        st.warning("选定日期范围内没有数据")
        return
    
    # 显示列选择
    display_cols = [
        'posting_date', 'work_center_desc', 'operation_desc', 
        'material_desc', 'order_no', 'labor_hours', 'machine_hours',
        'actual_qty', 'scrap_qty'
    ]
    
    df_display = df_filtered[display_cols].copy()
    df_display.columns = [
        '日期', '工作中心', '工序', '物料描述', '订单号',
        '人工工时', '机器工时', '实际数量', '报废数量'
    ]
    
    # 格式化日期
    df_display['日期'] = df_display['日期'].dt.strftime('%Y-%m-%d')
    
    st.dataframe(
        df_display,
        use_container_width=True,
        height=400,
        hide_index=True
    )
    
    # 下载按钮
    csv = df_display.to_csv(index=False, encoding='utf-8-sig')
    st.download_button(
        label="📥 下载数据 (CSV)",
        data=csv,
        file_name=f"labor_hours_{start_date}_{end_date}.csv",
        mime="text/csv"
    )

def main():
    st.title("⏱️ 工时分析看板")
    st.markdown("---")
    
    # 加载数据
    with st.spinner("加载数据中..."):
        df = load_labor_hours_data()
        df_calendar = load_calendar_data()
        df_planned = load_planned_hours_data()
    
    if df.empty:
        st.error("没有工时数据，请先导入数据")
        return
    
    # 侧边栏筛选器
    st.sidebar.header("📅 筛选条件")
    
    # 日期范围
    min_date = df['posting_date'].min().date()
    max_date = df['posting_date'].max().date()
    
    # 快捷日期选择
    date_preset = st.sidebar.selectbox(
        "快捷选择",
        ["自定义", "本月", "上月", "本季度", "本年", "最近30天", "最近90天"]
    )
    
    today = datetime.now().date()
    
    if date_preset == "本月":
        start_date = today.replace(day=1)
        end_date = today
    elif date_preset == "上月":
        first_of_month = today.replace(day=1)
        end_date = first_of_month - timedelta(days=1)
        start_date = end_date.replace(day=1)
    elif date_preset == "本季度":
        quarter = (today.month - 1) // 3
        start_date = today.replace(month=quarter * 3 + 1, day=1)
        end_date = today
    elif date_preset == "本年":
        start_date = today.replace(month=1, day=1)
        end_date = today
    elif date_preset == "最近30天":
        start_date = today - timedelta(days=30)
        end_date = today
    elif date_preset == "最近90天":
        start_date = today - timedelta(days=90)
        end_date = today
    else:
        start_date = st.sidebar.date_input("开始日期", value=max_date - timedelta(days=30), min_value=min_date, max_value=max_date)
        end_date = st.sidebar.date_input("结束日期", value=max_date, min_value=min_date, max_value=max_date)
    
    date_range = (start_date, end_date)
    
    st.sidebar.markdown(f"**选定范围**: {start_date} ~ {end_date}")
    
    # 工作中心筛选
    work_centers = ['全部'] + sorted(df['work_center_desc'].dropna().unique().tolist())
    selected_wc = st.sidebar.multiselect("工作中心", work_centers, default=['全部'])
    
    if '全部' not in selected_wc and selected_wc:
        df = df[df['work_center_desc'].isin(selected_wc)]
    
    # 计算 KPI
    kpis = calculate_kpis(df, df_calendar, df_planned, date_range)
    
    # 渲染 KPI 卡片
    st.subheader("📊 关键指标")
    render_kpi_cards(kpis)
    
    st.markdown("---")
    
    # 趋势图
    st.subheader("📈 工时趋势")
    granularity = st.radio("时间粒度", ["daily", "weekly", "monthly"], horizontal=True, format_func=lambda x: {"daily": "按日", "weekly": "按周", "monthly": "按月"}[x])
    render_trend_chart(df, df_planned, date_range, granularity)
    
    st.markdown("---")
    
    # 分布图
    st.subheader("📊 工时分布")
    render_distribution_charts(df, date_range)
    
    st.markdown("---")
    
    # 热力图
    st.subheader("🗓️ 工时热力图")
    render_heatmap(df, date_range)
    
    st.markdown("---")
    
    # 明细表
    st.subheader("📋 工时明细")
    render_detail_table(df, date_range)

if __name__ == "__main__":
    main()
