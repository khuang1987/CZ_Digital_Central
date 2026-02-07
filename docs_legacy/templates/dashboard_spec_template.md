# Dashboard Requirement Specification (Template)

## 1. 页面概览 (Page Overview)
*   **页面名称**: (例如: 设备OEE监控 / 人员效率分析)
*   **核心目标**: (例如: 让主管一目了然看到当日产出与目标的差距)
*   **参考设计**: (可选，粘贴 iOS/Material Design 参考图或手绘草图)

## 2. 布局结构 (Layout Grid)
*   **顶部 (Header)**: (例如: 筛选器 - 日期/车间/产线)
*   **上半部 (KPI Cards)**: 
    *   卡片1: [指标名] (数据来源: table.column)
    *   卡片2: [指标名]
*   **中部 (Charts)**:
    *   主图表: [类型 - 柱状/折线] (X轴: 时间, Y轴: 产量)
*   **下半部 (Tables)**:
    *   明细表: [列名1, 列名2, 列名3...]

## 3. 视觉与交互 (Visual & Interaction)
*   **配色风格**: (例如: iOS 18 极简蓝灰 / 高对比度工业风)
*   **交互逻辑**: 
    *   点击卡片 -> 筛选下方图表?
    *   点击图表 -> 钻取到明细?

## 4. 数据定义 (Data Contract)
*   **API 需求**: (例如: 需要一个新的聚合接口 `/api/oee/summary`)
*   **刷新频率**: (例如: 实时 / 每日刷新)
