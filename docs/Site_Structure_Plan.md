# 网站结构与导航规划 (Site Structure & Navigation)

## 目标 (Goal)
重构全局侧边栏导航，以对齐新的运营层级架构。本文档定义了主要和次要导航层级。

## 一级导航 (Primary Navigation)

侧边栏菜单项排序如下：

1.  **Dashboard (仪表盘)** (`/`)
    *   *描述：关键指标概览。*
2.  **EHS (环境、健康与安全)**
    *   当前路径：`/production/ehs`
    *   *描述：安全仪表盘，事故报告，隐患排查。*
3.  **Delivery (交付)**
    *   *状态：占位符*
    *   *规划：准时交付率 (OTD)，计划达成率。*
    *   **二级菜单 (Secondary)**:
        *   批记录 (Batch Records)
        *   验证状态 (Validation)
4.  **Efficiency (效率)**
    *   *状态：占位符*
    *   **二级菜单 (Secondary)**:
        *   OEE (设备综合效率)
        *   人工效率
        *   停机时间
        *   设备维护
5.  **Certification (认证与合规)**
    *   *状态：占位符*
    *   **二级菜单 (Secondary)**:
        *   黄带/绿带/黑带认证
        *   A3 项目
6.  **Production (生产)**
    *   当前路径：`/production/labor-eh` (人工效率)
    *   *描述：通用生产追踪，排程。*
7.  **Supply Chain (供应链)**
    *   当前路径：`/inventory` (库存)
    *   *描述：库存水平，物流追踪。*
8.  **Engineering (工程)**
    *   *状态：混合 (Hybrid)*
    *   **二级菜单 (Secondary)**:
        *   Project (项目管理 - 预留)
        *   Server (服务器监控)
            *   服务器状态
            *   服务器日志
            *   服务器维护 (含数据采集/刷新功能)

## 待办事项 (Implementation Steps)

-   [x] **更新 Sidebar**：在 `Sidebar.tsx` 中重新排序 `navItems`。
-   [ ] **设计二级菜单**：重构 `Sidebar.tsx` 以支持折叠/展开的子菜单结构。
-   [ ] **路由映射 (Map Routes)**：
    -   完善上述二级菜单的路由配置。

    -   `EHS` -> `/production/ehs`
    -   `Supply Chain` -> `/inventory`
    -   `Production` -> `/production/labor-eh` (临时默认)
    -   `Engineering` -> `/server` (临时默认)
    -   新项目 (`Delivery`, `Efficiency`, `Certification`) 设置为 `disabled` 或链接到占位页面，直到 `二级菜单` 定义完成。

## 用户行动 (User Actions)
-   [x] **确认层级**：确认现有页面到新分类的映射。
-   [ ] **细化子菜单**：请提供每个分类下的二级菜单结构。

