# 01 导航与布局规范 (Navigation & Layout)

本文档详细定义了 `CZ Digital Central` 的全局结构与页面分发逻辑。

---

## 1. 全局分层架构

所有业务看板必须基于 `StandardPageLayout` 组件构建。该组件确保了视觉一致性和高密度信息的合理布局。

### 1.1 核心比例与布局

- **Sidebar (侧边栏)**: 固定宽度 `320px` (w-80)。
- **Header (标题栏)**: 采用双行设计，固定在主体上方。
- **Grid (内容区)**: 基于 Tailwind 网格系统，推荐使用 `gap-4` 或 `gap-6`。

---

## 2. 侧边栏交互 (Sidebar Behaviors)

### 2.1 状态定义
- **Inactive (未选中)**: `text-slate-500`。Hover 时背景切换为 `bg-slate-100`。
- **Active (活动态)**: 
    - 背景: `bg-[#004b87]` (Medtronic Navy)。
    - 文字: `text-white`。
    - 阴影: `shadow-lg shadow-blue-900/20`。
    - 动效: `scale-[1.02]` 微缩放。

### 2.2 二级菜单逻辑
- **收纳模式**: 子目录文件（如本规范）默认隐藏。
- **动态展开**: 只有当父目录被选中，或其子文件处于活动状态时，次级导航才会以 `animate-in` 动效展开。

---

## 3. 标题栏规范 (Header Rows)

### 3.1 Row 1: 全局上下文
- **Title (大标题)**: 位于左侧，配以功能图标。
- **Description (副标题)**: 紧随标题下方，`text-slate-400 text-xs`。
- **Actions (动作栏)**: 位于右侧，包含：
    - 全球化切换 (Language Toggle)
    - 主题切换 (Theme Toggle)
    - 全局搜索 (Search Bar)
    - 全屏切换 (Fullscreen)

### 3.2 Row 2: 模块内导航
- **Tabs (选项卡)**: 采用下划线风格，选中项底部显示 3px 宽的 `border-medtronic`。
- **顺序原则**: 遵循 `Overview (总览) -> Analytics (分析) -> Records (明细)`。
