# 02 看板页面设计规范 (Web Dashboard Standards)

本文档定义了 `CZ Digital Central` 通用业务看板页面的 UI/UX 开发标准，旨在实现“WOW”级的视觉美感与专业交互体验。

---

## 1. 页面骨架与布局 (Skeleton & Layout)

### 1.1 核心组件：`StandardPageLayout`
所有业务页面必须继承 `StandardPageLayout`，严禁手动拼凑 Header。
- **Row 1 (Header)**: 图标 + 页面大标题 + 描述性副标题 + 全局动作组件。
- **Row 2 (Tabs)**: 遵循 `Overview -> Analytics -> Records` 的三段式导航流。
- **Sidebar (Filters)**: 位于左侧，作为数据筛选的唯一入口。

### 1.2 高密度设计规则 (High-Density)
- **无滚动体验**：在 1080p 分辨率下，首屏应展示核心 KPI 与主要趋势图。
- **圆角规范**：统一使用 `rounded-xl` (12px) 或 `rounded-2xl` (16px)。

---

## 2. 按钮与交互规范 (Button Standards)

所有交互元素需具备明确的物理反馈感。

| 按钮类型 | 样式定义 | 适用场景 |
| :--- | :--- | :--- |
| **Primary (主操作)** | 矩形圆角 (`rounded-xl`), 背景色深蓝 (`bg-[#004b87]`), 带悬浮阴影。 | 提交、核心功能触发、选中态标识。 |
| **Secondary (次操作)** | 圆角矩形, 浅灰背景 (`bg-slate-100`), 无边框。 | 重置、取消、次要辅助操作。 |
| **Ghost (幽灵/图标)** | 背景透明, 仅 Hover 时显示背景。 | 纯图标触发器、顶栏工具链。 |

---

## 3. 面板与质感设计 (Aesthetics)

### 3.1 玻璃拟态 (Glassmorphism)
- **浮动面板**：背景使用 `bg-white/80` (半透明)，配合 `backdrop-blur-md` 模糊效果。
- **描边**：使用浅色半透明描边 (`border-slate-200/50`) 增强边界立体感。

### 3.2 阴影方案 (Shadows)
- **Active 深度**：选中项应使用带有一丝蓝色基调的柔和阴影：`shadow-[0_8px_30px_rgb(0,37,84,0.12)]`。
- **全局 UI**：使用 `shadow-2xl` 强调浮层层级。

---

## 4. 图表与数据可视化 (Charts)

- **高度标准**：
    - 主趋势图/Pareto：固定高度 `h-[250px]`。
    - 侧边辅助图：固定高度 `h-[140px]`。
- **配色**：
    - 正常指标使用 Medtronic Blue 或 Emerald Green。
    - 异常/超期使用 Rose Red。
- **交互**：点击图表项必须联动更新全局过滤器。

---

## 5. 视觉令牌 (Visual Tokens)

- **主色 (Medtronic)**: `#002554` (基础底色) / `#004b87` (Active 高亮)。
- **背景面**: `bg-slate-50` (极简冷灰)。
- **圆角**: `rounded-2xl` (16px)。
- **动效**: `duration-300 ease-in-out` + `active:scale-95`。
