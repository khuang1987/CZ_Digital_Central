# 03 数据可视化规范 (Data Visualization)

看板的核心价值在于数据密度与直观性。本文档定义了图表与指标的呈现标准。

---

## 1. KPI 卡片标准 (KpiCard)

### 1.1 结构化布局
- **Title**: 置顶，加粗，`tracking-widest`。
- **Main Value**: 中央核心，字体加粗，大小依赖卡片容器。
- **Trend Info**: 底部显示，红色（下降/异常）或绿色（上升/正常）。

### 1.2 配色标准
- **Medtronic Blue**: 默认状态。
- **Emerald Green**: 达标、安全。
- **Amber Orange**: 接近警戒值。
- **Rose Red**: 严重偏差、超期、异常。

---

## 2. 图表标准 (Charts & Recharts)

### 2.1 高度与容器
- **大型趋势图**: `h-[250px]`。
- **多级嵌套图**: `h-[140px]`。
- **背景**: 图表容器应具有阴影，并设置 `overflow-hidden`。

### 2.2 Recharts 配置
- **Tooltip**: 必须自定义，背景使用玻璃拟态（White 80% + Blur 10px）。
- **坐标轴**: 剔除不必要的 `outline` 且文字旋转不超过 45 度。

---

## 3. 明细表格 (Data Tables)

### 3.1 性能与交互
- **Sticky Header**: 表头必须吸顶，背景色为 `bg-slate-50` 以覆盖内容。
- **Row Padding**: 使用 `py-2` 确保高密度展示。
- **Row Hover**: 鼠标滑过时整行高亮，背景颜色 `bg-sky-50/50`。

### 3.2 字段渲染
- **Status Badges**: 所有状态值（如 Open/Closed）必须使用 `StatusBadge` 组件。
- **Percentages**: 统一保留一位小数位。
