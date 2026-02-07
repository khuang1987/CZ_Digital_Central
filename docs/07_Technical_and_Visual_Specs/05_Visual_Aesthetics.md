# 05 视觉审美令牌 (Visual Aesthetics)

定义 CZ Digital Dashboard 的原子级样式基石。

---

## 1. 核心调色盘 (Unified Palette)

| 颜色名称 | 十六进制 | 用途描述 |
| :--- | :--- | :--- |
| **Medtronic Navy** | `#002554` | 系统主基调，侧边栏、背景。 |
| **Active Blue** | `#004b87` | 焦点项、主要按钮。 |
| **Success Green** | `#10b981` | 正常、达标。 |
| **Failure Red** | `#e11d48` | 告警、异常。 |

---

## 2. 深度与阴影 (Elevation)

### 2.1 阴影系统
- **Shadow Base**: `shadow-sm` 用于页面内的小型卡片。
- **Active Item**: `shadow-[0_8px_30px_rgb(0,37,84,0.12)]`。
- **Extreme Depth**: `shadow-2xl` 用于模态框、悬浮浮动按钮。

---

## 3. 玻璃拟态 (Glassmorphism)

### 3.1 参数标准
- **Blur**: `backdrop-blur-md` (12px)。
- **Opacity**: `bg-white/80` (Light) / `bg-slate-900/50` (Dark)。
- **Border**: 使用 `border-white/20` 提升“薄片”感。

---

## 4. 圆角规范 (Radius)

- **原子组件 (Buttons/Badges)**: `rounded-xl` (12px)。
- **容器组件 (Cards/Panels)**: `rounded-2xl` (16px)。
- **特例**: 仅头像或特定系统按钮允许使用 `rounded-full`。
