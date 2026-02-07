# 02 交互组件规范 (Interactive Components)

本文档定义了看板系统中的原子级交互组件及其视觉逻辑。

---

## 1. 按钮系统 (Button Design)

所有组件必须遵循“圆角、阴影、微动效”三大原则。

### 1.1 按钮类型定义

| 类别 | 视觉描述 | 代码参考 (Tailwind) |
| :--- | :--- | :--- |
| **Primary** | 实色深蓝，带强烈投影 | `bg-medtronic text-white rounded-xl shadow-lg` |
| **Segmented** | 浅灰背景，低对比度 | `bg-slate-50 text-slate-500 rounded-xl px-4 py-2` |
| **Action** | 加粗文字，悬停变色 | `bg-slate-100/80 text-slate-700 font-bold` |
| **Control** | 带边框，内部含有图标 | `border border-slate-200 bg-white/50 rounded-xl` |

---

## 2. 交互反馈 (Interactive Feedback)

### 2.1 物理反馈 (Skeuomorphic Touch)
- **Hover (悬停)**: 背景亮度提升 10% 或产生微缩放 `hover:scale-[1.03]`。
- **Active (按下)**: 强制位移缩短 `active:scale-95`。
- **Transition (过渡)**: 统一使用 `transition-all duration-300 ease-in-out`。

### 2.2 状态提示 (Status Indicators)
- **通知点**: 在图标右上角叠加红色实心圆。
- **激活点**: 在列表项侧边显示呼吸动效的绿色/蓝色圆点 (`animate-pulse`)。

---

## 3. 表单与选择器 (Inputs & Selects)

### 3.1 审美令牌
- **圆角**: 统一 `rounded-xl` (12px)。
- **背景**: 即使在 Light 模式也建议使用 `bg-slate-50/50` 增加深度感。
- **边框**: 极细描边 `border-slate-200/50`。

### 3.2 时间选择器 (Date Pickers)
- 必须包含：年份切客、月份切客、前一周期/后一周期快捷导航。
- 选中的时间片段必须使用高对比度的深蓝色高亮。
