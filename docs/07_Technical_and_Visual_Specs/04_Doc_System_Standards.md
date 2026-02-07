# 04 文档系统标准 (Doc System Standards)

针对文档中心的阅读体验与 Markdown 渲染引擎的技术实现。

---

## 1. 渲染引擎特性

### 1.1 自动折叠 (Auto-Collapse)
- **H3 (Module Level)**: 作为主要折叠块，默认开启，箭向下。
- **H4 (Section Level)**: 作为次要折叠块。

### 1.2 缩进逻辑
- 内容必须相对于其标题进行视觉缩进：
    - **H3 内容**: `ml-6`。
    - **H4 内容**: `ml-12`。

---

## 2. Mermaid 图表规范

### 2.1 书写约定
所有 Mermaid 图表必须包裹在 `mermaid` 代码块中，并指定图形类型（如 `graph TD` 或 `erDiagram`）。

### 2.2 样式封装
- 渲染器自动将 Mermaid SVG 放入一个带有圆角、极简边框和白色背景的容器中。
- **配色**: 优先使用 Medtronic 指向的主色调（蓝色）。

---

## 3. 技术卡片与提示 (Callouts)

### 3.1 技术细节卡片 (`> `)
渲染引擎会将 Markdown 的 `quote` 语法转义为 `TechnicalCard`:
- **样式**: 左侧蓝色装饰条，顶部带有 Meta 标签头。
- **用途**: 用于标注表来源、更新频次及字段字典。

### 3.2 告警组件 (`[!NOTE]`)
支持 GitHub 风格的 Alert 标记，用于展示重要提醒。
