// 参数：最后刷新时间
// 用途：用于增量刷新，只加载此时间之后创建的文件
// 更新方式：每次刷新后自动更新，或手动设置为需要的日期

// 默认值：可以设置为一个较早的日期来全量加载，或者设置为最近的时间来增量加载
// 建议：首次使用设置为1-2周前的日期，后续会自动更新
#date(2025, 1, 1) meta [IsParameterQuery=true, Type="DateTime", IsParameterQueryRequired=true]
