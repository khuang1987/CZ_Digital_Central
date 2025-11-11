// 自动更新最后刷新时间
// 此查询会在每次刷新时记录当前时间，用于下次增量刷新
// 注意：Power Query无法直接修改参数，此查询仅用于记录
// 实际使用时，可以手动将结果复制到"参数_最后刷新时间"查询中

// 获取当前时间
DateTime.LocalNow() meta [IsParameterQuery=false]
