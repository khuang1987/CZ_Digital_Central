-- ============================================
-- 查询名称: Operation Mapping (工序映射表)
-- 报表: 01_labor_hours
-- 数据源: dim_operation_mapping
-- 说明: 工序名称与分类映射
-- ============================================

SELECT 
    operation_name,
    standard_routing,
    area,
    lead_time,
    erp_code
FROM dim_operation_mapping
ORDER BY operation_name;
