-- MDDAP 数据库初始化脚本
-- 创建时间: 2025-12-12
-- 数据库: SQLite (可迁移到 PostgreSQL)

-- ============================================================
-- MES 批次报告表
-- ============================================================
CREATE TABLE IF NOT EXISTS mes_batch_report (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- 业务主键字段
    batch_number TEXT,
    operation TEXT,
    machine TEXT,
    track_out_time DATETIME,
    
    -- 工厂信息
    factory_source TEXT,
    factory_name TEXT,
    
    -- 产品信息
    product_code TEXT,
    product_name TEXT,
    product_group TEXT,
    
    -- 时间字段
    track_in_time DATETIME,
    track_out_date DATE,
    previous_batch_end_time DATETIME,
    
    -- 计算指标
    lt_days REAL,           -- LT(d)
    pt_days REAL,           -- PT(d)
    st_days REAL,           -- ST(d)
    oee REAL,
    due_time DATETIME,
    nonworkday_days REAL,   -- NonWorkday(d)
    tolerance_hours REAL,   -- Tolerance(h)
    
    -- 状态字段
    completion_status TEXT,
    
    -- 数量字段
    qty_in REAL,
    qty_out REAL,
    qty_scrap REAL,
    
    -- 人员信息
    operator TEXT,
    
    -- 设备信息
    machine_number TEXT,
    
    -- 元数据
    source_file TEXT,
    record_hash TEXT UNIQUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_mes_track_out_time ON mes_batch_report(track_out_time);
CREATE INDEX IF NOT EXISTS idx_mes_factory ON mes_batch_report(factory_source);
CREATE INDEX IF NOT EXISTS idx_mes_batch_number ON mes_batch_report(batch_number);
CREATE INDEX IF NOT EXISTS idx_mes_product_code ON mes_batch_report(product_code);
CREATE INDEX IF NOT EXISTS idx_mes_record_hash ON mes_batch_report(record_hash);

-- ============================================================
-- SFC 批次报告表
-- ============================================================
CREATE TABLE IF NOT EXISTS sfc_batch_report (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- 业务主键字段
    batch_number TEXT,
    operation TEXT,
    work_center TEXT,
    
    -- 时间字段
    actual_start_time DATETIME,
    actual_end_time DATETIME,
    
    -- 产品信息
    material_code TEXT,
    material_description TEXT,
    
    -- 数量字段
    yield_qty REAL,
    scrap_qty REAL,
    
    -- 人员信息
    operator TEXT,
    
    -- 元数据
    source_file TEXT,
    record_hash TEXT UNIQUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_sfc_actual_end_time ON sfc_batch_report(actual_end_time);
CREATE INDEX IF NOT EXISTS idx_sfc_batch_number ON sfc_batch_report(batch_number);
CREATE INDEX IF NOT EXISTS idx_sfc_record_hash ON sfc_batch_report(record_hash);

-- ============================================================
-- SFC 产品检验表
-- ============================================================
CREATE TABLE IF NOT EXISTS sfc_product_inspection (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- 业务主键字段
    inspection_lot TEXT,
    batch_number TEXT,
    
    -- 检验信息
    inspection_type TEXT,
    inspection_result TEXT,
    inspection_date DATE,
    
    -- 产品信息
    material_code TEXT,
    material_description TEXT,
    
    -- 数量字段
    sample_size REAL,
    defect_qty REAL,
    
    -- 元数据
    source_file TEXT,
    record_hash TEXT UNIQUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_sfc_insp_date ON sfc_product_inspection(inspection_date);
CREATE INDEX IF NOT EXISTS idx_sfc_insp_lot ON sfc_product_inspection(inspection_lot);
CREATE INDEX IF NOT EXISTS idx_sfc_insp_hash ON sfc_product_inspection(record_hash);

-- ============================================================
-- SAP 工艺路线表
-- ============================================================
CREATE TABLE IF NOT EXISTS sap_routing (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- 业务主键字段
    material_code TEXT,
    plant TEXT,
    operation_number TEXT,
    
    -- 工艺信息
    work_center TEXT,
    operation_description TEXT,
    standard_time REAL,
    setup_time REAL,
    
    -- 元数据
    source_file TEXT,
    record_hash TEXT UNIQUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_sap_material ON sap_routing(material_code);
CREATE INDEX IF NOT EXISTS idx_sap_plant ON sap_routing(plant);
CREATE INDEX IF NOT EXISTS idx_sap_hash ON sap_routing(record_hash);

-- ============================================================
-- ETL 文件状态表（文件级增量检测）
-- ============================================================
CREATE TABLE IF NOT EXISTS etl_file_state (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    etl_name TEXT NOT NULL,           -- ETL名称：'mes_batch_report', 'sfc_batch_report' 等
    file_path TEXT NOT NULL,          -- 文件完整路径
    file_mtime REAL,                  -- 文件修改时间（Unix时间戳）
    file_size INTEGER,                -- 文件大小（字节）
    processed_time DATETIME,          -- 处理时间
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(etl_name, file_path)
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_file_state_etl ON etl_file_state(etl_name);
CREATE INDEX IF NOT EXISTS idx_file_state_path ON etl_file_state(file_path);

-- ============================================================
-- ETL 运行日志表
-- ============================================================
CREATE TABLE IF NOT EXISTS etl_run_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    etl_name TEXT NOT NULL,
    run_start DATETIME NOT NULL,
    run_end DATETIME,
    status TEXT,  -- 'running', 'success', 'failed'
    
    records_read INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    records_skipped INTEGER,
    
    error_message TEXT,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_etl_log_name ON etl_run_log(etl_name);
CREATE INDEX IF NOT EXISTS idx_etl_log_start ON etl_run_log(run_start);
