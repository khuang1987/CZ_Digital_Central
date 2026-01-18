-- KPI 监控模块数据库 Schema
-- 用于初始化或更新 mddap_v2.db

-- 1. KPI 定义表
CREATE TABLE IF NOT EXISTS KPI_Definition (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    Name TEXT NOT NULL UNIQUE,
    Description TEXT,
    TargetValue REAL,
    Unit TEXT,
    CreatedDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. KPI 数据表 (每日/每周聚合数据)
CREATE TABLE IF NOT EXISTS KPI_Data (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    KPI_Id INTEGER,
    Tag TEXT,             -- 聚合标签 (如: 不合格原因, 产品型号, 部门)
    CreatedDate DATE,     -- 数据日期
    Progress REAL,        -- 实际值 (数值或百分比)
    Details TEXT,         -- 额外详情 (JSON或其他描述)
    FOREIGN KEY (KPI_Id) REFERENCES KPI_Definition(Id)
);

-- 索引: 加速查询
CREATE INDEX IF NOT EXISTS idx_kpi_data_date ON KPI_Data(KPI_Id, CreatedDate);
CREATE INDEX IF NOT EXISTS idx_kpi_data_tag ON KPI_Data(Tag);

-- 3. 触发案例台账 (Trigger Case Registry)
-- 记录生成的 A3/报警案例状态
CREATE TABLE IF NOT EXISTS TriggerCaseRegistry (
    A3Id TEXT PRIMARY KEY,
    Category TEXT,        -- 类别 (对应 KPI_Data.Tag)
    TriggerType TEXT,     -- 触发类型 (如: PERSISTENT_TOP_3, GOAL_MISSED)
    Source TEXT,          -- AUTO / MANUAL
    Status TEXT,          -- OPEN, CLOSED
    OpenedAt TEXT,        -- 开启日期 YYYY-MM-DD
    ClosedAt TEXT,        -- 关闭日期 YYYY-MM-DD
    PlannerTaskId TEXT,   -- 关联的 Planner 任务 ID
    Notes TEXT,           -- 备注
    OriginalLevel TEXT,   -- 触发时的等级
    OriginalDesc TEXT,    -- 触发时的描述
    OriginalDetails TEXT, -- 触发时的详细数据
    OriginalValue TEXT,   -- 触发时的数值
    UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. 触发截断表 (Trigger Case Cutoff)
-- 用于记录关闭案例的时间点，避免历史数据重复触发
CREATE TABLE IF NOT EXISTS TriggerCaseCutoff (
    Tag TEXT,
    TriggerType TEXT,
    ClosedAt DATE,
    PRIMARY KEY (Tag, TriggerType)
);

-- 初始化默认 KPI 定义 (如果不存在)
INSERT OR IGNORE INTO KPI_Definition (Id, Name, Description, TargetValue, Unit) 
VALUES (1, 'Lead_Time', 'Average Manufacturing Lead Time', 24, 'Hours');

INSERT OR IGNORE INTO KPI_Definition (Id, Name, Description, TargetValue, Unit) 
VALUES (2, 'Schedule_Attainment', 'Schedule Attainment Rate', 90, 'Percent');

INSERT OR IGNORE INTO KPI_Definition (Id, Name, Description, TargetValue, Unit) 
VALUES (3, 'Safety_Issue_Rank', 'Safety Issue Label Rank (weekly)', NULL, 'Rank');
