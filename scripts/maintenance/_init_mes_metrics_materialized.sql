IF OBJECT_ID('dbo.mes_metrics_snapshot_a', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.mes_metrics_snapshot_a (
        id INT NOT NULL,
        BatchNumber NVARCHAR(MAX) NULL,
        Operation NVARCHAR(MAX) NULL,
        machine NVARCHAR(MAX) NULL,
        [Machine(#)] INT NULL,
        EnterStepTime DATETIME2 NULL,
        TrackInTime DATETIME2 NULL,
        TrackOutTime DATETIME2 NULL,
        TrackOutDate DATE NULL,
        Plant NVARCHAR(MAX) NULL,
        Product_Desc NVARCHAR(MAX) NULL,
        ProductNumber NVARCHAR(MAX) NULL,
        CFN NVARCHAR(MAX) NULL,
        ProductionOrder INT NULL,
        [Operation description] NVARCHAR(MAX) NULL,
        [Group] NVARCHAR(MAX) NULL,
        StepInQuantity FLOAT NULL,
        TrackOutQuantity FLOAT NULL,
        TrackOutOperator NVARCHAR(MAX) NULL,
        [Setup] NVARCHAR(8) NULL,
        [Setup Time (h)] FLOAT NULL,
        OEE FLOAT NULL,
        EH_machine FLOAT NULL,
        EH_labor FLOAT NULL,
        factory_name NVARCHAR(MAX) NULL,
        VSM NVARCHAR(MAX) NULL,
        Checkin_SFC DATETIME2 NULL,
        ScrapQty FLOAT NULL,
        PreviousBatchEndTime DATETIME2 NULL,
        unit_time FLOAT NULL,
        [LT(d)] FLOAT NULL,
        [PT(d)] FLOAT NULL,
        [ST(d)] FLOAT NULL,
        [PNW(d)] FLOAT NULL,
        [LNW(d)] FLOAT NULL,
        CompletionStatus NVARCHAR(32) NULL,
        CONSTRAINT PK_mes_metrics_snapshot_a PRIMARY KEY CLUSTERED (id)
    );
END;
GO

IF OBJECT_ID('dbo.mes_metrics_snapshot_b', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.mes_metrics_snapshot_b (
        id INT NOT NULL,
        BatchNumber NVARCHAR(MAX) NULL,
        Operation NVARCHAR(MAX) NULL,
        machine NVARCHAR(MAX) NULL,
        [Machine(#)] INT NULL,
        EnterStepTime DATETIME2 NULL,
        TrackInTime DATETIME2 NULL,
        TrackOutTime DATETIME2 NULL,
        TrackOutDate DATE NULL,
        Plant NVARCHAR(MAX) NULL,
        Product_Desc NVARCHAR(MAX) NULL,
        ProductNumber NVARCHAR(MAX) NULL,
        CFN NVARCHAR(MAX) NULL,
        ProductionOrder INT NULL,
        [Operation description] NVARCHAR(MAX) NULL,
        [Group] NVARCHAR(MAX) NULL,
        StepInQuantity FLOAT NULL,
        TrackOutQuantity FLOAT NULL,
        TrackOutOperator NVARCHAR(MAX) NULL,
        [Setup] NVARCHAR(8) NULL,
        [Setup Time (h)] FLOAT NULL,
        OEE FLOAT NULL,
        EH_machine FLOAT NULL,
        EH_labor FLOAT NULL,
        factory_name NVARCHAR(MAX) NULL,
        VSM NVARCHAR(MAX) NULL,
        Checkin_SFC DATETIME2 NULL,
        ScrapQty FLOAT NULL,
        PreviousBatchEndTime DATETIME2 NULL,
        unit_time FLOAT NULL,
        [LT(d)] FLOAT NULL,
        [PT(d)] FLOAT NULL,
        [ST(d)] FLOAT NULL,
        [PNW(d)] FLOAT NULL,
        [LNW(d)] FLOAT NULL,
        CompletionStatus NVARCHAR(32) NULL,
        CONSTRAINT PK_mes_metrics_snapshot_b PRIMARY KEY CLUSTERED (id)
    );
END;
GO

IF OBJECT_ID('dbo.mes_metrics_current', 'SN') IS NULL
BEGIN
    CREATE SYNONYM dbo.mes_metrics_current FOR dbo.mes_metrics_snapshot_a;
END;
GO

IF OBJECT_ID('dbo.v_mes_metrics', 'V') IS NOT NULL
    DROP VIEW dbo.v_mes_metrics;
GO

CREATE VIEW dbo.v_mes_metrics AS
SELECT
    id,
    BatchNumber,
    Operation,
    machine,
    [Machine(#)],
    EnterStepTime,
    TrackInTime,
    TrackOutTime,
    TrackOutDate,
    Plant,
    Product_Desc,
    ProductNumber,
    CFN,
    ProductionOrder,
    [Operation description],
    [Group],
    StepInQuantity,
    TrackOutQuantity,
    TrackOutOperator,
    [Setup],
    [Setup Time (h)],
    OEE,
    EH_machine,
    EH_labor,
    factory_name,
    VSM,
    Checkin_SFC,
    ScrapQty,
    PreviousBatchEndTime,
    unit_time,
    [LT(d)],
    [PT(d)],
    [ST(d)],
    [PNW(d)],
    [LNW(d)],
    CompletionStatus
FROM dbo.mes_metrics_current;
GO
