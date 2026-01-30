-- ======================================================
-- 步骤 2: 在服务器上执行 (Server Machine)
-- ======================================================
-- 请在服务器 SSMS 中打开此文件并根据实际路径修改后执行
-- 注意：确保此时 OneDrive 已经同步完了 .bak 文件

USE [master];
GO

-- 1. 还原数据库
-- 请将下面的路径修改为您服务器上实际同步过来的 OneDrive 路径
RESTORE DATABASE [mddap_v2] 
FROM DISK = N'C:\Users\czxmfg\OneDrive - Medtronic PLC\Huangkai Files\C_code\CZ_Digital_Central\backups\mddap_v2.bak' 
WITH FILE = 1, 
MOVE 'mddap_v2' TO 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\DATA\mddap_v2.mdf', 
MOVE 'mddap_v2_log' TO 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\DATA\mddap_v2_log.ldf',
NOUNLOAD, STATS = 5;
GO

-- 2. 检查数据库是否可访问
USE [mddap_v2];
GO
SELECT TOP 10 * FROM [dbo].[KPI_Data];
GO
