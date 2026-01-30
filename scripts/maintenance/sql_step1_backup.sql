-- ======================================================
-- 步骤 1: 在本地开发机执行 (Local Dev Machine)
-- ======================================================
-- 请在本地 SSMS 中新建查询，并执行以下命令
-- 注意：确保 C:\Temp 文件夹存在，或者修改为您想要保存备份的路径

USE [master];
GO

-- 1. 完整备份数据库
BACKUP DATABASE [mddap_v2] 
TO DISK = N'C:\Temp\mddap_v2.bak' 
WITH FORMAT, INIT, NAME = 'mddap_v2-Full Backup (Production Migration)', SKIP, NOREWIND, NOUNLOAD, STATS = 10;
GO

-- 备份完成后，请将 C:\Temp\mddap_v2.bak 移动到您的 OneDrive:
-- "Huangkai Files\C_code\CZ_Digital_Central\backups\" 目录下
