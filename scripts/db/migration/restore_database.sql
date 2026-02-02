-- MDDAP 2.0 Database Restore Script
-- Run this on the REMOTE server (target) using SSMS or sqlcmd

USE master;
GO

-- 1. Set the backup file path (where you uploaded the .bak file)
DECLARE @BackupPath NVARCHAR(255) = 'C:\Backup\mddap_v2.bak';

-- 2. Get the logical file names from the backup
-- This helps if we need to move files to different directories
RESTORE FILELISTONLY FROM DISK = @BackupPath;
GO

-- 3. Perform the Restore
-- IMPORTANT: Update 'MOVE' paths if your SQL Server data/log directories are different
-- Typically: C:\Program Files\Microsoft SQL Server\MSSQLXX.SQLEXPRESS\MSSQL\DATA\
RESTORE DATABASE [mddap_v2]
FROM DISK = 'C:\Backup\mddap_v2.bak'
WITH REPLACE,
     RECOVERY,
     STATS = 10;
     -- If you get directory errors, uncomment and adjust the following MOVE clauses:
     -- MOVE 'mddap_v2' TO 'C:\Path\To\Data\mddap_v2.mdf',
     -- MOVE 'mddap_v2_log' TO 'C:\Path\To\Logs\mddap_v2_log.ldf';
GO

PRINT 'Database [mddap_v2] restored successfully.';
