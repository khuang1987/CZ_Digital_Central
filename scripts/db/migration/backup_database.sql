-- MDDAP 2.0 Database Backup Script
-- Run this on the LOCAL machine (source) using SSMS or sqlcmd

USE master;
GO

-- 1. Ensure the backup directory exists
-- Change 'C:\Backup\' to your desired backup path if needed
DECLARE @BackupPath NVARCHAR(255) = 'C:\Backup\mddap_v2.bak';

PRINT 'Starting backup of [mddap_v2] to ' + @BackupPath + '...';

BACKUP DATABASE [mddap_v2] 
TO DISK = @BackupPath
WITH FORMAT, 
     MEDIANAME = 'MDDAP_V2_Backup', 
     NAME = 'Full Backup of mddap_v2',
     STATS = 10;
GO

PRINT 'Backup completed successfully.';
