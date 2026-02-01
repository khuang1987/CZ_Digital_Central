import sys
import os
from pathlib import Path
import logging

# Add project root to path
current_file = Path(__file__).resolve()
# Assuming script is in scripts/db/init_ehs.py (root/scripts/db/init_ehs.py)
# Root is parents[2]
project_root = str(current_file.parents[2])
if project_root not in sys.path:
    sys.path.append(project_root)

try:
    from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager
except ImportError:
    # Fallback if specific path structure implies different root
    # Try adding parent directories
    sys.path.append(str(current_file.parents[3])) 
    try:
        from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager
    except ImportError as e:
        print(f"Error importing DB Manager: {e}")
        print(f"Sys Path: {sys.path}")
        sys.exit(1)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("InitEHS")

def init_ehs_db():
    logger.info("Initializing EHS Database Tables...")
    
    # Connection Config (Matching existing ETLs)
    db = SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server"
    )
    
    table_schema = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='safety_green_cross_log' AND xtype='U')
    BEGIN
        CREATE TABLE safety_green_cross_log (
            date DATE PRIMARY KEY,
            status NVARCHAR(50) NOT NULL, -- 'Safe', 'Incident', 'Holiday'?
            updated_at DATETIME DEFAULT GETDATE(),
            incident_details NVARCHAR(MAX) NULL,
            marked_by NVARCHAR(255) NULL
        );
        PRINT 'Created table safety_green_cross_log';
    END
    ELSE
    BEGIN
        PRINT 'Table safety_green_cross_log already exists';
    END
    """
    
    try:
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(table_schema)
            conn.commit()
            logger.info("Schema applied successfully.")
            
            # Insert some dummy data for current month if empty
            check_sql = "SELECT COUNT(*) FROM safety_green_cross_log"
            count = cursor.execute(check_sql).fetchval()
            if count == 0:
                logger.info("Seeding initial data...")
                # Seed current month as Safe
                seed_sql = """
                DECLARE @startDate DATE = FORMAT(GETDATE(), 'yyyy-MM-01');
                DECLARE @i INT = 0;
                WHILE @i < DAY(GETDATE())
                BEGIN
                    DECLARE @d DATE = DATEADD(day, @i, @startDate);
                    IF NOT EXISTS (SELECT 1 FROM safety_green_cross_log WHERE date = @d)
                    BEGIN
                        INSERT INTO safety_green_cross_log (date, status) VALUES (@d, 'Safe');
                    END
                    SET @i = @i + 1;
                END
                """
                cursor.execute(seed_sql)
                conn.commit()
                logger.info("Seeded data.")
                
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise

if __name__ == "__main__":
    init_ehs_db()
