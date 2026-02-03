
import { NextResponse } from 'next/server';
import { getDbConnection, sql } from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET() {
    try {
        const pool = await getDbConnection();
        
        // 1. Summary Metrics
        const summaryResult = await pool.request().query(`
            SELECT 
                (SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_TYPE = 'BASE TABLE') as total_tables,
                (SELECT SUM(row_count) FROM sys.dm_db_partition_stats WHERE index_id IN (0,1)) as total_rows,
                (SELECT SUM(size * 8) / 1024 FROM sys.database_files WHERE type_desc = 'ROWS') as data_size_mb,
                (SELECT SUM(size * 8) / 1024 FROM sys.database_files WHERE type_desc = 'LOG') as log_size_mb
        `);
        
        const summary = summaryResult.recordset[0];

        // 2. Top Tables by Size/Rows
        const tablesResult = await pool.request().query(`
            SELECT TOP 20
                t.name AS table_name,
                s.name AS schema_name,
                p.rows AS row_count,
                CAST(ROUND(((SUM(a.total_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2)) AS total_space_mb,
                CAST(ROUND(((SUM(a.used_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2)) AS used_space_mb,
                (SELECT TOP 1 last_user_update FROM sys.dm_db_index_usage_stats WHERE object_id = t.object_id) as last_access
            FROM 
                sys.tables t
            INNER JOIN      
                sys.indexes i ON t.OBJECT_ID = i.object_id
            INNER JOIN 
                sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
            INNER JOIN 
                sys.allocation_units a ON p.partition_id = a.container_id
            LEFT JOIN 
                sys.schemas s ON t.schema_id = s.schema_id
            WHERE 
                t.is_ms_shipped = 0
                AND i.OBJECT_ID > 255 
            GROUP BY 
                t.name, s.name, p.rows, t.object_id
            ORDER BY 
                total_space_mb DESC, p.rows DESC
        `);

        return NextResponse.json({
            summary: {
                total_tables: summary.total_tables,
                total_rows: summary.total_rows,
                db_size_mb: Math.round(summary.data_size_mb + summary.log_size_mb)
            },
            tables: tablesResult.recordset
        });

    } catch (error) {
        console.error('Database Error:', error);
        return NextResponse.json(
            { error: 'Failed to fetch server stats' },
            { status: 500 }
        );
    }
}
