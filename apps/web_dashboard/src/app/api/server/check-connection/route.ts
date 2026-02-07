import { NextResponse } from 'next/server';
import { getDbConnection } from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET() {
    try {
        const pool = await getDbConnection();
        const server = process.env.DB_SERVER || process.env.MDDAP_SQL_SERVER || 'localhost';
        const database = process.env.DB_NAME || process.env.MDDAP_SQL_DATABASE || 'mddap_v2';

        return NextResponse.json({
            status: 'online',
            server,
            database,
            timestamp: new Date().toISOString()
        });
    } catch (error: any) {
        return NextResponse.json({
            status: 'offline',
            error: error.message || 'Unknown connection error',
            server: process.env.DB_SERVER || process.env.MDDAP_SQL_SERVER || 'localhost',
            database: process.env.DB_NAME || process.env.MDDAP_SQL_DATABASE || 'mddap_v2',
            timestamp: new Date().toISOString()
        }, { status: 500 });
    }
}
