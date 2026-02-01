import { NextRequest, NextResponse } from 'next/server';
import { getDbConnection, sql } from '@/lib/db';

export async function GET(req: NextRequest) {
    try {
        const pool = await getDbConnection();

        // 1. Fetch distinct structure (Year -> Month -> Week)
        // Optimization: Use DISTINCT at DB level to reduce thousands of rows to dozens
        const qStructure = `
            SELECT DISTINCT 
                fiscal_year,
                fiscal_month,
                fiscal_week
            FROM dbo.dim_calendar
            ORDER BY fiscal_year DESC, fiscal_month, fiscal_week
        `;

        // 2. Fetch current info
        const qCurrent = "SELECT TOP 1 fiscal_year, fiscal_month, fiscal_week FROM dim_calendar WHERE date = CAST(GETDATE() AS DATE)";

        // Run in parallel for speed
        const [resStructure, resCurrent] = await Promise.all([
            pool.request().query(qStructure),
            pool.request().query(qCurrent)
        ]);

        // Grouping the data for the frontend
        const years: string[] = [];
        const months: Record<string, string[]> = {};
        const weeks: Record<string, number[]> = {};

        resStructure.recordset.forEach(row => {
            if (!years.includes(row.fiscal_year)) {
                years.push(row.fiscal_year);
            }

            if (!months[row.fiscal_year]) {
                months[row.fiscal_year] = [];
            }
            if (!months[row.fiscal_year].includes(row.fiscal_month)) {
                months[row.fiscal_year].push(row.fiscal_month);
            }

            if (!weeks[row.fiscal_month]) {
                weeks[row.fiscal_month] = [];
            }
            if (!weeks[row.fiscal_month].includes(row.fiscal_week)) {
                weeks[row.fiscal_month].push(row.fiscal_week);
            }
        });

        return NextResponse.json({
            years,
            months,
            weeks,
            currentFiscalInfo: resCurrent.recordset[0] || null
        });

    } catch (error: any) {
        console.error('Calendar API Error:', error);
        return NextResponse.json({ error: error.message }, { status: 500 });
    }
}
