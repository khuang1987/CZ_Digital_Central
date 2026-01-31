import { NextRequest, NextResponse } from 'next/server';
import { getDbConnection, sql } from '@/lib/db';

export async function GET(req: NextRequest) {
    try {
        const pool = await getDbConnection();

        // Fetch unique Fiscal Years, Months, and Weeks for the dropdowns
        const query = `
      SELECT 
        fiscal_year,
        fiscal_month,
        fiscal_week,
        fiscal_week_label
      FROM dbo.dim_calendar
      ORDER BY fiscal_year DESC, fiscal_month, fiscal_week
    `;

        const result = await pool.request().query(query);

        // Grouping the data for the frontend
        const years: string[] = [];
        const months: Record<string, string[]> = {};
        const weeks: Record<string, number[]> = {};

        result.recordset.forEach(row => {
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
            weeks
        });

    } catch (error: any) {
        console.error('Calendar API Error:', error);
        return NextResponse.json({ error: error.message }, { status: 500 });
    }
}
