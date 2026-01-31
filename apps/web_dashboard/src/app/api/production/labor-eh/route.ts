import { NextRequest, NextResponse } from 'next/server';
import { getDbConnection, sql } from '@/lib/db';

export async function GET(req: NextRequest) {
  try {
    const { searchParams } = new URL(req.url);
    const week = searchParams.get('week') || '40';
    const year = searchParams.get('year') || 'FY26';

    const pool = await getDbConnection();

    // 1. Fetch KPI Summary
    // We'll calculate Average Hours, MTD EH, etc.
    const kpiQuery = `
      SELECT 
        AVG(EarnedLaborTime) as avgHours,
        SUM(EarnedLaborTime) as totalEH,
        COUNT(DISTINCT PostingDate) as daysCount
      FROM raw_sap_labor_hours l
      JOIN dim_calendar c ON l.PostingDate = c.date
      WHERE c.fiscal_week = @week AND c.fiscal_year = @year
    `;

    const kpiResult = await pool.request()
      .input('week', sql.Int, parseInt(week))
      .input('year', sql.NVarChar, year)
      .query(kpiQuery);

    // 2. Fetch Daily Trend
    const trendQuery = `
      SELECT 
        PostingDate, 
        SUM(EarnedLaborTime) as dailyEH
      FROM raw_sap_labor_hours l
      JOIN dim_calendar c ON l.PostingDate = c.date
      WHERE c.fiscal_week = @week AND c.fiscal_year = @year
      GROUP BY PostingDate
      ORDER BY PostingDate
    `;

    const trendResult = await pool.request()
      .input('week', sql.Int, parseInt(week))
      .input('year', sql.NVarChar, year)
      .query(trendQuery);

    // 3. Fetch Area Distribution (requires join with operation mapping)
    const areaQuery = `
      SELECT 
        m.area,
        SUM(l.EarnedLaborTime) as areaEH
      FROM raw_sap_labor_hours l
      LEFT JOIN dim_operation_mapping m ON CAST(l.Operation AS NVARCHAR(100)) = CAST(m.operation_name AS NVARCHAR(100))
      JOIN dim_calendar c ON l.PostingDate = c.date
      WHERE c.fiscal_week = @week AND c.fiscal_year = @year
      GROUP BY m.area
      ORDER BY areaEH DESC
    `;

    const areaResult = await pool.request()
      .input('week', sql.Int, parseInt(week))
      .input('year', sql.NVarChar, year)
      .query(areaQuery);

    // 4. Fetch Detailed Order Logs (Top 10)
    const detailsQuery = `
      SELECT TOP 10
        PostingDate,
        OrderNumber,
        EarnedLaborTime as eh,
        Operation,
        Material
      FROM raw_sap_labor_hours l
      JOIN dim_calendar c ON l.PostingDate = c.date
      WHERE c.fiscal_week = @week AND c.fiscal_year = @year
      ORDER BY PostingDate DESC, EarnedLaborTime DESC
    `;

    const detailsResult = await pool.request()
      .input('week', sql.Int, parseInt(week))
      .input('year', sql.NVarChar, year)
      .query(detailsQuery);

    return NextResponse.json({
      summary: kpiResult.recordset[0],
      trend: trendResult.recordset,
      distribution: areaResult.recordset,
      details: detailsResult.recordset
    });

  } catch (error: any) {
    console.error('API Error:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
