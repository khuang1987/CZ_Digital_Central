import { NextRequest, NextResponse } from 'next/server';
import { getDbConnection, sql } from '@/lib/db';

export async function GET(req: NextRequest) {
  try {
    const { searchParams } = new URL(req.url);
    const week = searchParams.get('week') || '40';
    const year = searchParams.get('year') || 'FY26';

    const pool = await getDbConnection();

    // 1. Fetch Changeover Metrics (Summary)
    // We'll count setups and calculate average durations from v_mes_metrics
    const summaryQuery = `
      SELECT 
        COUNT(CASE WHEN IsSetup = 'Yes' THEN 1 END) as setupCount,
        ROUND(AVG(CASE WHEN IsSetup = 'Yes' THEN [PT(d)] * 24 ELSE NULL END), 1) as avgSetupDuration,
        SUM(CASE WHEN IsSetup = 'Yes' AND CompletionStatus = 'OnTime' THEN 1 ELSE 0 END) as onTimeCount
      FROM v_mes_metrics m
      JOIN dim_calendar c ON CAST(m.TrackOutTime AS DATE) = c.date
      WHERE c.fiscal_week = @week AND c.fiscal_year = @year
    `;

    const summaryResult = await pool.request()
      .input('week', sql.Int, parseInt(week))
      .input('year', sql.NVarChar, year)
      .query(summaryQuery);

    // 2. Fetch Trend by Fiscal Week
    const trendQuery = `
      SELECT 
        c.fiscal_week,
        COUNT(CASE WHEN IsSetup = 'Yes' THEN 1 END) as setups
      FROM v_mes_metrics m
      JOIN dim_calendar c ON CAST(m.TrackOutTime AS DATE) = c.date
      WHERE c.fiscal_year = @year
      GROUP BY c.fiscal_week
      ORDER BY c.fiscal_week
    `;

    const trendResult = await pool.request()
      .input('year', sql.NVarChar, year)
      .query(trendQuery);

    // 3. Top CFNs by Duration
    const topCfnQuery = `
      SELECT TOP 5
        CFN,
        ROUND(SUM([PT(d)] * 24), 1) as totalDuration
      FROM v_mes_metrics m
      JOIN dim_calendar c ON CAST(m.TrackOutTime AS DATE) = c.date
      WHERE c.fiscal_week = @week AND c.fiscal_year = @year AND m.IsSetup = 'Yes'
      GROUP BY CFN
      ORDER BY totalDuration DESC
    `;

    const topCfnResult = await pool.request()
      .input('week', sql.Int, parseInt(week))
      .input('year', sql.NVarChar, year)
      .query(topCfnQuery);

    return NextResponse.json({
      summary: summaryResult.recordset[0],
      trend: trendResult.recordset,
      topCfns: topCfnResult.recordset
    });

  } catch (error: any) { // eslint-disable-line @typescript-eslint/no-explicit-any
    console.error('API Error:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
