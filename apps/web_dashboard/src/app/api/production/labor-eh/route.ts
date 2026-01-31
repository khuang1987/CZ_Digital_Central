import { NextRequest, NextResponse } from 'next/server';
import { getDbConnection, sql } from '@/lib/db';

export async function GET(req: NextRequest) {
  try {
    const { searchParams } = new URL(req.url);
    const granularity = searchParams.get('granularity') || 'month'; // Default to month per user request
    const year = searchParams.get('year') || 'FY26';
    const month = searchParams.get('month');
    const weeksParam = searchParams.get('week'); // Can be "40,41"
    const plant = searchParams.get('plant') || '9997';
    const startDateParam = searchParams.get('startDate');
    const endDateParam = searchParams.get('endDate');
    const areasParam = searchParams.get('areas'); // Comma-separated area filter
    const processesParam = searchParams.get('processes'); // Comma-separated operationDesc filter
    const productSchedulersParam = searchParams.get('productSchedulers'); // Comma-separated ProductionScheduler filter

    const pool = await getDbConnection();

    // 1. Determine Date Range based on granularity
    let startDay, endDay;

    if (granularity === 'week') {
      const weeks = (weeksParam || '40').split(',').map(w => parseInt(w));
      const res = await pool.request()
        .input('year', sql.NVarChar, year)
        .query(`SELECT MIN(TRY_CAST(date AS DATE)) as s, MAX(TRY_CAST(date AS DATE)) as e FROM dim_calendar WHERE fiscal_year = @year AND fiscal_week IN (${weeks.join(',')})`);
      ({ s: startDay, e: endDay } = res.recordset[0]);
    } else if (granularity === 'month') {
      const res = await pool.request()
        .input('month', sql.NVarChar, month || 'Apr')
        .input('year', sql.NVarChar, year)
        .query(`SELECT MIN(TRY_CAST(date AS DATE)) as s, MAX(TRY_CAST(date AS DATE)) as e FROM dim_calendar WHERE fiscal_month = @month AND fiscal_year = @year`);
      ({ s: startDay, e: endDay } = res.recordset[0]);
    } else if (granularity === 'year') {
      const res = await pool.request()
        .input('year', sql.NVarChar, year)
        .query(`SELECT MIN(TRY_CAST(date AS DATE)) as s, MAX(TRY_CAST(date AS DATE)) as e FROM dim_calendar WHERE fiscal_year = @year`);
      ({ s: startDay, e: endDay } = res.recordset[0]);
    } else { // granularity === 'custom'
      startDay = startDateParam ? new Date(startDateParam) : new Date();
      endDay = endDateParam ? new Date(endDateParam) : new Date();
    }

    const targetCol = plant === '1303' ? 'target_eh_1303' : 'target_eh_9997';

    // Build filter conditions for area/process
    let areaFilter = '';
    if (areasParam) {
      const areas = areasParam.split(',').map(a => `'${a.replace(/'/g, "''")}'`).join(',');
      areaFilter += ` AND om.area IN (${areas})`;
    }
    if (processesParam) {
      const processes = processesParam.split(',').map(p => `'${p.replace(/'/g, "''")}'`).join(',');
      areaFilter += ` AND om.operation_name IN (${processes})`;
    }

    let schedulerFilter = '';
    if (productSchedulersParam) {
      const schedulers = productSchedulersParam.split(',').map(s => `'${s.replace(/'/g, "''")}'`).join(',');
      schedulerFilter = ` AND rslh.ProductionScheduler IN (${schedulers})`;
    }

    // 2. Summary & YTD (Unified Query)
    const summaryRes = await pool.request()
      .input('s', sql.Date, startDay)
      .input('e', sql.Date, endDay)
      .input('plant', sql.NVarChar, plant)
      .input('year', sql.NVarChar, year)
      .query(`
        DECLARE @FYStart DATE;
        SELECT @FYStart = MIN(TRY_CAST(date AS DATE)) FROM dim_calendar WHERE fiscal_year = @year;

        SELECT 
          ISNULL(SUM(l.EarnedLaborTime), 0) as actualEH,
          (SELECT ISNULL(SUM(${targetCol}), 0) FROM dim_production_targets WHERE TRY_CAST(Date AS DATE) BETWEEN @s AND @e) as targetEH,
          COUNT(DISTINCT TRY_CAST(l.PostingDate AS DATE)) as actualDays,
          (SELECT ISNULL(SUM(is_workday), 0) FROM dim_production_targets WHERE TRY_CAST(Date AS DATE) BETWEEN @s AND @e) as targetDays,
          (SELECT ISNULL(SUM(l2.EarnedLaborTime), 0) 
           FROM raw_sap_labor_hours l2
           LEFT JOIN dim_operation_mapping om2 ON l2.OperationDesc = om2.operation_name AND l2.Plant = om2.erp_code
           WHERE TRY_CAST(l2.PostingDate AS DATE) BETWEEN @FYStart AND @e AND l2.Plant = @plant ${areaFilter.replace('om.', 'om2.')} ${schedulerFilter.replace('rslh.', 'l2.')}) as ytdActualEH,
          (SELECT ISNULL(SUM(${targetCol}), 0) FROM dim_production_targets WHERE TRY_CAST(Date AS DATE) BETWEEN @FYStart AND @e) as ytdTargetEH
        FROM raw_sap_labor_hours l
        LEFT JOIN dim_operation_mapping om ON l.OperationDesc = om.operation_name AND l.Plant = om.erp_code
        WHERE TRY_CAST(l.PostingDate AS DATE) BETWEEN @s AND @e AND l.Plant = @plant ${areaFilter} ${schedulerFilter.replace('rslh.', 'l.')}
      `);
    const summary = summaryRes.recordset[0];

    // 6. Detailed Area/Operation Distribution with Yesterday & Weekly Data
    // 6a. Yesterday'shelement hours (current date - 1)
    const yesterdayRes = await pool.request()
      .input('plant', sql.NVarChar, plant)
      .query(`
        SELECT 
          ISNULL(om.area, 'Unknown') as area,
          ISNULL(om.operation_name, 'Unknown') as operationName,
          ISNULL(SUM(rslh.EarnedLaborTime), 0) as yesterdayHours
        FROM raw_sap_labor_hours rslh
        LEFT JOIN dim_operation_mapping om ON rslh.OperationDesc = om.operation_name AND rslh.Plant = om.erp_code
        WHERE TRY_CAST(rslh.PostingDate AS DATE) = DATEADD(day, -1, CAST(GETDATE() AS DATE))
          AND rslh.Plant = @plant
          ${areaFilter.replace('om.', 'om.')}
          ${schedulerFilter}
        GROUP BY om.area, om.operation_name
      `);

    // 6b. Weekly hours by area/operation (within selected date range)
    const weeklyRes = await pool.request()
      .input('s', sql.Date, startDay)
      .input('e', sql.Date, endDay)
      .input('plant', sql.NVarChar, plant)
      .query(`
        SELECT 
          ISNULL(om.area, 'Unknown') as area,
          ISNULL(om.operation_name, 'Unknown') as operationName,
          dc.fiscal_week as fiscalWeek,
          ISNULL(SUM(rslh.EarnedLaborTime), 0) as weeklyHours
        FROM raw_sap_labor_hours rslh
        LEFT JOIN dim_calendar dc ON TRY_CAST(rslh.PostingDate AS DATE) = TRY_CAST(dc.date AS DATE)
        LEFT JOIN dim_operation_mapping om ON rslh.OperationDesc = om.operation_name AND rslh.Plant = om.erp_code
        WHERE TRY_CAST(dc.date AS DATE) BETWEEN @s AND @e
          AND rslh.Plant = @plant
          ${areaFilter.replace('om.', 'om.')}
          ${schedulerFilter}
        GROUP BY om.area, om.operation_name, dc.fiscal_week
        ORDER BY om.area, om.operation_name, dc.fiscal_week
      `);

    // 6c. Area percentages (total earned hours by area for percentage calculation)
    const areaDistRes = await pool.request()
      .input('s', sql.Date, startDay)
      .input('e', sql.Date, endDay)
      .input('plant', sql.NVarChar, plant)
      .query(`
        SELECT 
          ISNULL(om.area, 'Unknown') as area,
          ISNULL(SUM(l.EarnedLaborTime), 0) as earnedHours
        FROM raw_sap_labor_hours l
        LEFT JOIN dim_operation_mapping om ON l.OperationDesc = om.operation_name AND l.Plant = om.erp_code
        WHERE TRY_CAST(l.PostingDate AS DATE) BETWEEN @s AND @e AND l.Plant = @plant ${areaFilter} ${schedulerFilter.replace('rslh.', 'l.')}
        GROUP BY om.area
        ORDER BY earnedHours DESC
      `);

    const totalEH = areaDistRes.recordset.reduce((sum, r) => sum + (r.earnedHours || 0), 0);

    // Build area distribution structure
    const areaMap = new Map();
    areaDistRes.recordset.forEach(r => {
      areaMap.set(r.area, {
        area: r.area,
        totalHours: r.earnedHours, // Add total earned hours for the selected range
        percentage: totalEH > 0 ? Math.round((r.earnedHours / totalEH) * 100) : 0,
        operations: []
      });
    });

    // Group yesterday and weekly data by area/operation
    const operationMap = new Map();
    yesterdayRes.recordset.forEach(r => {
      const key = `${r.area}|${r.operationName}`;
      operationMap.set(key, { area: r.area, operationName: r.operationName, yesterday: r.yesterdayHours, weeklyData: [] });
    });

    weeklyRes.recordset.forEach(r => {
      const key = `${r.area}|${r.operationName}`;
      if (!operationMap.has(key)) {
        operationMap.set(key, { area: r.area, operationName: r.operationName, yesterday: 0, weeklyData: [] });
      }
      operationMap.get(key).weeklyData.push({ fiscalWeek: r.fiscalWeek, hours: r.weeklyHours });
    });

    // Populate operations into areas
    operationMap.forEach(op => {
      if (areaMap.has(op.area)) {
        areaMap.get(op.area).operations.push({
          operationName: op.operationName,
          yesterday: op.yesterday,
          weeklyData: op.weeklyData
        });
      }
    });

    const areaOperationDetail = Array.from(areaMap.values());

    // Fetch available filter options (areas and operation names)
    // Operation names are filtered by selected areas (cascading filter)
    let operationQuery = `
      SELECT DISTINCT operation_name
      FROM dim_operation_mapping
      WHERE erp_code = @plant AND operation_name IS NOT NULL
    `;

    // If areas are selected, only show operations for those areas
    if (areasParam) {
      const areas = areasParam.split(',').map(a => `'${a.replace(/'/g, "''")}'`).join(',');
      operationQuery += ` AND area IN (${areas})`;
    }

    operationQuery += ` ORDER BY operation_name;`;

    const filterOptionsRes = await pool.request()
      .input('plant', sql.NVarChar, plant)
      .query(`
        SELECT DISTINCT area
        FROM dim_operation_mapping
        WHERE erp_code = @plant AND area IS NOT NULL
        ORDER BY area;

        ${operationQuery}
        
        SELECT DISTINCT ProductionScheduler
        FROM raw_sap_labor_hours
        WHERE Plant = @plant AND ProductionScheduler IS NOT NULL AND ProductionScheduler <> ''
        ORDER BY ProductionScheduler;
      `);

    const availableAreas = (filterOptionsRes.recordsets as any)[0].map((r: any) => r.area);
    const availableOperations = (filterOptionsRes.recordsets as any)[1].map((r: any) => r.operation_name);
    const availableSchedulers = (filterOptionsRes.recordsets as any)[2].map((r: any) => r.ProductionScheduler);

    // 3. Trend Data
    let trendQuery;
    if (granularity === 'year') {
      trendQuery = `
        SELECT c.fiscal_month as Label, MIN(TRY_CAST(c.date AS DATE)) as SDate, ISNULL(SUM(l.EarnedLaborTime), 0) as actualEH, ISNULL(SUM(t.${targetCol}), 0) as targetEH
        FROM dim_calendar c
        LEFT JOIN raw_sap_labor_hours l ON TRY_CAST(c.date AS DATE) = TRY_CAST(l.PostingDate AS DATE) AND l.Plant = @plant
        LEFT JOIN dim_production_targets t ON TRY_CAST(c.date AS DATE) = TRY_CAST(t.Date AS DATE)
        WHERE TRY_CAST(c.date AS DATE) BETWEEN @s AND @e
        GROUP BY c.fiscal_month ORDER BY SDate
      `;
    } else {
      trendQuery = `
        SELECT FORMAT(TRY_CAST(c.date AS DATE), 'MM-dd') as Label, TRY_CAST(c.date AS DATE) as SDate, ISNULL(SUM(l.EarnedLaborTime), 0) as actualEH, ISNULL(MAX(t.${targetCol}), 0) as targetEH
        FROM dim_calendar c
        LEFT JOIN raw_sap_labor_hours l ON TRY_CAST(c.date AS DATE) = TRY_CAST(l.PostingDate AS DATE) AND l.Plant = @plant
        LEFT JOIN dim_operation_mapping om ON l.OperationDesc = om.operation_name AND l.Plant = om.erp_code
        LEFT JOIN dim_production_targets t ON TRY_CAST(c.date AS DATE) = TRY_CAST(t.Date AS DATE)
        WHERE TRY_CAST(c.date AS DATE) BETWEEN @s AND @e ${areaFilter}
        GROUP BY c.date ORDER BY SDate
      `;
    }
    const trend = await pool.request().input('s', sql.Date, startDay).input('e', sql.Date, endDay).input('plant', sql.NVarChar, plant).query(trendQuery);

    // 4. Details (Top 50)
    const details = await pool.request().input('s', sql.Date, startDay).input('e', sql.Date, endDay).input('plant', sql.NVarChar, plant)
      .query(`
        SELECT TOP 50 
          TRY_CAST(l.PostingDate AS DATE) as PostingDate, 
          l.OrderNumber, l.Material, l.EarnedLaborTime as actualEH, 
          l.WorkCenter, l.Plant, om.area, om.operation_name as operationDesc
        FROM raw_sap_labor_hours l
        LEFT JOIN dim_operation_mapping om ON l.OperationDesc = om.operation_name AND l.Plant = om.erp_code
        WHERE TRY_CAST(l.PostingDate AS DATE) BETWEEN @s AND @e AND l.Plant = @plant ${areaFilter}
        ORDER BY l.PostingDate DESC
      `);

    // 5. Anomalies
    const anomalies = await pool.request().input('s', sql.Date, startDay).input('e', sql.Date, endDay).input('plant', sql.NVarChar, plant)
      .query(`
        SELECT TOP 5 
          l.Material, 
          ISNULL(SUM(l.EarnedLaborTime), 0) as actualEH, 
          COUNT(*) as orderCount
        FROM raw_sap_labor_hours l
        LEFT JOIN dim_operation_mapping om ON l.OperationDesc = om.operation_name AND l.Plant = om.erp_code
        WHERE TRY_CAST(l.PostingDate AS DATE) BETWEEN @s AND @e AND l.Plant = @plant ${areaFilter}
        GROUP BY l.Material 
        ORDER BY actualEH DESC
      `);

    return NextResponse.json({
      summary: { ...summary, actualAvgEH: summary.actualEH / (summary.actualDays || 1), targetAvgEH: summary.targetEH / (summary.targetDays || 1) },
      trend: trend.recordset,
      anomalies: anomalies.recordset,
      details: details.recordset,
      areaOperationDetail,
      filterOptions: {
        areas: availableAreas,
        operations: availableOperations,
        schedulers: availableSchedulers
      }
    });

  } catch (error: any) {
    console.error('Labor API Error:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
