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

    // 3. Trend Query Logic (Moved Up)
    let trendQuery;
    if (granularity === 'year') {
      trendQuery = `
        WITH MonthlyLabor AS (
            SELECT 
                dc.fiscal_month,
                SUM(l.EarnedLaborTime) as Actuals
            FROM raw_sap_labor_hours l
            JOIN dim_calendar dc ON TRY_CAST(l.PostingDate AS DATE) = TRY_CAST(dc.date AS DATE)
            LEFT JOIN dim_operation_mapping om ON l.OperationDesc = om.operation_name AND l.Plant = om.erp_code
            WHERE l.Plant = @plant AND dc.fiscal_year = @year ${areaFilter}
            GROUP BY dc.fiscal_month
        ),
        MonthlyTargets AS (
            SELECT 
                dc.fiscal_month,
                SUM(t.${targetCol}) as Targets
            FROM dim_production_targets t
            JOIN dim_calendar dc ON TRY_CAST(t.Date AS DATE) = TRY_CAST(dc.date AS DATE)
            WHERE dc.fiscal_year = @year
            GROUP BY dc.fiscal_month
        )
        SELECT 
            c.fiscal_month as Label, 
            MIN(TRY_CAST(c.date AS DATE)) as SDate, 
            MAX(ISNULL(ml.Actuals, 0)) as actualEH, 
            MAX(ISNULL(mt.Targets, 0)) as targetEH
        FROM dim_calendar c
        LEFT JOIN MonthlyLabor ml ON c.fiscal_month = ml.fiscal_month
        LEFT JOIN MonthlyTargets mt ON c.fiscal_month = mt.fiscal_month
        WHERE c.fiscal_year = @year
        GROUP BY c.fiscal_month 
        ORDER BY SDate
      `;
    } else {
      trendQuery = `
        WITH DailyLabor AS (
            SELECT 
                TRY_CAST(l.PostingDate AS DATE) as PostDate,
                SUM(l.EarnedLaborTime) as Actuals
            FROM raw_sap_labor_hours l
            LEFT JOIN (SELECT operation_name, erp_code, MAX(area) as area FROM dim_operation_mapping WHERE erp_code = @plant GROUP BY operation_name, erp_code) om 
                ON l.OperationDesc = om.operation_name AND l.Plant = om.erp_code
            WHERE l.Plant = @plant 
              AND TRY_CAST(l.PostingDate AS DATE) BETWEEN @s AND @e
              ${areaFilter}
            GROUP BY TRY_CAST(l.PostingDate AS DATE)
        ),
        DailyTargets AS (
            SELECT 
                TRY_CAST(t.Date AS DATE) as TDate,
                MAX(t.${targetCol}) as Targets
            FROM dim_production_targets t
            WHERE TRY_CAST(t.Date AS DATE) BETWEEN @s AND @e
            GROUP BY TRY_CAST(t.Date AS DATE)
        )
        SELECT 
            FORMAT(TRY_CAST(c.date AS DATE), 'MM-dd') as Label, 
            TRY_CAST(c.date AS DATE) as SDate, 
            MAX(ISNULL(dl.Actuals, 0)) as actualEH, 
            MAX(ISNULL(dt.Targets, 0)) as targetEH
        FROM dim_calendar c
        LEFT JOIN DailyLabor dl ON TRY_CAST(c.date AS DATE) = dl.PostDate
        LEFT JOIN DailyTargets dt ON TRY_CAST(c.date AS DATE) = dt.TDate
        WHERE TRY_CAST(c.date AS DATE) BETWEEN @s AND @e 
        GROUP BY c.date 
        ORDER BY SDate
      `;
    }

    // 4. Parallel Execution (All independent Queries)
    const [summaryRes, yesterdayRes, weeklyRes, areaDistRes, filterOptionsRes, trendRes, detailsRes, anomaliesRes] = await Promise.all([
      // A. Summary
      pool.request()
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
                   LEFT JOIN (SELECT operation_name, erp_code, MAX(area) as area FROM dim_operation_mapping WHERE erp_code = @plant GROUP BY operation_name, erp_code) om2 
                      ON l2.OperationDesc = om2.operation_name AND l2.Plant = om2.erp_code
                   WHERE TRY_CAST(l2.PostingDate AS DATE) BETWEEN @FYStart AND @e AND l2.Plant = @plant ${areaFilter.replace('om.', 'om2.')} ${schedulerFilter.replace('rslh.', 'l2.')}) as ytdActualEH,
                  (SELECT ISNULL(SUM(${targetCol}), 0) FROM dim_production_targets WHERE TRY_CAST(Date AS DATE) BETWEEN @FYStart AND @e) as ytdTargetEH
                FROM raw_sap_labor_hours l
                LEFT JOIN (SELECT operation_name, erp_code, MAX(area) as area FROM dim_operation_mapping WHERE erp_code = @plant GROUP BY operation_name, erp_code) om 
                    ON l.OperationDesc = om.operation_name AND l.Plant = om.erp_code
                WHERE TRY_CAST(l.PostingDate AS DATE) BETWEEN @s AND @e AND l.Plant = @plant ${areaFilter} ${schedulerFilter.replace('rslh.', 'l.')}
            `),

      // B. Yesterday
      pool.request()
        .input('plant', sql.NVarChar, plant)
        .query(`
                SELECT 
                  ISNULL(om.area, 'Unknown') as area,
                  ISNULL(om.operation_name, 'Unknown') as operationName,
                  ISNULL(SUM(rslh.EarnedLaborTime), 0) as yesterdayHours
                FROM raw_sap_labor_hours rslh
                LEFT JOIN (SELECT operation_name, erp_code, MAX(area) as area FROM dim_operation_mapping WHERE erp_code = @plant GROUP BY operation_name, erp_code) om 
                    ON rslh.OperationDesc = om.operation_name AND rslh.Plant = om.erp_code
                WHERE TRY_CAST(rslh.PostingDate AS DATE) = DATEADD(day, -1, CAST(GETDATE() AS DATE))
                  AND rslh.Plant = @plant
                  ${areaFilter.replace('om.', 'om.')}
                  ${schedulerFilter}
                GROUP BY om.area, om.operation_name
            `),

      // C. Weekly
      pool.request()
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
                LEFT JOIN (SELECT operation_name, erp_code, MAX(area) as area FROM dim_operation_mapping WHERE erp_code = @plant GROUP BY operation_name, erp_code) om 
                    ON rslh.OperationDesc = om.operation_name AND rslh.Plant = om.erp_code
                WHERE TRY_CAST(dc.date AS DATE) BETWEEN @s AND @e
                  AND rslh.Plant = @plant
                  ${areaFilter.replace('om.', 'om.')}
                  ${schedulerFilter}
                GROUP BY om.area, om.operation_name, dc.fiscal_week
                ORDER BY om.area, om.operation_name, dc.fiscal_week
            `),

      // D. Area Dist
      pool.request()
        .input('s', sql.Date, startDay)
        .input('e', sql.Date, endDay)
        .input('plant', sql.NVarChar, plant)
        .query(`
                SELECT 
                  ISNULL(om.area, 'Unknown') as area,
                  ISNULL(SUM(l.EarnedLaborTime), 0) as earnedHours
                FROM raw_sap_labor_hours l
                LEFT JOIN (SELECT operation_name, erp_code, MAX(area) as area FROM dim_operation_mapping WHERE erp_code = @plant GROUP BY operation_name, erp_code) om 
                    ON l.OperationDesc = om.operation_name AND l.Plant = om.erp_code
                WHERE TRY_CAST(l.PostingDate AS DATE) BETWEEN @s AND @e AND l.Plant = @plant ${areaFilter} ${schedulerFilter.replace('rslh.', 'l.')}
                GROUP BY om.area
                ORDER BY earnedHours DESC
            `),

      // E. Filter Options
      pool.request()
        .input('plant', sql.NVarChar, plant)
        .query(`
                SELECT DISTINCT area
                FROM dim_operation_mapping
                WHERE erp_code = @plant AND area IS NOT NULL
                ORDER BY area;

                SELECT DISTINCT operation_name
                FROM dim_operation_mapping
                WHERE erp_code = @plant AND operation_name IS NOT NULL
                ${areasParam ? `AND area IN (${areasParam.split(',').map(a => `'${a.replace(/'/g, "''")}'`).join(',')})` : ''}
                ORDER BY operation_name;
                
                SELECT DISTINCT ProductionScheduler
                FROM raw_sap_labor_hours
                WHERE Plant = @plant AND ProductionScheduler IS NOT NULL AND ProductionScheduler <> ''
                ORDER BY ProductionScheduler;
            `),

      // F. Trend
      pool.request()
        .input('s', sql.Date, startDay)
        .input('e', sql.Date, endDay)
        .input('plant', sql.NVarChar, plant)
        .input('year', sql.NVarChar, year)
        .query(trendQuery),

      // G. Details
      pool.request()
        .input('s', sql.Date, startDay)
        .input('e', sql.Date, endDay)
        .input('plant', sql.NVarChar, plant)
        .query(`
                SELECT TOP 50 
                  TRY_CAST(l.PostingDate AS DATE) as PostingDate, 
                  l.OrderNumber, l.Material, l.EarnedLaborTime as actualEH, 
                  l.WorkCenter, l.Plant, om.area, om.operation_name as operationDesc
                FROM raw_sap_labor_hours l
                LEFT JOIN (SELECT operation_name, erp_code, MAX(area) as area FROM dim_operation_mapping WHERE erp_code = @plant GROUP BY operation_name, erp_code) om 
                    ON l.OperationDesc = om.operation_name AND l.Plant = om.erp_code
                WHERE TRY_CAST(l.PostingDate AS DATE) BETWEEN @s AND @e AND l.Plant = @plant ${areaFilter}
                ORDER BY l.PostingDate DESC
            `),

      // H. Anomalies
      pool.request()
        .input('s', sql.Date, startDay)
        .input('e', sql.Date, endDay)
        .input('plant', sql.NVarChar, plant)
        .query(`
                SELECT TOP 5 
                  l.Material, 
                  ISNULL(SUM(l.EarnedLaborTime), 0) as actualEH, 
                  COUNT(*) as orderCount
                FROM raw_sap_labor_hours l
                LEFT JOIN (SELECT operation_name, erp_code, MAX(area) as area FROM dim_operation_mapping WHERE erp_code = @plant GROUP BY operation_name, erp_code) om 
                    ON l.OperationDesc = om.operation_name AND l.Plant = om.erp_code
                WHERE TRY_CAST(l.PostingDate AS DATE) BETWEEN @s AND @e AND l.Plant = @plant ${areaFilter}
                GROUP BY l.Material 
                ORDER BY actualEH DESC
            `),

      // I. DEBUG: Data Accuracy Check (Check for duplication on specific date)
      pool.request()
        .input('plant', sql.NVarChar, plant)
        .query(`
                SELECT 
                    '2024-10-26' as CheckDate,
                    (SELECT SUM(EarnedLaborTime) FROM raw_sap_labor_hours WHERE TRY_CAST(PostingDate AS DATE) = '2024-10-26' AND Plant = @plant) as RawSum,
                    (SELECT SUM(l.EarnedLaborTime) 
                     FROM raw_sap_labor_hours l 
                     LEFT JOIN (SELECT operation_name, erp_code, MAX(area) as area FROM dim_operation_mapping WHERE erp_code = @plant GROUP BY operation_name, erp_code) om 
                        ON l.OperationDesc = om.operation_name AND l.Plant = om.erp_code
                     WHERE TRY_CAST(l.PostingDate AS DATE) = '2024-10-26' AND l.Plant = @plant) as JoinedSum,
                     (SELECT COUNT(*) FROM dim_operation_mapping WHERE erp_code = @plant) as TotalMappings,
                     (SELECT COUNT(*) FROM (SELECT operation_name, COUNT(*) as c FROM dim_operation_mapping WHERE erp_code = @plant GROUP BY operation_name HAVING COUNT(*) > 1) as Dups) as DuplicateMappingsCount
            `)
    ]);

    // 5. Post-Processing & Transformations
    const summary = summaryRes.recordset[0];
    const totalEH = areaDistRes.recordset.reduce((sum: number, r: any) => sum + (r.earnedHours || 0), 0);

    // Build area distribution structure
    const areaMap = new Map();
    areaDistRes.recordset.forEach((r: any) => {
      areaMap.set(r.area, {
        area: r.area,
        totalHours: r.earnedHours,
        percentage: totalEH > 0 ? Math.round((r.earnedHours / totalEH) * 100) : 0,
        operations: []
      });
    });

    // Group yesterday and weekly data
    const operationMap = new Map();
    yesterdayRes.recordset.forEach((r: any) => {
      const key = `${r.area}|${r.operationName}`;
      operationMap.set(key, { area: r.area, operationName: r.operationName, yesterday: r.yesterdayHours, weeklyData: [] });
    });

    weeklyRes.recordset.forEach((r: any) => {
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
    const availableAreas = (filterOptionsRes.recordsets as any)[0].map((r: any) => r.area);
    const availableOperations = (filterOptionsRes.recordsets as any)[1].map((r: any) => r.operation_name);
    const availableSchedulers = (filterOptionsRes.recordsets as any)[2].map((r: any) => r.ProductionScheduler);

    return NextResponse.json({
      summary: { ...summary, actualAvgEH: summary.actualEH / (summary.actualDays || 1), targetAvgEH: summary.targetEH / (summary.targetDays || 1) },
      trend: trendRes.recordset,
      anomalies: anomaliesRes.recordset,
      details: detailsRes.recordset,
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
