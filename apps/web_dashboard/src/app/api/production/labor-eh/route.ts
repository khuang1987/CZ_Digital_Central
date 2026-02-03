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

    // FIX: Globally exclude No Area and Outsourced
    areaFilter += ` AND (om.area IS NULL OR om.area NOT IN (N'无区域 NA', N'外协 OS'))`;

    if (processesParam) {
      const processes = processesParam.split(',').map(p => `'${p.replace(/'/g, "''")}'`).join(',');
      areaFilter += ` AND om.display_name IN (${processes})`;
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
            -- FIX: Use display_name for join/grouping if desired, but here we just need sum.
            -- Actually, if we want to filter by PROCESS using display name, we should map valid processes.
            -- But for total sum, we just need the area filter which is applied below.
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

    // 4. Parallel Execution (Conditional based on mode)
    const mode = searchParams.get('mode') || 'dashboard'; // 'dashboard' or 'records'
    const page = parseInt(searchParams.get('page') || '1');
    const pageSize = parseInt(searchParams.get('pageSize') || '50');
    const offset = (page - 1) * pageSize;

    let promises = [];

    if (mode === 'records') {
      // Records Mode: Only fetch details (paginated)
      promises = [
        Promise.resolve(null), // summary
        Promise.resolve(null), // yesterday
        Promise.resolve(null), // weekly
        Promise.resolve(null), // areaDist
        Promise.resolve(null), // filterOptions
        Promise.resolve(null), // trend
        pool.request()
          .input('s', sql.Date, startDay)
          .input('e', sql.Date, endDay)
          .input('plant', sql.NVarChar, plant)
          .input('offset', sql.Int, offset)
          .input('limit', sql.Int, pageSize)
          .query(`
                    SELECT 
                        TRY_CAST(l.PostingDate AS DATE) as PostingDate, 
                        l.OrderNumber, m.BatchNumber, l.Material, l.EarnedLaborTime as actualEH, l.ActualQuantity,
                        l.WorkCenter, l.Plant, l.ProductionScheduler as productLine,
                        l.Operation, l.OperationDesc as rawOpDesc, 
                        om.area, 
                        om.display_name as operationDesc
                    FROM raw_sap_labor_hours l
                    LEFT JOIN (SELECT operation_name, display_name, erp_code, MAX(area) as area FROM dim_operation_mapping WHERE erp_code = @plant GROUP BY operation_name, display_name, erp_code) om 
                        ON l.OperationDesc = om.operation_name AND l.Plant = om.erp_code
                    OUTER APPLY (
                        SELECT TOP 1 BatchNumber 
                        FROM raw_mes m
                        WHERE m.ProductionOrder = l.OrderNumber
                    ) m
                    WHERE TRY_CAST(l.PostingDate AS DATE) BETWEEN @s AND @e AND l.Plant = @plant ${areaFilter}
                    ORDER BY l.PostingDate DESC
                    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY
                `), // details
        Promise.resolve(null), // anomalies
        Promise.resolve(null), // debug
        Promise.resolve(null)  // heatmap
      ];
    } else {
      // Dashboard Mode: Fetch everything EXCEPT details (return empty array for details)
      promises = [
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
                        (SELECT COUNT(*) 
                        FROM dim_production_targets 
                        WHERE TRY_CAST(Date AS DATE) >= @s 
                            AND TRY_CAST(Date AS DATE) <= @e 
                            AND TRY_CAST(Date AS DATE) < CAST(GETDATE() AS DATE) 
                            AND ${targetCol} > 0
                        ) as actualDays,
                        (SELECT ISNULL(SUM(is_workday), 0) FROM dim_production_targets WHERE TRY_CAST(Date AS DATE) BETWEEN @s AND @e) as targetDays,
                        (SELECT ISNULL(SUM(l2.EarnedLaborTime), 0) 
                        FROM raw_sap_labor_hours l2
                        LEFT JOIN (SELECT operation_name, erp_code, MAX(area) as area FROM dim_operation_mapping WHERE erp_code = @plant GROUP BY operation_name, erp_code) om2 
                            ON l2.OperationDesc = om2.operation_name AND l2.Plant = om2.erp_code
                        WHERE TRY_CAST(l2.PostingDate AS DATE) BETWEEN @FYStart AND @e AND l2.Plant = @plant ${areaFilter.replace(/om\./g, 'om2.')} ${schedulerFilter.replace('rslh.', 'l2.')}) as ytdActualEH,
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
                        ISNULL(om.display_name, 'Unknown') as operationName,
                        ISNULL(SUM(rslh.EarnedLaborTime), 0) as yesterdayHours
                        FROM raw_sap_labor_hours rslh
                        LEFT JOIN (SELECT operation_name, display_name, erp_code, MAX(area) as area FROM dim_operation_mapping WHERE erp_code = @plant GROUP BY operation_name, display_name, erp_code) om 
                            ON rslh.OperationDesc = om.operation_name AND rslh.Plant = om.erp_code
                        WHERE TRY_CAST(rslh.PostingDate AS DATE) = DATEADD(day, -1, CAST(GETDATE() AS DATE))
                        AND rslh.Plant = @plant
                        ${areaFilter.replace('om.', 'om.')}
                        ${schedulerFilter}
                        GROUP BY om.area, om.display_name
                    `),

        // C. Weekly
        pool.request()
          .input('s', sql.Date, startDay)
          .input('e', sql.Date, endDay)
          .input('plant', sql.NVarChar, plant)
          .query(`
                        SELECT 
                        ISNULL(om.area, 'Unknown') as area,
                        ISNULL(om.display_name, 'Unknown') as operationName,
                        dc.fiscal_week as fiscalWeek,
                        ISNULL(SUM(rslh.EarnedLaborTime), 0) as weeklyHours
                        FROM raw_sap_labor_hours rslh
                        LEFT JOIN dim_calendar dc ON TRY_CAST(rslh.PostingDate AS DATE) = TRY_CAST(dc.date AS DATE)
                        LEFT JOIN (SELECT operation_name, display_name, erp_code, MAX(area) as area FROM dim_operation_mapping WHERE erp_code = @plant GROUP BY operation_name, display_name, erp_code) om 
                            ON rslh.OperationDesc = om.operation_name AND rslh.Plant = om.erp_code
                        WHERE TRY_CAST(dc.date AS DATE) BETWEEN @s AND @e
                        AND rslh.Plant = @plant
                        ${areaFilter.replace('om.', 'om.')}
                        ${schedulerFilter}
                        GROUP BY om.area, om.display_name, dc.fiscal_week
                        ORDER BY om.area, om.display_name, dc.fiscal_week
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

                        /* Additional logic for cleaned names list if UI needs it */
                        SELECT DISTINCT display_name as operation_name
                        FROM dim_operation_mapping
                        WHERE erp_code = @plant AND display_name IS NOT NULL
                        ${areasParam ? `AND area IN (${areasParam.split(',').map(a => `'${a.replace(/'/g, "''")}'`).join(',')})` : ''}
                        ORDER BY display_name;
                        
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

        // G. Details - SKIPPED IN DASHBOARD MODE
        Promise.resolve({ recordset: [] }),

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
                        LEFT JOIN (SELECT operation_name, display_name, erp_code, MAX(area) as area FROM dim_operation_mapping WHERE erp_code = @plant GROUP BY operation_name, display_name, erp_code) om 
                            ON l.OperationDesc = om.operation_name AND l.Plant = om.erp_code
                        WHERE TRY_CAST(l.PostingDate AS DATE) BETWEEN @s AND @e AND l.Plant = @plant ${areaFilter}
                        GROUP BY l.Material 
                        ORDER BY actualEH DESC
                    `),

        // I. DEBUG: Data Accuracy Check
        pool.request()
          .input('plant', sql.NVarChar, plant)
          .query(`
                        SELECT 
                            '2024-10-26' as CheckDate,
                            (SELECT SUM(EarnedLaborTime) FROM raw_sap_labor_hours WHERE TRY_CAST(PostingDate AS DATE) = '2024-10-26' AND Plant = @plant) as RawSum,
                            0 as JoinedSum, 0 as TotalMappings, 0 as DuplicateMappingsCount
                    `),
        // J. Heatmap Data (Last 20 Days from EndDate, but capped at today)
        pool.request()
          .input('e', sql.Date, endDay)
          .input('plant', sql.NVarChar, plant)
          .query(`
                        -- Use the earlier of endDay or today to avoid querying future dates
                        DECLARE @effectiveEnd DATE = CASE WHEN @e > CAST(GETDATE() AS DATE) THEN CAST(GETDATE() AS DATE) ELSE @e END;
                        
                        SELECT 
                            COALESCE(om.display_name, l.OperationDesc) as op,
                            MAX(om.area) as area,
                            CAST(l.PostingDate AS DATE) as date,
                            SUM(l.EarnedLaborTime) as val
                        FROM raw_sap_labor_hours l
                        LEFT JOIN (SELECT operation_name, display_name, erp_code, MAX(area) as area FROM dim_operation_mapping WHERE erp_code = @plant GROUP BY operation_name, display_name, erp_code) om 
                            ON LTRIM(RTRIM(l.OperationDesc)) = LTRIM(RTRIM(om.operation_name)) AND l.Plant = om.erp_code
                        WHERE l.Plant = @plant
                        AND CAST(l.PostingDate AS DATE) BETWEEN DATEADD(day, -20, @effectiveEnd) AND @effectiveEnd
                        ${areaFilter}
                        ${processesParam ? `AND COALESCE(om.display_name, l.OperationDesc) IN (${processesParam.split(',').map(p => `'${p.replace(/'/g, "''")}'`).join(',')})` : ''}
                        ${productSchedulersParam ? `AND l.ProductionScheduler IN (${productSchedulersParam.split(',').map(s => `'${s.replace(/'/g, "''")}'`).join(',')})` : ''}
                        GROUP BY COALESCE(om.display_name, l.OperationDesc), CAST(l.PostingDate AS DATE)
                        ORDER BY op, date
                    `)
      ];
    }

    const [summaryRes, yesterdayRes, weeklyRes, areaDistRes, filterOptionsRes, trendRes, detailsRes, anomaliesRes, , heatmapRes] = await Promise.all(promises);

    // 5. Post-Processing & Transformations
    // If mode is records, return simplified JSON
    if (mode === 'records') {
      return NextResponse.json({
        details: (detailsRes as any).recordset, // eslint-disable-line @typescript-eslint/no-explicit-any
        page,
        pageSize,
        hasMore: (detailsRes as any).recordset.length === pageSize // eslint-disable-line @typescript-eslint/no-explicit-any
      });
    }

    const summary = (summaryRes as any).recordset[0]; // eslint-disable-line @typescript-eslint/no-explicit-any
    const totalEH = (areaDistRes as any).recordset.reduce((sum: number, r: any) => sum + (r.earnedHours || 0), 0); // eslint-disable-line @typescript-eslint/no-explicit-any

    // Build area distribution structure
    const areaMap = new Map();
    (areaDistRes as any).recordset.forEach((r: any) => { // eslint-disable-line @typescript-eslint/no-explicit-any
      areaMap.set(r.area, {
        area: r.area,
        totalHours: r.earnedHours,
        percentage: totalEH > 0 ? Math.round((r.earnedHours / totalEH) * 100) : 0,
        operations: []
      });
    });

    // Group yesterday and weekly data
    const operationMap = new Map();
    (yesterdayRes as any).recordset.forEach((r: any) => { // eslint-disable-line @typescript-eslint/no-explicit-any
      const key = `${r.area}|${r.operationName}`;
      operationMap.set(key, { area: r.area, operationName: r.operationName, yesterday: r.yesterdayHours, weeklyData: [], totalHours: 0 });
    });

    (weeklyRes as any).recordset.forEach((r: any) => { // eslint-disable-line @typescript-eslint/no-explicit-any
      const key = `${r.area}|${r.operationName}`;
      if (!operationMap.has(key)) {
        operationMap.set(key, { area: r.area, operationName: r.operationName, yesterday: 0, weeklyData: [], totalHours: 0 });
      }
      const item = operationMap.get(key);
      item.weeklyData.push({ fiscalWeek: r.fiscalWeek, hours: r.weeklyHours });
      item.totalHours += r.weeklyHours;
    });

    // Populate operations into areas
    operationMap.forEach(op => {
      if (areaMap.has(op.area)) {
        areaMap.get(op.area).operations.push({
          operationName: op.operationName,
          yesterday: op.yesterday,
          actualEH: op.totalHours, // Include the total EH for this operation
          weeklyData: op.weeklyData
        });
      }
    });

    const areaOperationDetail = Array.from(areaMap.values());
    const availableAreas = (filterOptionsRes as any).recordsets[0].map((r: any) => r.area); // eslint-disable-line @typescript-eslint/no-explicit-any
    const availableOperations = (filterOptionsRes as any).recordsets[2].map((r: any) => r.operation_name); // eslint-disable-line @typescript-eslint/no-explicit-any
    const availableSchedulers = (filterOptionsRes as any).recordsets[3].map((r: any) => r.ProductionScheduler); // eslint-disable-line @typescript-eslint/no-explicit-any

    return NextResponse.json({
      summary: { ...summary, actualAvgEH: summary.actualEH / (summary.actualDays || 1), targetAvgEH: summary.targetEH / (summary.targetDays || 1) },
      trend: (trendRes as any).recordset, // eslint-disable-line @typescript-eslint/no-explicit-any
      anomalies: (anomaliesRes as any).recordset, // eslint-disable-line @typescript-eslint/no-explicit-any
      details: [], // Skip details in dashboard mode
      areaOperationDetail,
      filterOptions: {
        areas: availableAreas,
        operations: availableOperations,
        schedulers: availableSchedulers
      },
      heatmap: (heatmapRes as any).recordset // eslint-disable-line @typescript-eslint/no-explicit-any
    });

  } catch (error: any) { // eslint-disable-line @typescript-eslint/no-explicit-any
    console.error('Labor API Error:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
