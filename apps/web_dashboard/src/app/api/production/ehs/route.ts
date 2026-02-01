import { NextRequest, NextResponse } from 'next/server';
import { getDbConnection, sql } from '@/lib/db';

export async function GET(req: NextRequest) {
    try {
        const { searchParams } = new URL(req.url);
        const fiscalYear = searchParams.get('fiscalYear'); // "FY26"
        const fiscalMonth = searchParams.get('fiscalMonth'); // "FY26 M10 FEB"
        const area = searchParams.get('area');
        const labels = searchParams.get('labels')?.split(',') || [];
        const progress = searchParams.get('progress')?.split(',') || [];
        const priority = searchParams.get('priority')?.split(',') || [];

        console.log('[EHS API] Params:', { fiscalYear, fiscalMonth, area });

        // DB Connection
        const pool = await getDbConnection();

        // 1. Determine Date Range from Calendar (Fiscal or Standard)
        let startDate: Date, endDate: Date;

        if (fiscalYear && fiscalMonth) {
            // lookup fiscal range
            console.log('[EHS API] Looking up fiscal range for:', fiscalYear, fiscalMonth);
            const calRes = await pool.request()
                .input('fy', sql.NVarChar, fiscalYear)
                .input('fm', sql.NVarChar, fiscalMonth)
                .query(`
                    SELECT MIN(date) as s, MAX(date) as e 
                    FROM dim_calendar 
                    WHERE fiscal_year = @fy AND fiscal_month = @fm
                `);
            console.log('[EHS API] Calendar query result:', calRes.recordset);
            if (calRes.recordset.length > 0 && calRes.recordset[0].s) {
                startDate = new Date(calRes.recordset[0].s);
                endDate = new Date(calRes.recordset[0].e);
                console.log('[EHS API] Using fiscal dates:', { startDate, endDate });
            } else {
                // Fallback
                console.warn('[EHS API] No fiscal dates found, using current month');
                const now = new Date();
                startDate = new Date(now.getFullYear(), now.getMonth(), 1);
                endDate = new Date(now.getFullYear(), now.getMonth() + 1, 0);
            }
        } else {
            // Default: Current Month
            console.log('[EHS API] Using current month (no fiscal params)');
            const now = new Date();
            startDate = new Date(now.getFullYear(), now.getMonth(), 1);
            endDate = new Date(now.getFullYear(), now.getMonth() + 1, 0);
        }

        const startOfYear = new Date(startDate.getFullYear(), 0, 1);
        // Fiscal YTD Start
        const fyStartRes = await pool.request()
            .input('fy', sql.NVarChar, fiscalYear || 'FY24') // fallback
            .query(`SELECT MIN(date) as s FROM dim_calendar WHERE fiscal_year = @fy`);
        const fiscalYtdStart = fyStartRes.recordset[0]?.s || startOfYear;

        // --- Filter Helpers ---
        // Labels: stored in planner_task_labels (CleanedLabel)
        const labelFilter = labels.length > 0 ? `
            AND EXISTS (
                SELECT 1 FROM planner_task_labels l 
                WHERE l.TaskId = t.TaskId 
                AND l.CleanedLabel IN (${labels.map(l => `'${l}'`).join(',')})
            )
        ` : '';

        // Progress
        let progressFilter = '';
        if (progress.length > 0) {
            const conditions = [];
            if (progress.includes('Not Started')) conditions.push("t.PercentComplete = 0");
            if (progress.includes('In Progress')) conditions.push("(t.PercentComplete > 0 AND t.PercentComplete < 100)");
            if (progress.includes('Completed')) conditions.push("t.PercentComplete = 100");
            if (conditions.length > 0) {
                progressFilter = `AND (${conditions.join(' OR ')})`;
            }
        }

        // Priority
        const priorityMap: Record<string, number> = { 'Urgent': 1, 'Important': 3, 'Medium': 5, 'Low': 9 };
        let priorityFilter = '';
        if (priority.length > 0) {
            const pLine = priority.map(p => priorityMap[p]).filter(Boolean).join(',');
            if (pLine) priorityFilter = `AND t.Priority IN (${pLine})`;
        }

        // Helper to bind area optionally
        const withArea = (req: any) => {
            if (area) req.input('area', sql.NVarChar, area);
            return req;
        };

        // Define Queries

        // A. Green Cross Log
        const pA = pool.request()
            .input('s', sql.Date, startDate)
            .input('e', sql.Date, endDate)
            .query(`
                SELECT date, status, incident_details 
                FROM safety_green_cross_log 
                WHERE date BETWEEN @s AND @e
                ORDER BY date
            `);

        // B. YTD Incidents
        const pB = withArea(pool.request()
            .input('ytd', sql.Date, fiscalYtdStart))
            .query(`
                SELECT 
                    COUNT(DISTINCT t.TaskId) as count
                FROM planner_tasks t
                JOIN planner_task_labels l ON t.TaskId = l.TaskId
                WHERE t.CreatedDate >= @ytd
                AND (
                    l.CleanedLabel IN (N'急救事件', N'可记录事故', N'工伤', N'Lost Time Injury')
                    OR l.CleanedLabel LIKE N'%事故%' 
                )
                ${area ? `AND t.TeamName = @area` : ''}
                ${labelFilter} ${progressFilter} ${priorityFilter}
                AND ISNULL(t.IsDeleted, 0) = 0
            `);

        // C. Open Hazards
        const pC = withArea(pool.request()
            .input('s', sql.Date, startDate)
            .input('e', sql.Date, endDate))
            .query(`
                SELECT COUNT(*) as count
                FROM planner_tasks t
                WHERE (BucketName = N'安全' OR BucketName = N'Safe' OR BucketName = N'Safety')
                AND t.CreatedDate BETWEEN @s AND @e 
                ${area ? `AND TeamName = @area` : ''}
                AND (Status != 'Completed' AND Status != 'Closed')
                ${labelFilter} ${progressFilter} ${priorityFilter}
                AND ISNULL(IsDeleted, 0) = 0
            `);

        // D. Safe Days
        const reqD = pool.request();
        if (area) reqD.input('area', sql.NVarChar, area);
        const pD = reqD.query(`
                ${area ? `
                    SELECT 
                        DATEDIFF(day, 
                            ISNULL((
                                SELECT MAX(t.CreatedDate) 
                                FROM planner_tasks t
                                JOIN planner_task_labels l ON t.TaskId = l.TaskId
                                WHERE t.TeamName = @area
                                AND (
                                    l.CleanedLabel IN (N'急救事件', N'可记录事故', N'工伤', N'Lost Time Injury')
                                    OR l.CleanedLabel LIKE N'%事故%' 
                                )
                            ), '2024-01-01'), 
                            GETDATE()
                        ) as safeDays
                ` : `
                    SELECT 
                        DATEDIFF(day, 
                            ISNULL((SELECT MAX(date) FROM safety_green_cross_log WHERE status = 'Incident'), '2024-01-01'), 
                            GETDATE()
                        ) as safeDays
                `}
            `);

        // E. Hazards by Area
        const pE = withArea(pool.request()
            .input('s', sql.Date, startDate)
            .input('e', sql.Date, endDate))
            .query(`
                SELECT 
                    TeamName as area,
                    COUNT(*) as count
                FROM planner_tasks t
                WHERE (BucketName = N'安全' OR BucketName = N'Safe' OR BucketName = N'Safety')
                AND t.CreatedDate BETWEEN @s AND @e
                AND (Status != 'Completed' AND Status != 'Closed')
                ${labelFilter} ${progressFilter} ${priorityFilter}
                ${area ? `AND TeamName = @area` : ''}
                AND ISNULL(IsDeleted, 0) = 0
                GROUP BY TeamName
                ORDER BY count DESC
            `);

        // F. Distinct Teams
        const pF = pool.request().query(`
            SELECT DISTINCT TeamName FROM planner_tasks 
            WHERE TeamName IS NOT NULL AND TeamName != ''
            ORDER BY TeamName
        `);

        // G. YTD Incident Details (List)
        const pG = withArea(pool.request()
            .input('ytd', sql.Date, fiscalYtdStart))
            .query(`
                SELECT 
                    t.TaskName as title, 
                    t.CreatedDate as date, 
                    t.TeamName as area,
                    l.CleanedLabel as classification
                FROM planner_tasks t
                JOIN planner_task_labels l ON t.TaskId = l.TaskId
                WHERE t.CreatedDate >= @ytd
                AND (
                    l.CleanedLabel IN (N'急救事件', N'可记录事故', N'工伤', N'Lost Time Injury')
                    OR l.CleanedLabel LIKE N'%事故%' 
                )
                ${area ? `AND t.TeamName = @area` : ''}
                ${labelFilter} ${progressFilter} ${priorityFilter}
                AND ISNULL(t.IsDeleted, 0) = 0
                ORDER BY t.CreatedDate DESC
            `);

        // H. Hazard Heatmap Data (YTD Monthly Distribution)
        const pH = withArea(pool.request()
            .input('ytd', sql.Date, fiscalYtdStart))
            .query(`
                SELECT 
                    t.TeamName as area,
                    c.fiscal_month as month,
                    COUNT(*) as count
                FROM planner_tasks t
                JOIN dim_calendar c ON CAST(t.CreatedDate AS DATE) = c.date
                WHERE (t.BucketName = N'安全' OR t.BucketName = N'Safe' OR t.BucketName = N'Safety')
                AND t.CreatedDate >= @ytd
                AND (t.Status != 'Completed' AND t.Status != 'Closed')
                ${area ? `AND t.TeamName = @area` : ''}
                AND ISNULL(t.IsDeleted, 0) = 0
                GROUP BY t.TeamName, c.fiscal_month
            `);

        // Execute All Safely
        const results = await Promise.allSettled([pA, pB, pC, pD, pE, pF, pG, pH]);

        // Helpers to extract data or default
        const unwrap = (res: PromiseSettledResult<any>, index: number, label: string, defaultVal: any) => {
            if (res.status === 'fulfilled') return res.value.recordset;
            console.error(`[EHS API] Query [${label}] (index ${index}) Failed:`, res.reason);
            return defaultVal;
        };

        const greenCrossData = unwrap(results[0], 0, 'GreenCross', []);
        const incidentsData = unwrap(results[1], 1, 'YTDIncidents', [{ count: 0 }]);
        const hazardsData = unwrap(results[2], 2, 'OpenHazards', [{ count: 0 }]);
        const safeDaysData = unwrap(results[3], 3, 'SafeDays', [{ safeDays: 0 }]);
        const areaHazardsData = unwrap(results[4], 4, 'HazardsByArea', []);
        const distinctAreasData = unwrap(results[5], 5, 'DistinctTeams', []);
        const incidentsList = unwrap(results[6], 6, 'IncidentsList', []);
        const hazardHeatmapData = unwrap(results[7], 7, 'HazardHeatmap', []);

        // Process Data
        const greenCross = greenCrossData
            .filter((r: any) => r && r.date) // Ensure row and date exist
            .map((r: any) => {
                try {
                    const dateStr = r.date instanceof Date
                        ? r.date.toISOString().split('T')[0]
                        : new Date(r.date).toISOString().split('T')[0];
                    return {
                        date: dateStr,
                        status: r.status,
                        details: r.incident_details
                    };
                } catch (e) {
                    console.error('[EHS API] Date mapping error for row:', r, e);
                    return null;
                }
            })
            .filter(Boolean);

        const stats = {
            incidents: Number(incidentsData[0]?.count || 0),
            hazards: Number(hazardsData[0]?.count || 0),
            safeDays: Number(safeDaysData[0]?.safeDays || 0)
        };

        const filterOptions = {
            areas: (distinctAreasData || []).map((r: any) => r.TeamName).filter((name: string) => name && name.trim() !== '')
        };

        return NextResponse.json({
            greenCross,
            stats,
            areaHazards: areaHazardsData,
            incidentsList,
            hazardHeatmap: hazardHeatmapData,
            filterOptions
        });

    } catch (error: any) {
        console.error('EHS API Major Error:', error);
        return NextResponse.json({ error: error.message }, { status: 500 });
    }
}

export async function POST(req: NextRequest) {
    try {
        const body = await req.json();
        const { date, status, details } = body;

        if (!date || !status) {
            return NextResponse.json({ error: 'Date and status required' }, { status: 400 });
        }

        const pool = await getDbConnection();

        if (status === 'Safe') {
            await pool.request()
                .input('date', sql.Date, new Date(date))
                .query(`DELETE FROM safety_green_cross_log WHERE date = @date`);
        } else {
            await pool.request()
                .input('date', sql.Date, new Date(date))
                .input('status', sql.NVarChar, status)
                .input('details', sql.NVarChar, details || null)
                .query(`
                    MERGE safety_green_cross_log AS target
                    USING (SELECT @date AS date) AS source
                    ON (target.date = source.date)
                    WHEN MATCHED THEN
                        UPDATE SET status = @status, incident_details = @details, updated_at = GETDATE()
                    WHEN NOT MATCHED THEN
                        INSERT (date, status, incident_details)
                        VALUES (@date, @status, @details);
                `);
        }

        return NextResponse.json({ success: true });
    } catch (error: any) {
        console.error('EHS POST Error:', error);
        return NextResponse.json({ error: error.message }, { status: 500 });
    }
}
