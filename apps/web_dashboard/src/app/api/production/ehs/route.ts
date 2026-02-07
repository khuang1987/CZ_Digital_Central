
import { NextRequest, NextResponse } from 'next/server';
import { getDbConnection, sql } from '@/lib/db';

export async function GET(req: NextRequest) {
    try {
        const { searchParams } = new URL(req.url);
        const fiscalYear = searchParams.get('fiscalYear'); // e.g. "FY25"
        const areas = searchParams.get('areas'); // Comma-separated list
        const paramStart = searchParams.get('startDate');
        const paramEnd = searchParams.get('endDate');

        console.log('[EHS API] Fetching Data params:', { fiscalYear, areas, paramStart, paramEnd });
        const pool = await getDbConnection();

        // 1. Determine Date Range (Range Slicer)
        const now = new Date();
        let startDate = new Date(now.getFullYear(), now.getMonth(), 1);
        let endDate = new Date(now.getFullYear(), now.getMonth() + 1, 0);

        if (paramStart && paramEnd) {
            startDate = new Date(paramStart);
            endDate = new Date(paramEnd);
        } else if (fiscalYear) {
            const fyRangeRes = await pool.request()
                .input('fy', sql.NVarChar, fiscalYear)
                .query(`SELECT MIN(date) as s, MAX(date) as e FROM dim_calendar WHERE fiscal_year = @fy`);

            if (fyRangeRes.recordset[0]?.s) {
                // If specific fiscalMonth is provided within fiscalYear mode
                const fMonth = searchParams.get('fiscalMonth');
                if (fMonth) {
                    const fmRangeRes = await pool.request()
                        .input('fy', sql.NVarChar, fiscalYear)
                        .input('fm', sql.NVarChar, fMonth)
                        .query(`SELECT MIN(date) as s, MAX(date) as e FROM dim_calendar WHERE fiscal_year = @fy AND fiscal_month = @fm`);
                    if (fmRangeRes.recordset[0]?.s) {
                        startDate = new Date(fmRangeRes.recordset[0].s);
                        endDate = new Date(fmRangeRes.recordset[0].e);
                    }
                } else {
                    startDate = new Date(fyRangeRes.recordset[0].s);
                    endDate = new Date(fyRangeRes.recordset[0].e);
                }
            }
        }

        // 2. Determine YTD Range (Based on fiscalYear or current date)
        let ytdStart = new Date(now.getFullYear(), 0, 1);
        let ytdEnd = new Date(now.getFullYear(), 11, 31);

        const effectiveFY = fiscalYear || (await pool.request().query(`SELECT TOP 1 fiscal_year FROM dim_calendar WHERE date = CAST(GETDATE() AS DATE)`)).recordset[0]?.fiscal_year;

        if (effectiveFY) {
            const fyFullRange = await pool.request()
                .input('fy', sql.NVarChar, effectiveFY)
                .query(`SELECT MIN(date) as s, MAX(date) as e FROM dim_calendar WHERE fiscal_year = @fy`);
            if (fyFullRange.recordset[0]?.s) {
                ytdStart = new Date(fyFullRange.recordset[0].s);
                ytdEnd = new Date(fyFullRange.recordset[0].e);
            }
        }

        console.log('[EHS API] Ranges:', { startDate, endDate, ytdStart, ytdEnd });

        // Helper for multi-area filtering logic
        const areaFilter = areas ? `AND t.TeamName IN (SELECT value FROM STRING_SPLIT(@areas, ','))` : '';

        // ==========================================
        // QUERY G: INCIDENT LIST (YTD)
        // ==========================================
        const incidentsQuery = `
            SELECT 
                t.TaskId,
                t.TaskName as title, 
                t.Description as description,
                t.Status as status,
                t.CreatedDate as date, 
                t.TeamName as area,
                t.Labels as classification,
                CASE WHEN t.Status = 'Completed' THEN 100 ELSE 0 END as progress
            FROM planner_tasks t
            WHERE t.Labels LIKE @keyword
            AND t.CreatedDate BETWEEN @ytdStart AND @ytdEnd 
            ${areaFilter}
            ORDER BY t.CreatedDate DESC
        `;

        // ==========================================
        // QUERY H: HAZARD HEATMAP (YTD)
        // ==========================================
        const heatmapQuery = `
             SELECT 
                t.TeamName as area,
                c.fiscal_month as month,
                COUNT(*) as count
            FROM planner_tasks t
            JOIN dim_calendar c ON CAST(t.CreatedDate AS DATE) = c.date
            WHERE (t.BucketName = N'安全' OR t.BucketName = N'Safe' OR t.BucketName = N'Safety')
            AND t.CreatedDate BETWEEN @ytdStart AND @ytdEnd
            AND (t.Status != 'Completed' AND t.Status != 'Closed')
            ${areaFilter}
            AND ISNULL(t.IsDeleted, 0) = 0
            GROUP BY t.TeamName, c.fiscal_month
        `;

        // ==========================================
        // QUERY I: HAZARD TASKS LIST (Range Slicer Responsive)
        // ==========================================
        const hazardTasksQuery = `
            SELECT 
                t.TaskId, 
                t.TaskName as title, 
                t.Description as description,
                t.CreatedDate as date, 
                t.Status as status, 
                t.Labels as classification, 
                t.TeamName as area,
                t.CreatedBy as creator,
                t.Assignees as assignees,
                t.Priority as priority
            FROM planner_tasks t
            WHERE (t.BucketName = N'安全' OR t.BucketName = N'Safe' OR t.BucketName = N'Safety')
            AND t.CreatedDate BETWEEN @start AND @end 
            ${areaFilter}
            AND ISNULL(t.IsDeleted, 0) = 0
            ORDER BY t.CreatedDate DESC
        `;

        // ==========================================
        // QUERY B: INCIDENT COUNT (YTD)
        // ==========================================
        const incidentCountQuery = `
            SELECT COUNT(DISTINCT t.TaskId) as count
            FROM planner_tasks t
            WHERE t.CreatedDate BETWEEN @ytdStart AND @ytdEnd
            AND t.Labels LIKE @keyword
            ${areaFilter}
            AND ISNULL(t.IsDeleted, 0) = 0
        `;

        // ==========================================
        // QUERY C: OPEN HAZARDS COUNT (Global/Filtered)
        // ==========================================
        const hazardCountQuery = `
            SELECT COUNT(*) as count
            FROM planner_tasks t
            WHERE (BucketName = N'安全' OR BucketName = N'Safe' OR BucketName = N'Safety')
            -- Open hazards are usually viewed globally, but here we scope to YTD for consistency
            AND t.CreatedDate BETWEEN @ytdStart AND @ytdEnd
            ${areaFilter}
            AND t.Status NOT IN ('Completed', 'Closed')
            AND ISNULL(IsDeleted, 0) = 0
        `;

        const areasQuery = `SELECT DISTINCT TeamName as area FROM planner_tasks WHERE TeamName IS NOT NULL AND TeamName <> '' ORDER BY TeamName ASC`;

        // ==========================================
        // QUERY K: GREEN CROSS DATA (Range Responder)
        // ==========================================
        const greenCrossQuery = `
            SELECT 
                CAST(date AS NVARCHAR) as date,
                status,
                incident_details as details
            FROM safety_green_cross_log
            WHERE date BETWEEN @start AND @end
        `;

        // EXECUTE QUERIES PARALLEL
        const pIncidents = pool.request()
            .input('keyword', sql.NVarChar, '%急救%')
            .input('ytdStart', sql.Date, ytdStart)
            .input('ytdEnd', sql.Date, ytdEnd)
            .input('areas', sql.NVarChar, areas || '')
            .query(incidentsQuery);

        const pHeatmap = pool.request()
            .input('ytdStart', sql.Date, ytdStart)
            .input('ytdEnd', sql.Date, ytdEnd)
            .input('areas', sql.NVarChar, areas || '')
            .query(heatmapQuery);

        const pHazardTasks = pool.request()
            .input('ytdStart', sql.Date, ytdStart)
            .input('ytdEnd', sql.Date, ytdEnd)
            .input('areas', sql.NVarChar, areas || '')
            .query(hazardTasksQuery.replace('@start AND @end', '@ytdStart AND @ytdEnd'));

        const pCountIncidents = pool.request()
            .input('keyword', sql.NVarChar, '%急救%')
            .input('ytdStart', sql.Date, ytdStart)
            .input('ytdEnd', sql.Date, ytdEnd)
            .input('areas', sql.NVarChar, areas || '')
            .query(incidentCountQuery);

        const pCountHazards = pool.request()
            .input('ytdStart', sql.Date, ytdStart)
            .input('ytdEnd', sql.Date, ytdEnd)
            .input('areas', sql.NVarChar, areas || '')
            .query(hazardCountQuery);

        const pAreas = pool.request().query(areasQuery);
        const pGreenCross = pool.request()
            .input('start', sql.Date, startDate)
            .input('end', sql.Date, endDate)
            .query(greenCrossQuery);

        // Safe days uses specific logic (since last incident)
        const safeDaysQuery = `
             SELECT 
                DATEDIFF(day, 
                    ISNULL((
                        SELECT MAX(t.CreatedDate) 
                        FROM planner_tasks t
                        WHERE t.Labels LIKE @keyword
                        ${areaFilter}
                    ), '2024-01-01'), 
                    GETDATE()
                ) as safeDays
        `;
        const pSafeDays = pool.request()
            .input('keyword', sql.NVarChar, '%急救%')
            .input('areas', sql.NVarChar, areas || '')
            .query(safeDaysQuery);


        const [incidentsRes, heatmapRes, countIncidentsRes, countHazardsRes, safeDaysRes, hazardTasksRes, areasRes, greenCrossRes] = await Promise.all([
            pIncidents, pHeatmap, pCountIncidents, pCountHazards, pSafeDays, pHazardTasks, pAreas, pGreenCross
        ]);

        console.log('[EHS API] Incidents Found:', incidentsRes.recordset.length);

        return NextResponse.json({
            incidents: incidentsRes.recordset,
            hazardTasks: hazardTasksRes.recordset,
            hazardHeatmap: heatmapRes.recordset,
            stats: {
                incidents: countIncidentsRes.recordset[0]?.count || 0,
                hazards: countHazardsRes.recordset[0]?.count || 0,
                safeDays: safeDaysRes.recordset[0]?.safeDays || 0
            },
            greenCross: greenCrossRes.recordset,
            areaHazards: [],
            filterOptions: {
                areas: areasRes.recordset.map(r => r.area)
            }
        });

    } catch (error: any) { // eslint-disable-line @typescript-eslint/no-explicit-any
        console.error('[EHS API] ERROR:', error);
        return NextResponse.json({ error: error.message }, { status: 500 });
    }
}

export async function POST(req: NextRequest) {
    try {
        const body = await req.json();
        const { date, status, details } = body;

        const pool = await getDbConnection();

        // UPSERT logic using MERGE
        const upsertQuery = `
            MERGE INTO dbo.safety_green_cross_log AS target
            USING (SELECT @date AS date, @status AS status, @details AS incident_details) AS source
            ON (target.date = source.date)
            WHEN MATCHED THEN
                UPDATE SET 
                    status = source.status, 
                    incident_details = source.incident_details,
                    updated_at = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (date, status, incident_details, updated_at)
                VALUES (source.date, source.status, source.incident_details, GETDATE());
        `;

        await pool.request()
            .input('date', sql.Date, date)
            .input('status', sql.NVarChar, status)
            .input('details', sql.NVarChar, details || '')
            .query(upsertQuery);

        return NextResponse.json({ success: true });
    } catch (error: any) {
        console.error('[EHS API POST] ERROR:', error);
        return NextResponse.json({ error: error.message }, { status: 500 });
    }
}
