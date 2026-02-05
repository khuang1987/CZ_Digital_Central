
import { NextRequest, NextResponse } from 'next/server';
import { getDbConnection, sql } from '@/lib/db';

export async function GET(req: NextRequest) {
    try {
        const { searchParams } = new URL(req.url);
        const fiscalYear = searchParams.get('fiscalYear'); // e.g. "FY25"
        const area = searchParams.get('area');

        console.log('[EHS API] Fetching Data params:', { fiscalYear, area });
        const pool = await getDbConnection();

        // 1. Determine Date Range (Fiscal Year Start & End)
        // Default to Current Calendar Year if no FY provided
        const now = new Date();
        let startDate = new Date(now.getFullYear(), 0, 1);
        let endDate = new Date(now.getFullYear(), 11, 31);

        if (fiscalYear) {
            const fyRangeRes = await pool.request()
                .input('fy', sql.NVarChar, fiscalYear)
                .query(`SELECT MIN(date) as s, MAX(date) as e FROM dim_calendar WHERE fiscal_year = @fy`);

            if (fyRangeRes.recordset[0]?.s) {
                startDate = new Date(fyRangeRes.recordset[0].s);
                endDate = new Date(fyRangeRes.recordset[0].e);
            }
        }

        console.log('[EHS API] Date Range:', { startDate, endDate });

        // ==========================================
        // QUERY G: INCIDENT LIST (Parameterized)
        // ==========================================
        // Using parameterized input for Chinese characters to avoid encoding issues
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
            -- Filtering by date range to match fiscal year
            AND t.CreatedDate BETWEEN @start AND @end 
            ${area ? `AND t.TeamName = @area` : ''}
            ORDER BY t.CreatedDate DESC
        `;

        // ==========================================
        // QUERY H: HAZARD HEATMAP (Fiscal Range)
        // ==========================================
        const heatmapQuery = `
             SELECT 
                t.TeamName as area,
                c.fiscal_month as month,
                COUNT(*) as count
            FROM planner_tasks t
            JOIN dim_calendar c ON CAST(t.CreatedDate AS DATE) = c.date
            WHERE (t.BucketName = N'安全' OR t.BucketName = N'Safe' OR t.BucketName = N'Safety')
            AND t.CreatedDate BETWEEN @start AND @end
            AND (t.Status != 'Completed' AND t.Status != 'Closed')
            ${area ? `AND t.TeamName = @area` : ''}
            AND ISNULL(t.IsDeleted, 0) = 0
            GROUP BY t.TeamName, c.fiscal_month
        `;

        // ==========================================
        // QUERY I: HAZARD TASKS LIST (Full List for Analysis)
        // ==========================================
        const hazardTasksQuery = `
            SELECT 
                t.TaskId, 
                t.TaskName as title, 
                t.CreatedDate as date, 
                t.Status as status, 
                t.Labels as classification, 
                t.TeamName as area,
                t.Priority as priority
            FROM planner_tasks t
            WHERE (t.BucketName = N'安全' OR t.BucketName = N'Safe' OR t.BucketName = N'Safety')
            AND t.CreatedDate BETWEEN @start AND @end 
            AND ISNULL(t.IsDeleted, 0) = 0
            ORDER BY t.CreatedDate DESC
        `;

        // ==========================================
        // QUERY B: INCIDENT COUNT
        // ==========================================
        const incidentCountQuery = `
            SELECT COUNT(DISTINCT t.TaskId) as count
            FROM planner_tasks t
            WHERE t.CreatedDate BETWEEN @start AND @end
            AND t.Labels LIKE @keyword
            ${area ? `AND t.TeamName = @area` : ''}
            AND ISNULL(t.IsDeleted, 0) = 0
        `;

        // ==========================================
        // QUERY C: OPEN HAZARDS COUNT
        // ==========================================
        const hazardCountQuery = `
            SELECT COUNT(*) as count
            FROM planner_tasks t
            WHERE (BucketName = N'安全' OR BucketName = N'Safe' OR BucketName = N'Safety')
            AND t.CreatedDate BETWEEN @start AND @end
            ${area ? `AND TeamName = @area` : ''}
            AND t.Status NOT IN ('Completed', 'Closed')
            AND ISNULL(IsDeleted, 0) = 0
        `;

        // EXECUTE QUERIES PARALLEL
        const pIncidents = pool.request()
            .input('keyword', sql.NVarChar, '%急救%') // Parameterized!
            .input('start', sql.Date, startDate)
            .input('end', sql.Date, endDate)
            .input('area', sql.NVarChar, area || '')
            .query(incidentsQuery);

        const pHeatmap = pool.request()
            .input('start', sql.Date, startDate)
            .input('end', sql.Date, endDate)
            .input('area', sql.NVarChar, area || '')
            .query(heatmapQuery);

        const pHazardTasks = pool.request()
            .input('start', sql.Date, startDate)
            .input('end', sql.Date, endDate)
            .input('area', sql.NVarChar, area || '')
            .query(hazardTasksQuery);

        const pCountIncidents = pool.request()
            .input('keyword', sql.NVarChar, '%急救%')
            .input('start', sql.Date, startDate)
            .input('end', sql.Date, endDate)
            .input('area', sql.NVarChar, area || '')
            .query(incidentCountQuery);

        const pCountHazards = pool.request()
            .input('start', sql.Date, startDate)
            .input('end', sql.Date, endDate)
            .input('area', sql.NVarChar, area || '')
            .query(hazardCountQuery);

        // Safe days uses specific logic (since last incident), usually independent of selected FY range but for now simplifying
        const safeDaysQuery = `
             SELECT 
                DATEDIFF(day, 
                    ISNULL((
                        SELECT MAX(t.CreatedDate) 
                        FROM planner_tasks t
                        WHERE t.Labels LIKE @keyword
                        ${area ? `AND t.TeamName = @area` : ''}
                    ), '2024-01-01'), 
                    GETDATE()
                ) as safeDays
        `;
        const pSafeDays = pool.request()
            .input('keyword', sql.NVarChar, '%急救%')
            .input('area', sql.NVarChar, area || '')
            .query(safeDaysQuery);


        const [incidentsRes, heatmapRes, countIncidentsRes, countHazardsRes, safeDaysRes, hazardTasksRes] = await Promise.all([
            pIncidents, pHeatmap, pCountIncidents, pCountHazards, pSafeDays, pHazardTasks
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
            greenCross: [],
            areaHazards: [],
            filterOptions: { areas: [] }
        });

    } catch (error: any) { // eslint-disable-line @typescript-eslint/no-explicit-any
        console.error('[EHS API] ERROR:', error);
        return NextResponse.json({ error: error.message }, { status: 500 });
    }
}

export async function POST() {
    return NextResponse.json({ success: true });
}
