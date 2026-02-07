import { NextRequest, NextResponse } from 'next/server';
import { getDbConnection, sql } from '@/lib/db';

export async function GET(req: NextRequest) {
    try {
        const searchParams = req.nextUrl.searchParams;
        const dateParam = searchParams.get('date');
        const vsmParam = searchParams.get('vsm');
        const factoriesParam = searchParams.get('factories');
        const areasParam = searchParams.get('areas');
        const productsParam = searchParams.get('products');

        const pool = await getDbConnection();

        // 1. Determine Snapshot Date
        let snapshotDate = dateParam;
        if (!snapshotDate) {
            const latestRes = await pool.request().query(`
                SELECT MAX(snapshot_date) as latest FROM (
                    SELECT TRY_CAST(snapshot_date AS DATE) as snapshot_date FROM raw_mes_wip_cmes WITH (NOLOCK)
                    UNION
                    SELECT TRY_CAST(snapshot_date AS DATE) as snapshot_date FROM raw_sfc_wip_czm WITH (NOLOCK)
                ) d
            `);
            snapshotDate = latestRes.recordset[0]?.latest;
        }
        if (!snapshotDate) snapshotDate = new Date().toISOString().split('T')[0];
        const dateStr = new Date(snapshotDate).toISOString().split('T')[0];

        // 2. Fetch Raw Data for Main Dashboard
        // We pull raw records and do mapping/filtering in JS to avoid SQL timeouts
        const [mesRaw, sfcRaw, mapRaw] = await Promise.all([
            pool.request().input('date', sql.Date, dateStr).query(`
                SELECT 'MES' as Plant, ProductNumber as product_no, ProductionOrder as batch_no, Step as operation, 
                       TRY_CAST(ISNULL(MaterialQty, 0) AS FLOAT) as qty, MaterialState as status, DateEnteredStep as track_in_time, 
                       snapshot_date, CAST(ERPMRPController AS NVARCHAR(100)) as VSM
                FROM raw_mes_wip_cmes WITH (NOLOCK) WHERE snapshot_date = @date
            `),
            pool.request().input('date', sql.Date, dateStr).query(`
                SELECT 'CZM' as Plant, product_no, batch_no, current_operation_name as operation, 
                       TRY_CAST(ISNULL(qualified_qty, 0) AS FLOAT) as qty, operation_status as status, 
                       TRY_CAST(check_in_time AS DATETIME) as track_in_time, TRY_CAST(snapshot_date AS DATE) as snapshot_date, 
                       CAST(product_type AS NVARCHAR(100)) as VSM
                FROM raw_sfc_wip_czm WITH (NOLOCK) WHERE TRY_CAST(snapshot_date AS DATE) = @date
            `),
            pool.request().query(`
                SELECT operation_name, MAX(area) as area, MAX(lead_time) as lead_time 
                FROM dim_operation_mapping WITH (NOLOCK) GROUP BY operation_name
            `)
        ]);

        // 3. Data Processing in JS
        const mappingTable = new Map(mapRaw.recordset.map(m => [m.operation_name, m]));
        const unified = [...mesRaw.recordset, ...sfcRaw.recordset].map(row => {
            const map = mappingTable.get(row.operation);
            const area = map?.area || 'Other';
            const standard_lt = map?.lead_time || 5.0;
            const lt_days = row.track_in_time ? (new Date().getTime() - new Date(row.track_in_time).getTime()) / (1000 * 60 * 60 * 24) : 0;
            const overdue_status = lt_days > (standard_lt + 8.0 / 24.0) ? 'Overdue' : 'Normal';
            const is_not_started = row.track_in_time ? 0 : 1;
            return { ...row, Area: area, standard_lt, lt_days, overdue_status, is_not_started };
        });

        // 4. Client-side Filtering Logic
        const filterFactories = factoriesParam?.split(',').filter(v => v) || [];
        const filterAreas = areasParam?.split(',').filter(v => v) || [];
        const filterVsms = (vsmParam || searchParams.get('vsms'))?.split(',').filter(v => v) || [];
        const filterProducts = productsParam?.split(',').filter(v => v) || [];

        const filtered = unified.filter(row => {
            if (filterFactories.length && !filterFactories.includes(row.Plant)) return false;
            if (filterAreas.length && !filterAreas.includes(row.Area)) return false;
            if (filterVsms.length && !filterVsms.includes(row.VSM)) return false;
            if (filterProducts.length && !filterProducts.includes(row.product_no)) return false;
            return true;
        });

        // 5. Aggregate Results
        const stats = {
            totalBatches: filtered.length,
            totalQty: filtered.reduce((sum, r) => sum + (r.qty || 0), 0),
            notStartedCount: filtered.reduce((sum, r) => sum + r.is_not_started, 0),
            overdueCount: filtered.reduce((sum, r) => sum + (r.overdue_status === 'Overdue' ? 1 : 0), 0),
            overdueNotStartedCount: filtered.reduce((sum, r) => sum + (r.overdue_status === 'Overdue' && r.is_not_started ? 1 : 0), 0),
            avgLT: filtered.length ? filtered.reduce((sum, r) => sum + (r.lt_days || 0), 0) / filtered.length : 0
        };

        const areaDistributionMap = new Map();
        filtered.forEach(r => {
            const curr = areaDistributionMap.get(r.Area) || { Area: r.Area, qty: 0, overdue_qty: 0 };
            curr.qty += (r.qty || 0);
            if (r.overdue_status === 'Overdue') curr.overdue_qty += (r.qty || 0);
            areaDistributionMap.set(r.Area, curr);
        });
        const areaDistribution = Array.from(areaDistributionMap.values()).sort((a, b) => b.qty - a.qty);

        const opDistributionMap = new Map();
        filtered.forEach(r => {
            const key = `${r.operation}|${r.Area}`;
            const curr = opDistributionMap.get(key) || { operation: r.operation, Area: r.Area, qty: 0, overdue_qty: 0 };
            curr.qty += (r.qty || 0);
            if (r.overdue_status === 'Overdue') curr.overdue_qty += (r.qty || 0);
            opDistributionMap.set(key, curr);
        });
        const opDistribution = Array.from(opDistributionMap.values()).sort((a, b) => b.qty - a.qty).slice(0, 30);

        // 6. Async Fetch Trend & Options (Slower queries)
        const [trendRes, optionsRes] = await Promise.all([
            pool.request().query(`
                SELECT TRY_CAST(snapshot_date AS DATE) as date, SUM(qty) as qty
                FROM (
                    SELECT snapshot_date, TRY_CAST(ISNULL(MaterialQty, 0) AS FLOAT) as qty FROM raw_mes_wip_cmes WITH (NOLOCK) WHERE snapshot_date >= DATEADD(DAY, -45, GETDATE())
                    UNION ALL
                    SELECT TRY_CAST(snapshot_date AS DATE) as snapshot_date, TRY_CAST(ISNULL(qualified_qty, 0) AS FLOAT) as qty FROM raw_sfc_wip_czm WITH (NOLOCK) WHERE TRY_CAST(snapshot_date AS DATE) >= DATEADD(DAY, -45, GETDATE())
                ) combined
                GROUP BY TRY_CAST(snapshot_date AS DATE) ORDER BY date DESC OFFSET 0 ROWS FETCH NEXT 15 ROWS ONLY
            `),
            pool.request().query(`
                SELECT DISTINCT date as snapshot_date FROM (
                    SELECT TRY_CAST(snapshot_date AS DATE) as date FROM raw_mes_wip_cmes WITH (NOLOCK) WHERE snapshot_date >= DATEADD(DAY, -90, GETDATE())
                    UNION 
                    SELECT TRY_CAST(snapshot_date AS DATE) as date FROM raw_sfc_wip_czm WITH (NOLOCK) WHERE TRY_CAST(snapshot_date AS DATE) >= DATEADD(DAY, -90, GETDATE())
                ) d ORDER BY snapshot_date DESC;
                SELECT DISTINCT factory as Plant FROM dim_area_mapping WITH (NOLOCK) ORDER BY factory;
                SELECT DISTINCT area as Area FROM dim_operation_mapping WITH (NOLOCK) WHERE area IS NOT NULL ORDER BY area;
                SELECT DISTINCT VSM FROM (
                    SELECT DISTINCT CAST(ERPMRPController AS NVARCHAR(100)) as VSM FROM raw_mes_wip_cmes WITH (NOLOCK) WHERE snapshot_date >= DATEADD(DAY, -30, GETDATE()) AND ERPMRPController IS NOT NULL
                    UNION
                    SELECT DISTINCT CAST(product_type AS NVARCHAR(100)) as VSM FROM raw_sfc_wip_czm WITH (NOLOCK) WHERE TRY_CAST(snapshot_date AS DATE) >= DATEADD(DAY, -30, GETDATE()) AND product_type IS NOT NULL
                ) v ORDER BY VSM;
            `)
        ]);

        return NextResponse.json({
            snapshotDate: dateStr,
            stats,
            areaDistribution,
            opDistribution,
            records: filtered.sort((a, b) => {
                if (a.overdue_status !== b.overdue_status) return a.overdue_status === 'Overdue' ? -1 : 1;
                return b.lt_days - a.lt_days;
            }).slice(0, 500),
            trendHistory: trendRes.recordset?.reverse() || [],
            filterOptions: {
                dates: (optionsRes as any).recordsets[0].map((r: any) => new Date(r.snapshot_date).toISOString().split('T')[0]),
                factories: (optionsRes as any).recordsets[1].map((r: any) => r.Plant),
                areas: (optionsRes as any).recordsets[2].map((r: any) => r.Area),
                vsms: (optionsRes as any).recordsets[3].map((r: any) => r.VSM)
            }
        });

    } catch (err: any) {
        console.error('WIP API Error:', err);
        return NextResponse.json({ error: err.message, stack: err.stack }, { status: 500 });
    }
}
