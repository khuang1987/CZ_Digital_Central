import { NextRequest, NextResponse } from 'next/server';
import { getDbConnection, sql } from '@/lib/db';

export async function GET(req: NextRequest) {
    try {
        const { searchParams } = new URL(req.url);
        const year = searchParams.get('year') || 'FY26';
        const month = searchParams.get('month');
        const granularity = searchParams.get('granularity') || 'month';
        const week = searchParams.get('week');
        const startDateParam = searchParams.get('startDate');
        const endDateParam = searchParams.get('endDate');
        const productsParam = searchParams.get('products');
        const reasonsParam = searchParams.get('reasons');
        const processesParam = searchParams.get('processes');
        const recordersParam = searchParams.get('recorders');
        const statusParam = searchParams.get('status');

        const factoriesParam = searchParams.get('factories');
        const areasParam = searchParams.get('areas');

        const pool = await getDbConnection();

        // 1. Determine Date Range
        let startDay, endDay;

        if (granularity === 'custom' && startDateParam && endDateParam) {
            startDay = new Date(startDateParam);
            endDay = new Date(endDateParam);
        } else {
            let dateRangeQuery = '';
            const request = pool.request().input('year', sql.NVarChar, year);

            if (granularity === 'week' && week) {
                const weekList = week.split(',').map(w => parseInt(w));
                dateRangeQuery = `SELECT MIN(TRY_CAST(date AS DATE)) as s, MAX(TRY_CAST(date AS DATE)) as e FROM dim_calendar WHERE fiscal_year = @year AND fiscal_week IN (${weekList.join(',')})`;
            } else if (granularity === 'month' && month) {
                dateRangeQuery = `SELECT MIN(TRY_CAST(date AS DATE)) as s, MAX(TRY_CAST(date AS DATE)) as e FROM dim_calendar WHERE fiscal_year = @year AND fiscal_month = @month`;
                request.input('month', sql.NVarChar, month);
            } else {
                // Default to Year
                dateRangeQuery = `SELECT MIN(TRY_CAST(date AS DATE)) as s, MAX(TRY_CAST(date AS DATE)) as e FROM dim_calendar WHERE fiscal_year = @year`;
            }

            const res = await request.query(dateRangeQuery);
            const range = res.recordset[0];

            if (!range || !range.s || !range.e) {
                // Fallback to FY26 full year if specific month/week not found
                console.warn(`Date range not found for ${granularity} in ${year}. Falling back to FY range.`);
                const fallback = await pool.request().input('year', sql.NVarChar, year).query(`SELECT MIN(TRY_CAST(date AS DATE)) as s, MAX(TRY_CAST(date AS DATE)) as e FROM dim_calendar WHERE fiscal_year = @year`);
                ({ s: startDay, e: endDay } = fallback.recordset[0]);
            } else {
                ({ s: startDay, e: endDay } = range);
            }
        }

        if (!startDay || !endDay) {
            throw new Error(`Invalid date range determined for filters. Start: ${startDay}, End: ${endDay}`);
        }

        // Ensure startDay and endDay are Date objects or ISO strings without timezone if possible
        const s = new Date(startDay);
        const e = new Date(endDay);

        // Build Filters
        let filterSql = '';
        if (productsParam) {
            const vals = productsParam.split(',').filter(v => v).map(v => `'${v.replace(/'/g, "''")}'`).join(',');
            if (vals) filterSql += ` AND product_no IN (${vals})`;
        }
        if (reasonsParam) {
            const vals = reasonsParam.split(',').filter(v => v).map(v => `'${v.replace(/'/g, "''")}'`).join(',');
            if (vals) filterSql += ` AND nc_reason IN (${vals})`;
        }
        if (processesParam) {
            const vals = processesParam.split(',').filter(v => v).map(v => `'${v.replace(/'/g, "''")}'`).join(',');
            if (vals) filterSql += ` AND operation_name IN (${vals})`;
        }
        if (recordersParam) {
            const vals = recordersParam.split(',').filter(v => v).map(v => `'${v.replace(/'/g, "''")}'`).join(',');
            if (vals) filterSql += ` AND recorder IN (${vals})`;
        }
        if (statusParam) {
            const vals = statusParam.split(',').filter(v => v).map(v => `'${v.replace(/'/g, "''")}'`).join(',');
            if (vals) filterSql += ` AND operation_status IN (${vals})`;
        }
        if (factoriesParam) {
            const vals = factoriesParam.split(',').filter(v => v).map(v => `'${v.replace(/'/g, "''")}'`).join(',');
            if (vals) filterSql += ` AND EXISTS (SELECT 1 FROM dim_operation_mapping dom JOIN dim_area_mapping dam ON dom.area = dam.area WHERE dom.operation_name = nc.operation_name AND dam.factory IN (${vals}))`;
        }
        if (areasParam) {
            const vals = areasParam.split(',').filter(v => v).map(v => `'${v.replace(/'/g, "''")}'`).join(',');
            if (vals) filterSql += ` AND EXISTS (SELECT 1 FROM dim_operation_mapping dom WHERE dom.operation_name = nc.operation_name AND dom.area IN (${vals}))`;
        }

        const commonInputs = (req: any) => req
            .input('s', sql.Date, s)
            .input('e', sql.Date, e);

        const promises = [
            // A. Summary Stats
            (async () => {
                try {
                    return await commonInputs(pool.request())
                        .query(`
                            SELECT 
                                COUNT(*) as totalRecords,
                                SUM(nc_qty) as totalQty,
                                (SELECT COUNT(*) FROM raw_sfc_nc nc WHERE record_time BETWEEN @s AND @e AND operation_status <> N'完工' ${filterSql}) as openNCs,
                                (SELECT TOP 1 nc_reason FROM raw_sfc_nc nc WHERE record_time BETWEEN @s AND @e ${filterSql} GROUP BY nc_reason ORDER BY SUM(nc_qty) DESC) as topReason
                            FROM raw_sfc_nc nc
                            WHERE record_time BETWEEN @s AND @e ${filterSql}
                        `);
                } catch (err) {
                    console.error('Error fetching Summary Stats:', err);
                    return { recordset: [] };
                }
            })(),

            // B. Pareto Data
            (async () => {
                try {
                    return await commonInputs(pool.request())
                        .query(`
                            SELECT 
                                nc_reason as reason,
                                COUNT(*) as count,
                                SUM(nc_qty) as qty
                            FROM raw_sfc_nc nc
                            WHERE record_time BETWEEN @s AND @e ${filterSql}
                            GROUP BY nc_reason
                            ORDER BY qty DESC
                        `);
                } catch (err) {
                    console.error('Error fetching Pareto Data:', err);
                    return { recordset: [] };
                }
            })(),

            // C. Trend Data
            (async () => {
                try {
                    return await commonInputs(pool.request())
                        .query(`
                            SELECT 
                                FORMAT(TRY_CAST(record_time AS DATE), 'yyyy-MM') as month,
                                nc_reason as reason,
                                SUM(nc_qty) as qty
                            FROM raw_sfc_nc nc
                            WHERE record_time BETWEEN @s AND @e ${filterSql}
                            GROUP BY FORMAT(TRY_CAST(record_time AS DATE), 'yyyy-MM'), nc_reason
                            ORDER BY month
                        `);
                } catch (err) {
                    console.error('Error fetching Trend Data:', err);
                    return { recordset: [] };
                }
            })(),

            // D. Detailed Records (Top 500 for now to keep it responsive)
            (async () => {
                try {
                    return await commonInputs(pool.request())
                        .query(`
                            SELECT TOP 500
                                record_time as date,
                                batch_no as batch,
                                product_no as product,
                                operation_name as process,
                                machine_no as machine,
                                nc_qty as qty,
                                nc_reason as reason,
                                recorder,
                                action_taken,
                                operation_status as status,
                                feature_name,
                                nc_description
                            FROM raw_sfc_nc nc
                            WHERE record_time BETWEEN @s AND @e ${filterSql}
                            ORDER BY record_time DESC
                        `);
                } catch (err) {
                    console.error('Error fetching Detailed Records:', err);
                    return { recordset: [] };
                }
            })(),

            // E. Filter Options
            (async () => {
                try {
                    return await pool.request().query(`
                        SELECT DISTINCT product_no FROM raw_sfc_nc WHERE product_no IS NOT NULL ORDER BY product_no;
                        SELECT DISTINCT nc_reason FROM raw_sfc_nc WHERE nc_reason IS NOT NULL ORDER BY nc_reason;
                        SELECT DISTINCT operation_name FROM raw_sfc_nc WHERE operation_name IS NOT NULL ORDER BY operation_name;
                        SELECT DISTINCT recorder FROM raw_sfc_nc WHERE recorder IS NOT NULL ORDER BY recorder;
                        SELECT DISTINCT factory FROM dim_area_mapping WHERE factory IS NOT NULL ORDER BY factory;
                        SELECT DISTINCT area FROM dim_area_mapping WHERE area IS NOT NULL ORDER BY area;
                    `);
                } catch (err) {
                    console.error('Error fetching Filter Options:', err);
                    return { recordsets: [[], [], [], [], [], []] };
                }
            })()
        ];

        const [statsRes, paretoRes, trendRes, recordsRes, filtersRes] = await Promise.all(promises);

        // Transformation: Pareto cumulative %
        const paretoRaw = (paretoRes as any).recordset || [];
        const totalParetoQty = paretoRaw.reduce((sum: number, r: any) => sum + (r.qty || 0), 0);
        let cumulative = 0;
        const paretoData = paretoRaw.map((r: any) => {
            cumulative += (r.qty || 0);
            return {
                ...r,
                percentage: totalParetoQty > 0 ? (r.qty / totalParetoQty) * 100 : 0,
                cumulativePercentage: totalParetoQty > 0 ? Math.round((cumulative / totalParetoQty) * 100) : 0
            };
        });

        // Transformation: Trend Data pivot
        const trendRaw = (trendRes as any).recordset || [];
        const trendMap = new Map();
        trendRaw.forEach((r: any) => {
            const monthKey = r.month || 'Other';
            if (!trendMap.has(monthKey)) trendMap.set(monthKey, { month: monthKey });
            trendMap.get(monthKey)[r.reason || 'Unknown'] = r.qty;
        });
        const trendData = Array.from(trendMap.values());

        return NextResponse.json({
            stats: (statsRes as any).recordset[0] || { totalRecords: 0, totalQty: 0, openNCs: 0, topReason: 'N/A' },
            paretoData,
            trendData,
            records: (recordsRes as any).recordset || [],
            filterOptions: {
                products: (filtersRes as any).recordsets[0].map((r: any) => r.product_no),
                reasons: (filtersRes as any).recordsets[1].map((r: any) => r.nc_reason),
                processes: (filtersRes as any).recordsets[2].map((r: any) => r.operation_name),
                recorders: (filtersRes as any).recordsets[3].map((r: any) => r.recorder),
                factories: (filtersRes as any).recordsets[4].map((r: any) => r.factory),
                areas: (filtersRes as any).recordsets[5].map((r: any) => r.area)
            }
        });

    } catch (error: any) {
        console.error('NC Analysis API Error Details:', {
            message: error.message,
            stack: error.stack,
            url: req.url
        });
        return NextResponse.json({ error: error.message, details: error.stack }, { status: 500 });
    }
}
