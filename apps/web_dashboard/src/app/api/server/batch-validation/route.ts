import { NextResponse } from 'next/server';
import { getDbConnection } from '@/lib/db';
import sql from 'mssql';

export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
    const { searchParams } = new URL(request.url);
    const mode = searchParams.get('mode');
    const batch = searchParams.get('batch');
    const operation = searchParams.get('operation');

    try {
        const pool = await getDbConnection();

        // Mode 1: Batch List with Filters
        if (mode === 'batch_list') {
            const startDate = searchParams.get('startDate');
            const endDate = searchParams.get('endDate');
            const year = searchParams.get('year');           // FY25
            const month = searchParams.get('month');         // May
            const areas = searchParams.get('areas');
            const ops = searchParams.get('ops');
            const weeks = searchParams.get('weeks');
            const search = searchParams.get('search');

            const needsCalendar = weeks || year || month;
            const needsOpMapping = ops;

            let query = `
                SELECT DISTINCT TOP 200 l.BatchNumber 
                FROM dbo.raw_mes l
                ${needsCalendar ? 'INNER JOIN dbo.dim_calendar dc ON TRY_CAST(l.TrackInTime AS DATE) = TRY_CAST(dc.date AS DATE)' : ''}
                ${needsOpMapping ? `
                LEFT JOIN dbo.dim_operation_mapping om ON 
                    l.Plant = om.erp_code
                    AND LTRIM(RTRIM(l.Operation)) = LTRIM(RTRIM(om.operation_name))
                ` : ''}
                WHERE 1=1
            `;

            const req = pool.request();

            if (search) {
                query += " AND l.BatchNumber LIKE @search";
                req.input('search', sql.NVarChar, `%${search}%`);
            }

            if (startDate) {
                query += " AND l.TrackInTime >= @startDate";
                req.input('startDate', sql.Date, startDate);
            }
            if (endDate) {
                query += " AND l.TrackOutTime < DATEADD(day, 1, @endDate)";
                req.input('endDate', sql.Date, endDate);
            }

            if (year) {
                query += " AND dc.fiscal_year = @year";
                req.input('year', sql.NVarChar, year);
            }
            if (month) {
                query += " AND dc.fiscal_month = @month";
                req.input('month', sql.NVarChar, month);
            }

            if (weeks) {
                const weekList = weeks.split(',').map(w => parseInt(w.trim())).filter(w => !isNaN(w));
                if (weekList.length > 0) {
                    query += ` AND dc.fiscal_week IN (${weekList.join(',')})`;
                }
            }

            if (areas) {
                const areaList = areas.split(',');
                const areaConditions = areaList.map((a, i) => {
                    const paramName = `area${i}`;
                    req.input(paramName, sql.NVarChar, a.trim());
                    return `l.Plant = @${paramName}`;
                });
                if (areaConditions.length > 0) {
                    query += ` AND (${areaConditions.join(' OR ')})`;
                }
            }

            if (ops) {
                const opList = ops.split(',');
                const opConditions = opList.map((op, i) => {
                    const paramName = `opName${i}`;
                    req.input(paramName, sql.NVarChar, op);
                    return `om.display_name = @${paramName}`;
                });
                if (opConditions.length > 0) {
                    query += ` AND (${opConditions.join(' OR ')})`;
                }
            }

            query += " ORDER BY l.BatchNumber DESC";

            const result = await req.query(query);
            return NextResponse.json(result.recordset.map(r => r.BatchNumber));
        }

        // Mode 1.1: All Operations (For Sidebar Filter)
        if (mode === 'all_ops') {
            const result = await pool.request().query(`
                SELECT DISTINCT 
                    LTRIM(RTRIM(display_name)) as value,
                    LTRIM(RTRIM(display_name)) as label
                FROM dbo.dim_operation_mapping
                WHERE erp_code IN ('1303', '9997')
                AND display_name IS NOT NULL
                AND LTRIM(RTRIM(display_name)) != ''
                ORDER BY label
            `);
            return NextResponse.json(result.recordset);
        }

        // Mode 2: Operation List (With Names)
        if (mode === 'op_list') {
            if (!batch) return NextResponse.json({ error: 'Batch required' }, { status: 400 });

            // We use v_mes_metrics or dim_operation_mapping to get names
            // Mapping from raw_mes Operation (code) to dim_operation_mapping
            const result = await pool.request()
                .input('batch', sql.NVarChar, batch)
                .query(`
                    SELECT DISTINCT 
                        LTRIM(RTRIM(l.Operation)) as code,
                        ISNULL(om.display_name, LTRIM(RTRIM(l.Operation))) as name,
                        LTRIM(RTRIM(l.OperationDesc)) as [desc]
                    FROM dbo.raw_mes l
                    LEFT JOIN dbo.dim_operation_mapping om ON 
                        l.Plant = om.erp_code
                        AND LTRIM(RTRIM(l.Operation)) = LTRIM(RTRIM(om.operation_name))
                    WHERE l.BatchNumber = @batch
                `);

            const ops = result.recordset
                .filter(r => r.code)
                .sort((a, b) => {
                    const numA = parseFloat(a.code);
                    const numB = parseFloat(b.code);
                    if (!isNaN(numA) && !isNaN(numB)) return numA - numB;
                    return a.code.localeCompare(b.code);
                });

            return NextResponse.json(ops);
        }


        // Mode 2.1: Batch Information (Header)
        if (mode === 'batch_info') {
            if (!batch || !operation) return NextResponse.json({ error: 'Batch and operation required' }, { status: 400 });

            const result = await pool.request()
                .input('batch', sql.NVarChar, batch)
                .input('op', sql.NVarChar, operation)
                .query(`
                    SELECT TOP 1
                        l.BatchNumber,
                        l.ProductionOrder as OrderNo,
                        l.ProductNumber as ProductNo_MES,
                        l.CFN as ProductNo,
                        l.Machine,
                        LTRIM(RTRIM(l.Operation)) as OpCode,
                        LTRIM(RTRIM(l.OperationDesc)) as RawOpDesc,
                        ISNULL(om.display_name, LTRIM(RTRIM(l.Operation))) as OpName,
                        l.StepInQuantity as Qty_In,
                        l.TrackOutQuantity as Qty_Out,
                        l.TrackOutOperator as Operator
                    FROM dbo.raw_mes l
                    LEFT JOIN dbo.dim_operation_mapping om ON 
                        l.Plant = om.erp_code
                        AND LTRIM(RTRIM(l.Operation)) = LTRIM(RTRIM(om.operation_name))
                    WHERE l.BatchNumber = @batch AND LTRIM(RTRIM(l.Operation)) = LTRIM(RTRIM(@op))
                    ORDER BY l.TrackOutTime DESC
                `);

            if (result.recordset.length === 0) return NextResponse.json({ error: 'Not found' }, { status: 404 });
            return NextResponse.json(result.recordset[0]);
        }

        // Mode 3: Validation Data (Full Dump for Verification)
        if (mode === 'validate') {
            if (!batch || !operation) return NextResponse.json({ error: 'Batch and Operation required' }, { status: 400 });

            // 1. View Metrics ("The Answer Key")
            const viewResult = await pool.request()
                .input('batch', sql.NVarChar, batch)
                .input('op', sql.NVarChar, operation)
                .query(`
                    SELECT * FROM dbo.v_mes_metrics
                    WHERE BatchNumber = @batch AND LTRIM(RTRIM(Operation)) = LTRIM(RTRIM(@op))
                `);

            // 2. Raw MES
            const mesResult = await pool.request()
                .input('batch', sql.NVarChar, batch)
                .input('op', sql.NVarChar, operation)
                .query(`
                    SELECT 
                        BatchNumber, Operation, Machine, CFN, ProductNumber, [Group],
                        EnterStepTime, TrackInTime, TrackOutTime,
                        StepInQuantity, TrackOutQuantity, TrackOutOperator
                    FROM dbo.raw_mes
                    WHERE BatchNumber = @batch AND LTRIM(RTRIM(Operation)) = LTRIM(RTRIM(@op))
                `);

            // 3. Raw SFC
            const sfcResult = await pool.request()
                .input('batch', sql.NVarChar, batch)
                .input('op', sql.NVarChar, operation)
                .query(`
                    SELECT 
                        BatchNumber, Operation, TrackInTime, ScrapQty
                    FROM dbo.raw_sfc
                    WHERE BatchNumber = @batch AND LTRIM(RTRIM(Operation)) = LTRIM(RTRIM(@op))
                `);

            // 4. Trace SAP Routing (if MES data exists)
            let routingResult: any[] = [];
            if (mesResult.recordset.length > 0) {
                const cfn = mesResult.recordset[0].CFN;
                const grp = mesResult.recordset[0].Group;
                if (cfn && grp) {
                    const rRes = await pool.request()
                        .input('cfn', sql.NVarChar, cfn)
                        .input('op', sql.NVarChar, operation)
                        .input('grp', sql.NVarChar, grp)
                        .query(`
                            SELECT TOP 1
                                CFN, Operation, [Group], StandardTime, EH_machine, EH_labor, Quantity, SetupTime, OEE
                            FROM dbo.raw_sap_routing
                            WHERE CFN = @cfn AND LTRIM(RTRIM(Operation)) = LTRIM(RTRIM(@op)) AND [Group] = @grp
                            ORDER BY COALESCE(updated_at, created_at) DESC
                        `);
                    routingResult = rRes.recordset;
                }
            }

            // 5. Calendar Info (For Logic Reproduction)
            // We fetch the relevant calendar dates based on the timestamps involved
            // Determine range from raw data to minimize query
            const times = [
                ...mesResult.recordset.map((r: any) => r.EnterStepTime),
                ...mesResult.recordset.map((r: any) => r.TrackInTime),
                ...mesResult.recordset.map((r: any) => r.TrackOutTime),
                ...sfcResult.recordset.map((r: any) => r.TrackInTime)
            ].filter(d => d);

            let calendarData: any[] = [];

            if (times.length > 0) {
                const minDate = new Date(Math.min(...times.map((d: any) => new Date(d).getTime())));
                const maxDate = new Date(Math.max(...times.map((d: any) => new Date(d).getTime())));
                // Expand range by 10 days just in case
                minDate.setDate(minDate.getDate() - 10);
                maxDate.setDate(maxDate.getDate() + 10);

                const calRes = await pool.request()
                    .input('start', sql.Date, minDate)
                    .input('end', sql.Date, maxDate)
                    .query(`
                        SELECT CalendarDate, IsWorkday, CumulativeNonWorkDays 
                        FROM dbo.dim_calendar_cumulative
                        WHERE CalendarDate BETWEEN @start AND @end
                    `);
                calendarData = calRes.recordset;
            }

            return NextResponse.json({
                view_metrics: viewResult.recordset,
                raw_mes: mesResult.recordset,
                raw_sfc: sfcResult.recordset,
                raw_routing: routingResult,
                calendar_context: calendarData
            });
        }

        return NextResponse.json({ error: 'Invalid mode' }, { status: 400 });

    } catch (error: any) {
        console.error('Batch Validation API Error:', error);
        return NextResponse.json({ error: error.message }, { status: 500 });
    }
}
