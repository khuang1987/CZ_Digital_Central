const sql = require('mssql');

async function run() {
    try {
        await sql.connect(process.env.DATABASE_URL);
        const res = await sql.query("SELECT TOP 1 * FROM dbo.dim_operation_mapping");
        if (res.recordset.length > 0) {
            console.log(Object.keys(res.recordset[0]));
        } else {
            // If empty, get schema info
            const schema = await sql.query("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'dim_operation_mapping'");
            console.log(schema.recordset.map(r => r.COLUMN_NAME));
        }
    } catch (e) {
        console.error(e);
    } finally {
        await sql.close();
    }
}

run();
