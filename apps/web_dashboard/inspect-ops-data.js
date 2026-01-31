const sql = require('mssql');
const fs = require('fs');
const path = require('path');

// Basic parser for .env.local
const envPath = path.join(__dirname, '.env.local');
const envContent = fs.readFileSync(envPath, 'utf8');
const env = {};
envContent.split('\n').forEach(line => {
    const parts = line.split('=');
    if (parts.length === 2) {
        env[parts[0].trim()] = parts[1].trim();
    }
});

const config = {
    user: env.DB_USER,
    password: env.DB_PASSWORD,
    server: env.DB_SERVER,
    port: parseInt(env.DB_PORT),
    database: env.DB_NAME,
    options: {
        encrypt: false,
        trustServerCertificate: true
    }
};

async function inspect() {
    try {
        const pool = await sql.connect(config);

        console.log('--- Sample: raw_sap_labor_hours (Top 5) ---');
        // Using * to see all operation-related columns
        const res1 = await pool.request().query("SELECT TOP 5 * FROM raw_sap_labor_hours");
        console.table(res1.recordset);

        console.log('\n--- Sample: dim_operation_mapping (Top 5) ---');
        const res2 = await pool.request().query("SELECT TOP 5 * FROM dim_operation_mapping");
        console.table(res2.recordset);

        await pool.close();
    } catch (err) {
        console.error(err.message);
    }
}

inspect();
