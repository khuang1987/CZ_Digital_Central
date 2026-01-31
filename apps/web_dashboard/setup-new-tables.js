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

async function setup() {
    try {
        const pool = await sql.connect(config);

        // 1. Create dim_plant
        console.log('Creating dim_plant...');
        await pool.request().query(`
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_plant')
            BEGIN
                CREATE TABLE dim_plant (
                    plant_code NVARCHAR(50) PRIMARY KEY,
                    plant_name NVARCHAR(200) NOT NULL,
                    business_unit NVARCHAR(100) DEFAULT 'China Operations'
                );
                INSERT INTO dim_plant (plant_code, plant_name) VALUES 
                ('9997', '康辉工厂 (Kanghui)'),
                ('1303', '常州医疗运营 (CZ Ops)');
            END
        `);

        // 2. Create dim_production_targets
        console.log('Creating dim_production_targets...');
        await pool.request().query(`
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_production_targets')
            BEGIN
                CREATE TABLE dim_production_targets (
                    Date DATE PRIMARY KEY,
                    target_eh_9997 FLOAT NULL,
                    target_eh_1303 FLOAT NULL,
                    is_workday INT DEFAULT 0
                );
            END
        `);

        console.log('Tables setup successfully.');
        await pool.close();
    } catch (err) {
        console.error('Error during setup:', err.message);
    }
}

setup();
