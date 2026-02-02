const sql = require('mssql');
const readline = require('readline');

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

const askQuestion = (query) => new Promise((resolve) => rl.question(query, resolve));

async function runTest() {
    console.log('=== MDDAP SQL Server Remote Connection Diagnostic ===');

    const server = await askQuestion('Enter Remote Server IP (e.g., 192.168.1.5): ');
    const user = await askQuestion('Enter DB User [mddap_user]: ') || 'mddap_user';
    const password = await askQuestion('Enter DB Password [1111]: ') || '1111';
    const database = await askQuestion('Enter DB Name [mddap_v2]: ') || 'mddap_v2';

    const config = {
        user: user,
        password: password,
        server: server,
        port: 1433,
        database: database,
        options: {
            encrypt: false,
            trustServerCertificate: true,
            connectTimeout: 10000 // 10 seconds
        }
    };

    console.log(`\nAttempting to connect to ${server}:1433...`);

    try {
        const pool = await sql.connect(config);
        console.log('✅ SUCCESS: Connected to SQL Server!');

        console.log('\nFetching server version...');
        const versionRes = await pool.request().query('SELECT @@VERSION as version');
        console.log(versionRes.recordset[0].version);

        console.log('\nChecking if database exists...');
        const dbRes = await pool.request().query(`SELECT name FROM sys.databases WHERE name = '${database}'`);
        if (dbRes.recordset.length > 0) {
            console.log(`✅ SUCCESS: Database [${database}] found.`);
        } else {
            console.log(`⚠️ WARNING: Connected to server, but database [${database}] was not found.`);
        }

        await pool.close();
        console.log('\nTest Completed Successfully.');
    } catch (err) {
        console.error('\n❌ CONNECTION FAILED:');
        console.error('Message:', err.message);
        console.error('Stack Trace:', err.stack);

        console.log('\nTroubleshooting Tips:');
        console.log('1. Ensure TCP/IP is ENABLED in SQL Server Configuration Manager on the remote server.');
        console.log('2. Ensure Port 1433 (TCP) is OPEN in the Windows Firewall on the remote server.');
        console.log('3. Ensure the SQL Server service is RUNNING.');
        console.log('4. Double check if the DB_USER has permission to log in via SQL Authentication.');
    } finally {
        rl.close();
    }
}

runTest();
