
import { getDbConnection, sql } from './src/lib/db';
import fs from 'fs';
import path from 'path';

// Manually load env
const envPath = path.resolve(process.cwd(), '.env.local');
if (fs.existsSync(envPath)) {
    const envConfig = fs.readFileSync(envPath, 'utf8');
    envConfig.split('\n').forEach(line => {
        const [key, value] = line.split('=');
        if (key && value) {
            process.env[key.trim()] = value.trim();
        }
    });
}

async function diagnose() {
    try {
        console.log('Connecting to DB...');
        const pool = await getDbConnection();

        console.log('\n--- DIAGNOSTIC START ---');

        // 1. Check Distinct Labels
        console.log('\n1. Checking Distinct Labels in planner_task_labels:');
        const r1 = await pool.request().query(`
            SELECT DISTINCT CleanedLabel 
            FROM planner_task_labels 
            WHERE CleanedLabel IS NOT NULL 
            ORDER BY CleanedLabel
        `);
        console.table(r1.recordset);

        // 2. Search for "急救" specifically
        console.log('\n2. Searching for labels containing "急救":');
        const r2 = await pool.request().query(`
            SELECT COUNT(*) as count 
            FROM planner_task_labels 
            WHERE CleanedLabel LIKE N'%急救%'
        `);
        console.log('Count of "%急救%":', r2.recordset[0].count);

        if (r2.recordset[0].count > 0) {
            const r2b = await pool.request().query(`
                SELECT TOP 5 * 
                FROM planner_task_labels 
                WHERE CleanedLabel LIKE N'%急救%'
            `);
            console.log('Sample Matches:', r2b.recordset);
        }

        // 3. Check Tasks joined with Labels (Simulate logic)
        console.log('\n3. Checking Joined Data (Last 10 items matched):');
        const r3 = await pool.request().query(`
            SELECT TOP 10 
                t.TaskName, 
                l.CleanedLabel, 
                t.CreatedDate 
            FROM planner_tasks t
            LEFT JOIN planner_task_labels l ON t.TaskId = l.TaskId
            WHERE l.CleanedLabel LIKE N'%急救%'
            ORDER BY t.CreatedDate DESC
        `);
        console.table(r3.recordset);

        console.log('\n--- DIAGNOSTIC END ---');
        process.exit(0);

    } catch (err) {
        console.error('Error:', err);
        process.exit(1);
    }
}

diagnose();
