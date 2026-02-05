import { NextResponse } from 'next/server';
import path from 'path';
import fs from 'fs/promises';

const STATUS_FILE = path.join(process.cwd(), '..', '..', 'shared_infrastructure', 'logs', 'pipeline_status.json');

export async function GET() {
    try {
        const data = await fs.readFile(STATUS_FILE, 'utf-8');
        const status = JSON.parse(data);
        return NextResponse.json(status);
    } catch (error) {
        // Return empty status if file doesn't exist
        return NextResponse.json({
            stages: {
                ingestion: { tasks: {} },
                cleaning: { tasks: {} },
                output: { tasks: {} },
                reports: { tasks: {} },
            },
        });
    }
}
