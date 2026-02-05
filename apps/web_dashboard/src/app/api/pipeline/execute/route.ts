import { NextRequest, NextResponse } from 'next/server';
import { spawn } from 'child_process';
import path from 'path';
import fs from 'fs/promises';

// Map UI stage/task to actual script paths
const SCRIPT_MAP: Record<string, { script: string; args?: string[] }> = {
    // Full Pipeline
    'full': {
        script: path.join(process.cwd(), '..', '..', 'scripts', 'orchestration', 'refresh_parallel.bat'),
    },
    // Data Ingestion Stage
    'ingestion-planner': {
        script: path.join(process.cwd(), '..', '..', 'scripts', 'orchestration', 'run_data_collection.py'),
        args: ['planner'],
    },
    'ingestion-cmes': {
        script: path.join(process.cwd(), '..', '..', 'scripts', 'orchestration', 'run_data_collection.py'),
        args: ['cmes'],
    },
    'ingestion-labor': {
        script: path.join(process.cwd(), '..', '..', 'scripts', 'orchestration', 'run_data_collection.py'),
        args: ['labor'],
    },
    'ingestion-all': {
        script: path.join(process.cwd(), '..', '..', 'scripts', 'orchestration', 'run_data_collection.py'),
        args: ['all'],
    },
    // Data Cleaning Stage (part of ETL pipeline)
    'cleaning-all': {
        script: path.join(process.cwd(), '..', '..', 'scripts', 'orchestration', 'refresh_parallel.bat'),
    },
    // Output Generation Stage
    'output-parquet': {
        script: path.join(process.cwd(), '..', '..', 'scripts', 'orchestration', 'export_core_to_a1.py'),
    },
    'output-validation': {
        script: path.join(process.cwd(), '..', '..', 'scripts', 'orchestration', 'validate_parquet_output.py'),
    },
};

// Status file path
const STATUS_FILE = path.join(process.cwd(), '..', '..', 'shared_infrastructure', 'logs', 'pipeline_status.json');

interface ExecutionStatus {
    timestamp: string;
    status: 'success' | 'running' | 'failed';
    exitCode?: number;
    error?: string;
}

interface PipelineStatus {
    stages: {
        ingestion: {
            lastRun?: string;
            status?: string;
            tasks: {
                planner?: ExecutionStatus;
                cmes?: ExecutionStatus;
                labor?: ExecutionStatus;
            };
        };
        cleaning: {
            lastRun?: string;
            status?: string;
        };
        output: {
            lastRun?: string;
            status?: string;
            tasks: {
                parquet?: ExecutionStatus;
                validation?: ExecutionStatus;
            };
        };
    };
    fullPipeline?: ExecutionStatus;
}

async function loadStatus(): Promise<PipelineStatus> {
    try {
        const data = await fs.readFile(STATUS_FILE, 'utf-8');
        return JSON.parse(data);
    } catch (error) {
        return {
            stages: {
                ingestion: { tasks: {} },
                cleaning: {},
                output: { tasks: {} },
            },
        };
    }
}

async function saveStatus(status: PipelineStatus): Promise<void> {
    try {
        await fs.mkdir(path.dirname(STATUS_FILE), { recursive: true });
        await fs.writeFile(STATUS_FILE, JSON.stringify(status, null, 2));
    } catch (error) {
        console.error('Failed to save pipeline status:', error);
    }
}

export async function POST(request: NextRequest) {
    try {
        const body = await request.json();
        const { stage, task } = body;

        // Validate input
        if (!stage) {
            return NextResponse.json({ error: 'Stage is required' }, { status: 400 });
        }

        // Construct key for script map
        const key = task ? `${stage}-${task}` : stage;
        const scriptConfig = SCRIPT_MAP[key];

        if (!scriptConfig) {
            return NextResponse.json(
                { error: `Unknown stage/task combination: ${key}` },
                { status: 400 }
            );
        }

        // Check if script exists
        try {
            await fs.access(scriptConfig.script);
        } catch {
            return NextResponse.json(
                { error: `Script not found: ${scriptConfig.script}` },
                { status: 404 }
            );
        }

        // Execute script in background
        const timestamp = new Date().toISOString();
        const isbat = scriptConfig.script.endsWith('.bat');

        const child = spawn(
            isbat ? scriptConfig.script : 'python',
            isbat ? [] : [scriptConfig.script, ...(scriptConfig.args || [])],
            {
                cwd: path.join(process.cwd(), '..', '..'),
                detached: true,
                stdio: 'ignore',
            }
        );

        child.unref();

        // Update status
        const status = await loadStatus();
        const executionStatus: ExecutionStatus = {
            timestamp,
            status: 'running',
        };

        if (stage === 'full') {
            status.fullPipeline = executionStatus;
        } else if (stage === 'ingestion' && task) {
            status.stages.ingestion.tasks[task as keyof typeof status.stages.ingestion.tasks] = executionStatus;
            status.stages.ingestion.lastRun = timestamp;
            status.stages.ingestion.status = 'running';
        } else if (stage === 'cleaning') {
            status.stages.cleaning.lastRun = timestamp;
            status.stages.cleaning.status = 'running';
        } else if (stage === 'output' && task) {
            status.stages.output.tasks[task as keyof typeof status.stages.output.tasks] = executionStatus;
            status.stages.output.lastRun = timestamp;
            status.stages.output.status = 'running';
        }

        await saveStatus(status);

        return NextResponse.json({
            success: true,
            message: `Started ${key}`,
            timestamp,
        });
    } catch (error) {
        console.error('Pipeline execution error:', error);
        return NextResponse.json(
            { error: 'Internal server error' },
            { status: 500 }
        );
    }
}
