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
    // Stage-specific runs (mapping UI stages to run_etl_parallel.py stages)
    'ingestion-stage': {
        script: path.join(process.cwd(), '..', '..', 'scripts', 'orchestration', 'run_etl_parallel.py'),
        args: ['--stage', '0'],
    },
    'cleaning-stage': {
        script: path.join(process.cwd(), '..', '..', 'scripts', 'orchestration', 'run_etl_parallel.py'),
        args: ['--stage', '1,2,3'],
    },
    'cleaning-sfc': {
        script: path.join(process.cwd(), '..', '..', 'scripts', 'orchestration', 'run_etl_parallel.py'),
        args: ['--stage', '1,2,3', '--task-filter', 'sfc'],
    },
    'cleaning-mes': {
        script: path.join(process.cwd(), '..', '..', 'scripts', 'orchestration', 'run_etl_parallel.py'),
        args: ['--stage', '1,2,3', '--task-filter', 'mes'],
    },
    'cleaning-sap': {
        script: path.join(process.cwd(), '..', '..', 'scripts', 'orchestration', 'run_etl_parallel.py'),
        args: ['--stage', '1,2,3', '--task-filter', 'sap'],
    },
    'cleaning-others': {
        script: path.join(process.cwd(), '..', '..', 'scripts', 'orchestration', 'run_etl_parallel.py'),
        args: ['--stage', '1,2,3', '--task-filter', 'planner,calendar,operation'],
    },
    'output-stage': {
        script: path.join(process.cwd(), '..', '..', 'scripts', 'orchestration', 'run_etl_parallel.py'),
        args: ['--stage', '4'],
    },
    // Data Ingestion Tasks
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
    // Output Generation Tasks
    'output-parquet': {
        script: path.join(process.cwd(), '..', '..', 'scripts', 'orchestration', 'export_core_to_a1.py'),
    },
    'output-validation': {
        script: path.join(process.cwd(), '..', '..', 'scripts', 'orchestration', 'validate_parquet_output.py'),
    },
    // Reports & Dashboards
    'reports-stage': {
        script: path.join(process.cwd(), '..', '..', 'scripts', 'orchestration', 'run_etl_parallel.py'),
        args: ['--stage', '5'],
    },
    'reports-powerbi': {
        script: path.join(process.cwd(), '..', '..', 'scripts', 'orchestration', 'run_data_collection.py'),
        args: ['refresh'],
    },
};

// Status file path
const STATUS_FILE = path.join(process.cwd(), '..', '..', 'shared_infrastructure', 'logs', 'pipeline_status.json');

interface ExecutionStatus {
    startTime: string;
    endTime?: string;
    status: 'success' | 'running' | 'failed';
    exitCode?: number;
    error?: string;
    logFile?: string;
}

interface TaskStatus {
    [key: string]: ExecutionStatus | undefined;
}

interface StageStatus {
    startTime?: string;
    endTime?: string;
    status?: string;
    logFile?: string;
    tasks: TaskStatus;
}

interface PipelineStatus {
    stages: {
        ingestion: StageStatus;
        cleaning: StageStatus;
        output: StageStatus;
        reports: StageStatus;
    };
    fullPipeline?: ExecutionStatus;
}

async function loadStatus(): Promise<PipelineStatus> {
    try {
        const data = await fs.readFile(STATUS_FILE, 'utf-8');
        const status = JSON.parse(data);
        // Ensure all stages exist
        const stages = ['ingestion', 'cleaning', 'output', 'reports'];
        stages.forEach(s => {
            if (!status.stages[s]) {
                status.stages[s] = { tasks: {} };
            } else if (!status.stages[s].tasks) {
                status.stages[s].tasks = {};
            }
        });
        return status;
    } catch (error) {
        return {
            stages: {
                ingestion: { tasks: {} },
                cleaning: { tasks: {} },
                output: { tasks: {} },
                reports: { tasks: {} },
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

        // Execute script
        const startTime = new Date().toISOString();
        const isbat = scriptConfig.script.endsWith('.bat');
        const logFile = `pipeline_${key}_${new Date().getTime()}.log`;
        const logPath = path.join(process.cwd(), '..', '..', 'shared_infrastructure', 'logs', logFile);

        const child = spawn(
            isbat ? scriptConfig.script : 'python',
            isbat ? [] : [scriptConfig.script, ...(scriptConfig.args || [])],
            {
                cwd: path.join(process.cwd(), '..', '..'),
                detached: true,
                stdio: ['ignore', 'pipe', 'pipe'], // Capture stdout and stderr
                env: {
                    ...process.env,
                    PYTHONIOENCODING: 'utf-8',
                    PYTHONUTF8: '1',
                }
            }
        );

        // Initial Update with PID
        const status = await loadStatus();
        const initialStatus: ExecutionStatus = {
            startTime,
            status: 'running',
            pid: child.pid,
            logFile,
        };

        updateStatusInfo(status, stage, task, initialStatus);
        await saveStatus(status);

        // Pipe output to log file
        const logStream = require('fs').createWriteStream(logPath);
        child.stdout?.pipe(logStream);
        child.stderr?.pipe(logStream);

        child.on('close', async (code) => {
            const endTime = new Date().toISOString();
            const currentStatus = await loadStatus();
            const result: ExecutionStatus = {
                startTime,
                endTime,
                status: code === 0 ? 'success' : 'failed',
                exitCode: code ?? undefined,
                logFile,
            };

            updateStatusInfo(currentStatus, stage, task, result);
            await saveStatus(currentStatus);
        });

        child.unref();

        return NextResponse.json({
            success: true,
            message: `Started ${key}`,
            startTime,
            pid: child.pid,
            logFile,
        });
    } catch (error) {
        console.error('Pipeline execution error:', error);
        return NextResponse.json(
            { error: 'Internal server error' },
            { status: 500 }
        );
    }
}

function updateStatusInfo(status: PipelineStatus, stage: string, task: string | undefined, execution: ExecutionStatus) {
    if (stage === 'full') {
        status.fullPipeline = execution;
        return;
    }

    // Defensive initialization
    if (!status.stages) status.stages = {} as any;
    if (!status.stages[stage as keyof typeof status.stages]) {
        (status.stages as any)[stage] = { tasks: {} };
    }

    const stageObj = status.stages[stage as keyof typeof status.stages];

    if (task === 'stage') {
        stageObj.startTime = execution.startTime;
        stageObj.endTime = execution.endTime;
        stageObj.status = execution.status;
        stageObj.logFile = execution.logFile;
    } else if (task) {
        if (!stageObj.tasks) stageObj.tasks = {};
        stageObj.tasks[task] = execution;
    }
}
