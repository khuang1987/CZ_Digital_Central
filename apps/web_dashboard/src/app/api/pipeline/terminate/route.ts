import { NextRequest, NextResponse } from 'next/server';
import { exec } from 'child_process';
import path from 'path';
import fs from 'fs/promises';
import { promisify } from 'util';

const execAsync = promisify(exec);
const STATUS_FILE = path.join(process.cwd(), '..', '..', 'shared_infrastructure', 'logs', 'pipeline_status.json');

interface ExecutionStatus {
    startTime: string;
    endTime?: string;
    status: 'success' | 'running' | 'failed';
    exitCode?: number;
    pid?: number;
    error?: string;
    logFile?: string;
}

interface PipelineStatus {
    stages: Record<string, any>;
    fullPipeline?: ExecutionStatus;
}

async function loadStatus(): Promise<PipelineStatus> {
    const data = await fs.readFile(STATUS_FILE, 'utf-8');
    return JSON.parse(data);
}

async function saveStatus(status: PipelineStatus): Promise<void> {
    await fs.writeFile(STATUS_FILE, JSON.stringify(status, null, 2));
}

export async function POST(request: NextRequest) {
    try {
        const body = await request.json();
        const { stage, task } = body;

        if (!stage) {
            return NextResponse.json({ error: 'Stage is required' }, { status: 400 });
        }

        const status = await loadStatus();
        let targetExecution: ExecutionStatus | undefined;

        // Find the target execution to kill
        if (stage === 'full' && status.fullPipeline) {
            targetExecution = status.fullPipeline;
        } else if (status.stages[stage]) {
            if (task === 'stage') {
                targetExecution = { status: status.stages[stage].status, pid: status.stages[stage].pid } as any;
            } else if (task && status.stages[stage].tasks[task]) {
                targetExecution = status.stages[stage].tasks[task];
            }
        }

        if (!targetExecution || targetExecution.status !== 'running' || !targetExecution.pid) {
            return NextResponse.json({ error: 'No active process found for this task' }, { status: 404 });
        }

        const pid = targetExecution.pid;
        console.log(`Terminating process tree for PID: ${pid}`);

        // Force kill the process tree on Windows
        try {
            await execAsync(`taskkill /F /T /PID ${pid}`);
        } catch (killError) {
            console.error('Kill command error:', killError);
            // Even if taskkill fails (e.g. process already dead), we should update the status
        }

        // Update status file
        const updatedStatus = await loadStatus();
        const endTime = new Date().toISOString();
        const terminationResult: Partial<ExecutionStatus> = {
            endTime,
            status: 'failed',
            error: 'Terminated by user'
        };

        if (stage === 'full' && updatedStatus.fullPipeline) {
            updatedStatus.fullPipeline = { ...updatedStatus.fullPipeline, ...terminationResult };
        } else if (updatedStatus.stages[stage]) {
            if (task === 'stage') {
                updatedStatus.stages[stage].status = 'failed';
                updatedStatus.stages[stage].endTime = endTime;
                updatedStatus.stages[stage].error = 'Terminated by user';
            } else if (task && updatedStatus.stages[stage].tasks[task]) {
                updatedStatus.stages[stage].tasks[task] = { ...updatedStatus.stages[stage].tasks[task], ...terminationResult };
            }
        }

        await saveStatus(updatedStatus);

        return NextResponse.json({
            success: true,
            message: `Process ${pid} terminated successfully`,
            pid
        });

    } catch (error) {
        console.error('Termination error:', error);
        return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
    }
}
