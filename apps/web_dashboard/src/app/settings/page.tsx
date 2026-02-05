'use client';
import { useEffect, useState } from 'react';
import StandardPageLayout, { PageTab } from '@/components/StandardPageLayout';
import { Settings, Server, Users, Shield, Zap, Activity, Database, FileBox, Play, Clock, CheckCircle2, XCircle, Loader2, AlertCircle } from 'lucide-react';

interface ExecutionStatus {
    timestamp: string;
    status: 'success' | 'running' | 'failed';
    exitCode?: number;
    error?: string;
}

interface TaskStatus {
    [key: string]: ExecutionStatus | undefined;
}

interface StageStatus {
    lastRun?: string;
    status?: string;
    tasks?: TaskStatus;
}

interface PipelineStatus {
    stages: {
        ingestion: StageStatus & { tasks: TaskStatus };
        cleaning: StageStatus;
        output: StageStatus & { tasks: TaskStatus };
    };
    fullPipeline?: ExecutionStatus;
}

export default function Page() {
    const [status, setStatus] = useState<PipelineStatus | null>(null);
    const [loading, setLoading] = useState(false);
    const [executing, setExecuting] = useState<string | null>(null);

    const tabs: PageTab[] = [
        { label: '常规 (General)', href: '/settings', icon: <Settings size={14} />, active: true },
        { label: '服务器 (Server)', href: '/server', icon: <Server size={14} /> },
        { label: '用户 (Users)', href: '/settings/users', icon: <Users size={14} />, disabled: true },
        { label: '权限 (Permissions)', href: '/settings/permissions', icon: <Shield size={14} />, disabled: true },
    ];

    // Fetch status on mount and periodically
    useEffect(() => {
        fetchStatus();
        const interval = setInterval(fetchStatus, 5000); // Poll every 5 seconds
        return () => clearInterval(interval);
    }, []);

    async function fetchStatus() {
        try {
            const res = await fetch('/api/pipeline/status');
            const data = await res.json();
            setStatus(data);
        } catch (error) {
            console.error('Failed to fetch pipeline status:', error);
        }
    }

    async function executeTask(stage: string, task?: string) {
        setExecuting(task ? `${stage}-${task}` : stage);
        setLoading(true);
        try {
            const res = await fetch('/api/pipeline/execute', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ stage, task }),
            });
            const data = await res.json();
            if (data.success) {
                // Immediately fetch updated status
                await fetchStatus();
            } else {
                alert(`执行失败: ${data.error}`);
            }
        } catch (error) {
            console.error('Execution error:', error);
            alert('执行失败，请查看控制台日志');
        } finally {
            setLoading(false);
            setExecuting(null);
        }
    }

    function formatTime(timestamp?: string) {
        if (!timestamp) return '未运行';
        const date = new Date(timestamp);
        const now = new Date();
        const diff = now.getTime() - date.getTime();
        const minutes = Math.floor(diff / 60000);
        if (minutes < 1) return '刚刚';
        if (minutes < 60) return `${minutes} 分钟前`;
        const hours = Math.floor(minutes / 60);
        if (hours < 24) return `${hours} 小时前`;
        return date.toLocaleString('zh-CN');
    }

    function getStatusBadge(taskStatus?: ExecutionStatus) {
        if (!taskStatus) return null;
        const { status } = taskStatus;
        if (status === 'running') {
            return (
                <div className="flex items-center gap-1 text-[10px] font-bold text-blue-600 dark:text-blue-400">
                    <Loader2 size={10} className="animate-spin" />
                    运行中
                </div>
            );
        }
        if (status === 'success') {
            return (
                <div className="flex items-center gap-1 text-[10px] font-bold text-emerald-600 dark:text-emerald-400">
                    <CheckCircle2 size={10} />
                    成功
                </div>
            );
        }
        if (status === 'failed') {
            return (
                <div className="flex items-center gap-1 text-[10px] font-bold text-red-600 dark:text-red-400">
                    <XCircle size={10} />
                    失败
                </div>
            );
        }
        return null;
    }

    const TaskButton = ({ stage, task, label, isExecuting }: { stage: string; task: string; label: string; isExecuting: boolean }) => {
        const taskStatus = status?.stages?.[stage as keyof typeof status.stages]?.tasks?.[task];
        return (
            <button
                onClick={() => executeTask(stage, task)}
                disabled={loading || isExecuting}
                className="flex flex-col items-start px-4 py-3 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-xs font-bold text-slate-600 dark:text-slate-300 hover:border-blue-500/50 hover:text-blue-600 dark:hover:text-blue-400 transition-all disabled:opacity-50 disabled:cursor-not-allowed group relative"
            >
                <div className="flex items-center justify-between w-full mb-1">
                    <span>{label}</span>
                    {isExecuting && <Loader2 size={12} className="animate-spin text-blue-500" />}
                </div>
                {taskStatus && (
                    <div className="flex items-center justify-between w-full">
                        <div className="flex items-center gap-1 text-[9px] text-slate-400">
                            <Clock size={9} />
                            {formatTime(taskStatus.timestamp)}
                        </div>
                        {getStatusBadge(taskStatus)}
                    </div>
                )}
            </button>
        );
    };

    return (
        <StandardPageLayout
            title="System Setting"
            description="Configure application preferences and permissions."
            icon={<Settings size={24} />}
            tabs={tabs}
        >
            <div className="h-full flex flex-col gap-6 p-6">
                <div className="flex items-center justify-between">
                    <div>
                        <h2 className="text-xl font-black text-slate-900 dark:text-white">Pipeline Console</h2>
                        <p className="text-xs text-slate-500 font-bold uppercase tracking-widest mt-1">Data Processing & Orchestration</p>
                    </div>
                </div>

                <div className="grid grid-cols-12 gap-6 h-full min-h-0">
                    {/* Left Column - Master Controller */}
                    <div className="col-span-12 lg:col-span-4 xl:col-span-3">
                        <div className="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-6 shadow-sm h-full flex flex-col">
                            <h3 className="text-sm font-black text-slate-800 dark:text-white uppercase tracking-widest mb-4 flex items-center gap-2">
                                <Zap className="text-yellow-500" size={16} />
                                Master Controller
                            </h3>

                            <div className="flex-1 flex flex-col items-center justify-center text-center space-y-6">
                                <div className="p-4 bg-slate-50 dark:bg-slate-800/50 rounded-full mb-2">
                                    <Activity size={48} className="text-slate-300 dark:text-slate-600" />
                                </div>
                                <div>
                                    <h4 className="text-lg font-bold text-slate-700 dark:text-slate-200">Full Auto Mode</h4>
                                    <p className="text-xs text-slate-400 mt-2 leading-relaxed max-w-[200px] mx-auto">
                                        Executes all pipeline stages sequentially with automatic error recovery.
                                    </p>
                                </div>
                                {status?.fullPipeline && (
                                    <div className="text-xs space-y-2">
                                        <div className="flex items-center justify-center gap-2">
                                            <Clock size={12} className="text-slate-400" />
                                            <span className="text-slate-500">{formatTime(status.fullPipeline.timestamp)}</span>
                                        </div>
                                        {getStatusBadge(status.fullPipeline)}
                                    </div>
                                )}
                                <button
                                    onClick={() => executeTask('full')}
                                    disabled={loading || executing === 'full'}
                                    className="w-full py-4 bg-emerald-500 hover:bg-emerald-600 text-white rounded-xl font-black uppercase tracking-widest shadow-lg shadow-emerald-500/20 active:scale-[0.98] transition-all flex items-center justify-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed"
                                >
                                    {executing === 'full' ? (
                                        <>
                                            <Loader2 size={18} className="animate-spin" />
                                            Running...
                                        </>
                                    ) : (
                                        <>
                                            <Zap size={18} fill="currentColor" />
                                            Run Pipeline
                                        </>
                                    )}
                                </button>
                            </div>

                            <div className="mt-auto pt-6 border-t border-slate-100 dark:border-slate-800">
                                <div className="flex justify-between items-center text-xs font-bold text-slate-400">
                                    <span>Status</span>
                                    <span className="text-slate-500">
                                        {status?.fullPipeline?.status === 'running' ? 'RUNNING' : 'IDLE'}
                                    </span>
                                </div>
                            </div>
                        </div>
                    </div>

                    {/* Right Column - Stages */}
                    <div className="col-span-12 lg:col-span-8 xl:col-span-9 flex flex-col gap-6">

                        {/* Stage 1: Ingestion */}
                        <div className="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-6 shadow-sm">
                            <div className="flex flex-col md:flex-row gap-6 items-start md:items-center">
                                <div className="min-w-[200px]">
                                    <h3 className="text-sm font-black text-slate-800 dark:text-white uppercase tracking-widest flex items-center gap-2 mb-1">
                                        <div className="w-2 h-2 rounded-full bg-blue-500" />
                                        Data Ingestion
                                    </h3>
                                    <p className="text-[10px] text-slate-400 font-bold">STAGE 1</p>
                                    {status?.stages?.ingestion?.lastRun && (
                                        <div className="flex items-center gap-1 text-[9px] text-slate-400 mt-1">
                                            <Clock size={9} />
                                            {formatTime(status.stages.ingestion.lastRun)}
                                        </div>
                                    )}
                                </div>

                                <div className="flex-1 w-full grid grid-cols-1 md:grid-cols-3 gap-3">
                                    <TaskButton stage="ingestion" task="planner" label="Planner" isExecuting={executing === 'ingestion-planner'} />
                                    <TaskButton stage="ingestion" task="cmes" label="CMES / MES" isExecuting={executing === 'ingestion-cmes'} />
                                    <TaskButton stage="ingestion" task="labor" label="Labor Records" isExecuting={executing === 'ingestion-labor'} />
                                </div>

                                <button
                                    onClick={() => executeTask('ingestion', 'all')}
                                    disabled={loading || executing?.startsWith('ingestion')}
                                    className="shrink-0 px-4 py-2 bg-blue-500 text-white rounded-lg text-xs font-black uppercase tracking-widest hover:bg-blue-600 transition-colors disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
                                >
                                    {executing?.startsWith('ingestion') && <Loader2 size={14} className="animate-spin" />}
                                    Run Stage
                                </button>
                            </div>
                        </div>

                        {/* Stage 2: Cleaning */}
                        <div className="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-6 shadow-sm">
                            <div className="flex flex-col md:flex-row gap-6 items-start md:items-center">
                                <div className="min-w-[200px]">
                                    <h3 className="text-sm font-black text-slate-800 dark:text-white uppercase tracking-widest flex items-center gap-2 mb-1">
                                        <div className="w-2 h-2 rounded-full bg-purple-500" />
                                        Data Cleaning
                                    </h3>
                                    <p className="text-[10px] text-slate-400 font-bold">STAGE 2</p>
                                    {status?.stages?.cleaning?.lastRun && (
                                        <div className="flex items-center gap-1 text-[9px] text-slate-400 mt-1">
                                            <Clock size={9} />
                                            {formatTime(status.stages.cleaning.lastRun)}
                                        </div>
                                    )}
                                </div>

                                <div className="flex-1 w-full grid grid-cols-1 md:grid-cols-3 gap-3">
                                    <div className="px-4 py-3 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-xs font-bold text-slate-600 dark:text-slate-300">
                                        SAP Raw
                                    </div>
                                    <div className="px-4 py-3 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-xs font-bold text-slate-600 dark:text-slate-300">
                                        SFC Batch
                                    </div>
                                    <div className="px-4 py-3 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-xs font-bold text-slate-600 dark:text-slate-300">
                                        MES Batch
                                    </div>
                                </div>

                                <button
                                    onClick={() => executeTask('cleaning', 'all')}
                                    disabled={loading || executing === 'cleaning-all'}
                                    className="shrink-0 px-4 py-2 bg-purple-500 text-white rounded-lg text-xs font-black uppercase tracking-widest hover:bg-purple-600 transition-colors disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
                                >
                                    {executing === 'cleaning-all' && <Loader2 size={14} className="animate-spin" />}
                                    Run Stage
                                </button>
                            </div>
                        </div>

                        {/* Stage 3: Output */}
                        <div className="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-6 shadow-sm">
                            <div className="flex flex-col md:flex-row gap-6 items-start md:items-center">
                                <div className="min-w-[200px]">
                                    <h3 className="text-sm font-black text-slate-800 dark:text-white uppercase tracking-widest flex items-center gap-2 mb-1">
                                        <div className="w-2 h-2 rounded-full bg-orange-500" />
                                        Output Gen
                                    </h3>
                                    <p className="text-[10px] text-slate-400 font-bold">STAGE 3</p>
                                    {status?.stages?.output?.lastRun && (
                                        <div className="flex items-center gap-1 text-[9px] text-slate-400 mt-1">
                                            <Clock size={9} />
                                            {formatTime(status.stages.output.lastRun)}
                                        </div>
                                    )}
                                </div>

                                <div className="flex-1 w-full grid grid-cols-1 md:grid-cols-2 gap-3">
                                    <TaskButton stage="output" task="parquet" label="Partitioned Parquet" isExecuting={executing === 'output-parquet'} />
                                    <TaskButton stage="output" task="validation" label="Validation Check" isExecuting={executing === 'output-validation'} />
                                </div>

                                <button className="shrink-0 px-4 py-2 bg-orange-500 text-white rounded-lg text-xs font-black uppercase tracking-widest hover:bg-orange-600 transition-colors opacity-50 cursor-not-allowed">
                                    Run Stage
                                </button>
                            </div>
                        </div>

                    </div>
                </div>

                {/* Flow Diagram Notice */}
                <div className="mt-4 p-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-xl flex items-start gap-3">
                    <AlertCircle size={16} className="text-blue-600 dark:text-blue-400 shrink-0 mt-0.5" />
                    <div className="text-xs text-blue-600 dark:text-blue-400">
                        <strong className="font-bold">Data Flow:</strong> Ingestion → Cleaning → Output → Database Update → PowerBI Refresh
                    </div>
                </div>
            </div>
        </StandardPageLayout>
    );
}
