'use client';
import { useEffect, useState } from 'react';
import StandardPageLayout, { PageTab } from '@/components/StandardPageLayout';
import { Settings, Server, Users, Shield, Zap, Activity, Database, FileBox, Play, Clock, CheckCircle2, XCircle, Loader2, AlertCircle, Lock, KeyRound, Globe } from 'lucide-react';

interface ExecutionStatus {
    startTime: string;
    endTime?: string;
    status: 'success' | 'running' | 'failed' | 'skipped';
    exitCode?: number;
    pid?: number;
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
    tasks?: TaskStatus;
}

interface PipelineStatus {
    stages: {
        ingestion: StageStatus & { tasks: TaskStatus };
        cleaning: StageStatus;
        output: StageStatus & { tasks: TaskStatus };
        reports: StageStatus & { tasks: TaskStatus };
    };
    fullPipeline?: ExecutionStatus;
}

export default function Page() {
    const [status, setStatus] = useState<PipelineStatus | null>(null);
    const [loading, setLoading] = useState(false);
    const [executing, setExecuting] = useState<string | null>(null);
    const [activeLog, setActiveLog] = useState<{ file: string; title: string; taskKey?: string } | null>(null);
    const [logContent, setLogContent] = useState<string>('');
    const [logPollingActive, setLogPollingActive] = useState(false);
    const [countdown, setCountdown] = useState<number | null>(null);
    const [sqlInfo, setSqlInfo] = useState<{ status: 'online' | 'offline'; server?: string; database?: string; error?: string } | null>(null);

    // Auth State
    const [isUnlocked, setIsUnlocked] = useState(false);
    const [password, setPassword] = useState('');
    const [showError, setShowError] = useState(false);
    const [isCheckingSession, setIsCheckingSession] = useState(true);

    const tabs: PageTab[] = [
        { label: '常规', href: '/settings', icon: <Settings size={14} />, active: true },
        { label: '服务器', href: '/server', icon: <Server size={14} /> },
        { label: '用户', href: '/settings/users', icon: <Users size={14} />, disabled: true },
        { label: '权限', href: '/settings/permissions', icon: <Shield size={14} />, disabled: true },
    ];

    // Check session storage for existing auth
    useEffect(() => {
        const auth = sessionStorage.getItem('settings_unlocked');
        if (auth === 'true') {
            setIsUnlocked(true);
        }
        setIsCheckingSession(false);

        fetchStatus();
        fetchSqlStatus();
        const interval = setInterval(() => {
            fetchStatus();
            fetchSqlStatus();
        }, 5000); // Poll every 5 seconds
        return () => clearInterval(interval);
    }, []);

    // Fetch logs if polling is active
    useEffect(() => {
        let interval: NodeJS.Timeout;
        if (activeLog) {
            fetchLogs();
            if (logPollingActive) {
                interval = setInterval(fetchLogs, 2000);
            }
        }
        return () => clearInterval(interval);
    }, [activeLog, logPollingActive]);

    // Countdown Effect
    useEffect(() => {
        let timer: NodeJS.Timeout;
        if (countdown !== null && countdown > 0) {
            timer = setTimeout(() => setCountdown(countdown - 1), 1000);
        } else if (countdown === 0) {
            setActiveLog(null);
            setCountdown(null);
        }
        return () => clearTimeout(timer);
    }, [countdown]);

    async function fetchStatus() {
        try {
            const res = await fetch('/api/pipeline/status');
            const data: PipelineStatus = await res.json();
            setStatus(data);

            // Handle countdown if active log task finished
            if (activeLog?.taskKey && countdown === null) {
                const parts = activeLog.taskKey.split('-');
                let taskStatus: ExecutionStatus | undefined;

                if (parts[0] === 'full') {
                    taskStatus = data.fullPipeline;
                } else if (parts.length === 2) {
                    const [stage, task] = parts;
                    const s = data.stages[stage as keyof typeof data.stages];
                    if (task === 'stage') {
                        taskStatus = {
                            status: s.status as any,
                            startTime: s.startTime!,
                            endTime: s.endTime
                        };
                    } else {
                        taskStatus = (s as any).tasks?.[task];
                    }
                }

                // Auto-close countdown trigger:
                // 1. Task must be in a terminal state (success, failed, skipped)
                // 2. We are either actively polling (meaning it just finished) OR it's already finished and we haven't started countdown yet
                const isTerminal = taskStatus && (taskStatus.status === 'success' || taskStatus.status === 'failed' || taskStatus.status === 'skipped');

                if (isTerminal && countdown === null) {
                    setLogPollingActive(false);
                    setCountdown(10);
                }
            }
        } catch (error) {
            console.error('Failed to fetch pipeline status:', error);
        }
    }

    async function fetchSqlStatus() {
        try {
            const res = await fetch('/api/server/check-connection');
            const data = await res.json();
            setSqlInfo(data);
        } catch (error) {
            console.error('Failed to fetch SQL status:', error);
            setSqlInfo({ status: 'offline', error: 'Connection failed' });
        }
    }

    async function fetchLogs() {
        if (!activeLog) return;
        try {
            const res = await fetch(`/api/pipeline/logs?file=${activeLog.file}`);
            const data = await res.json();
            if (data.content) {
                setLogContent(data.content);
            }
        } catch (error) {
            console.error('Failed to fetch logs:', error);
        }
    }

    function calculateDuration(start?: string, end?: string) {
        if (!start) return null;
        const s = new Date(start).getTime();
        const e = end ? new Date(end).getTime() : new Date().getTime();
        const diff = Math.floor((e - s) / 1000);
        const mins = Math.floor(diff / 60);
        const secs = diff % 60;
        return mins > 0 ? `${mins}m ${secs}s` : `${secs}s`;
    }

    async function executeTask(stage: string, task?: string) {
        const key = task ? `${stage}-${task}` : stage;

        // INTERLOCK: Check if already running in global status
        const stageData = status?.stages?.[stage as keyof typeof status.stages];
        const currentExecution = (task === 'stage' || !task)
            ? { status: stageData?.status, logFile: stageData?.logFile }
            : (stageData as any)?.tasks?.[task];

        if (currentExecution?.status === 'running' && currentExecution.logFile) {
            setActiveLog({
                file: currentExecution.logFile,
                title: `Active Logs: ${key}`,
                taskKey: (task === 'stage' || !task) ? `${stage}-stage` : key
            });
            setLogPollingActive(true);
            return;
        }

        setExecuting(key);
        setLoading(true);
        setCountdown(null); // Clear any existing countdown
        try {
            const res = await fetch('/api/pipeline/execute', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ stage, task }),
            });
            const data = await res.json();
            if (data.success) {
                if (data.logFile) {
                    setActiveLog({ file: data.logFile, title: `Logs: ${key}`, taskKey: key === stage ? `${stage}-stage` : key });
                    setLogPollingActive(true);
                }
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

    async function terminateTask(stage: string, task?: string) {
        const key = task ? `${stage}-${task}` : stage;
        if (!confirm(`确定要强制停止任务 ${labelMap[key] || key} 吗？\n这将结束所有相关后台进程（包括浏览器窗口）。`)) return;

        setLoading(true);
        try {
            const res = await fetch('/api/pipeline/terminate', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ stage, task }),
            });
            const data = await res.json();
            if (data.success) {
                await fetchStatus();
                if (activeLog?.taskKey === (task === 'stage' || !task ? `${stage}-stage` : key)) {
                    setLogPollingActive(false);
                }
            } else {
                alert(`停止失败: ${data.error}`);
            }
        } catch (error) {
            console.error('Termination error:', error);
            alert('操作失败，请重试');
        } finally {
            setLoading(false);
        }
    }

    const labelMap: Record<string, string> = {
        'full': '全流水线',
        'ingestion-planner': 'Planner 采集',
        'ingestion-cmes': 'CMES 采集',
        'ingestion-labor': '工时采集',
        'cleaning-sfc': 'SFC 清洗',
        'cleaning-mes': 'MES 清洗',
        'cleaning-sap': 'SAP 清洗',
        'cleaning-others': '基础资料清洗',
        'output-parquet': 'Parquet 导出',
        'output-validation': '数据校验',
        'reports-powerbi': 'PBI 刷新',
    };

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

    const handleUnlock = (e: React.FormEvent) => {
        e.preventDefault();
        if (password === '1212') {
            setIsUnlocked(true);
            sessionStorage.setItem('settings_unlocked', 'true');
            setShowError(false);
        } else {
            setShowError(true);
            setPassword('');
            setTimeout(() => setShowError(false), 3000);
        }
    };

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
        if (status === 'skipped') {
            return (
                <div className="flex items-center gap-1 text-[10px] font-bold text-slate-500 dark:text-slate-400">
                    <Clock size={10} />
                    跳过
                </div>
            );
        }
        return null;
    }

    const TaskButton = ({ stage, task, label, isExecuting }: { stage: string; task: string; label: string; isExecuting: boolean }) => {
        const stageData = status?.stages?.[stage as keyof typeof status.stages];
        const taskStatus = (stageData as any)?.tasks?.[task];
        const isActuallyRunning = taskStatus?.status === 'running';

        return (
            <div
                onClick={() => !loading && executeTask(stage, task)}
                className={`group relative flex flex-col items-start px-4 py-3 bg-white dark:bg-slate-900 border rounded-xl transition-all duration-300 ${isExecuting || isActuallyRunning
                    ? 'border-blue-500 shadow-lg shadow-blue-500/10'
                    : 'border-slate-200 dark:border-slate-800 hover:border-blue-500/50 hover:shadow-md cursor-pointer'
                    }`}
            >
                <div className="flex items-center justify-between w-full mb-2">
                    <span className="text-[11px] font-black text-slate-800 dark:text-slate-100 uppercase tracking-widest">{label}</span>
                    <div className="flex items-center gap-2">
                        {isActuallyRunning && (
                            <button
                                onClick={(e) => {
                                    e.stopPropagation();
                                    terminateTask(stage, task);
                                }}
                                className="p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-900/20 rounded transition-colors"
                                title="强制停止任务"
                            >
                                <XCircle size={12} />
                            </button>
                        )}
                        {isActuallyRunning || isExecuting ? (
                            <div className="flex items-center gap-1.5">
                                <span className="text-[9px] font-bold text-blue-500 animate-pulse">RUNNING</span>
                                <Loader2 size={12} className="animate-spin text-blue-500" />
                            </div>
                        ) : (
                            getStatusBadge(taskStatus)
                        )}
                    </div>
                </div>

                <div className="flex items-center justify-between w-full h-5">
                    {taskStatus ? (
                        <div className="flex items-center gap-1 text-[10px] text-slate-400 font-bold">
                            <Clock size={10} />
                            {formatTime(taskStatus.startTime)}
                            {taskStatus.pid && <span className="text-[9px] opacity-40 ml-1">PID: {taskStatus.pid}</span>}
                        </div>
                    ) : (
                        <div className="text-[10px] text-slate-300 dark:text-slate-700 font-bold uppercase tracking-widest italic">
                            READY
                        </div>
                    )}

                    {(isActuallyRunning || taskStatus?.logFile) && (
                        <button
                            onClick={(e) => {
                                e.stopPropagation();
                                setActiveLog({ file: taskStatus?.logFile || '', title: `Logs: ${label}`, taskKey: `${stage}-${task}` });
                                setLogPollingActive(isActuallyRunning);
                                setCountdown(null);
                            }}
                            className="flex items-center gap-1 text-[9px] font-black text-slate-400 hover:text-blue-500 transition-colors uppercase tracking-widest"
                            title="View Logs"
                        >
                            <FileBox size={10} />
                            LOG
                        </button>
                    )}
                </div>
            </div>
        );
    };

    return (
        <StandardPageLayout
            title="系统设置"
            description="配置应用程序首选项、权限及后端流水线。"
            icon={<Settings size={24} />}
            tabs={tabs}
        >
            {!isUnlocked && !isCheckingSession ? (
                <div className="flex-1 flex flex-col items-center justify-center p-6 bg-slate-50 dark:bg-[#020617] animate-in fade-in zoom-in-95 duration-500">
                    <div className="w-full max-w-md bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-3xl p-8 shadow-xl relative overflow-hidden">
                        {/* Decorative Background Element */}
                        <div className="absolute top-0 right-0 w-32 h-32 bg-blue-500/5 blur-[80px] rounded-full" />
                        <div className="absolute bottom-0 left-0 w-32 h-32 bg-purple-500/5 blur-[80px] rounded-full" />

                        <div className="relative z-10 text-center space-y-6">
                            <div className="mx-auto w-16 h-16 bg-blue-50 dark:bg-blue-900/20 rounded-2xl flex items-center justify-center text-blue-500 dark:text-blue-400 mb-2">
                                <Lock size={32} />
                            </div>

                            <div className="space-y-2">
                                <h2 className="text-2xl font-black text-slate-900 dark:text-white uppercase tracking-tight">Protected Area</h2>
                                <p className="text-sm text-slate-500 font-medium">Please enter the security password to access system settings.</p>
                            </div>

                            <form onSubmit={handleUnlock} className="space-y-4">
                                <div className="relative group">
                                    <div className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-400 group-focus-within:text-blue-500 transition-colors">
                                        <KeyRound size={18} />
                                    </div>
                                    <input
                                        type="password"
                                        value={password}
                                        onChange={(e) => setPassword(e.target.value)}
                                        placeholder="Enter Password"
                                        autoFocus
                                        className={`w-full pl-12 pr-4 py-4 bg-slate-50 dark:bg-slate-800 border-2 rounded-2xl outline-none transition-all font-black tracking-[0.5em] text-center text-lg ${showError
                                            ? 'border-red-500 text-red-500 animate-shake'
                                            : 'border-transparent focus:border-blue-500/50 focus:bg-white dark:focus:bg-slate-900'
                                            }`}
                                    />
                                </div>
                                {showError && (
                                    <p className="text-xs font-black text-red-500 uppercase tracking-widest animate-in slide-in-from-top-2">
                                        Access Denied: Incorrect Password
                                    </p>
                                )}
                                <button
                                    type="submit"
                                    className="w-full py-4 bg-blue-500 hover:bg-blue-600 text-white rounded-2xl font-black uppercase tracking-widest shadow-lg shadow-blue-500/20 active:scale-[0.98] transition-all flex items-center justify-center gap-2"
                                >
                                    Unlock Settings
                                </button>
                            </form>
                        </div>
                    </div>
                    <p className="mt-8 text-[10px] text-slate-400 font-bold uppercase tracking-[0.2em]">Sensitive Operations Authorization Required</p>
                </div>
            ) : isCheckingSession ? (
                <div className="flex-1 flex items-center justify-center">
                    <Loader2 className="animate-spin text-blue-500" size={32} />
                </div>
            ) : (
                <div className="h-full flex flex-col gap-6 p-6 animate-in fade-in duration-500">
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
                                            <div className="flex flex-col items-center gap-1">
                                                <div className="flex items-center gap-2 text-slate-500 font-bold">
                                                    <Clock size={12} className="text-slate-400" />
                                                    Start: {new Date(status.fullPipeline.startTime).toLocaleTimeString()}
                                                </div>
                                                {status.fullPipeline.endTime && (
                                                    <div className="text-[10px] text-slate-400">
                                                        Duration: {calculateDuration(status.fullPipeline.startTime, status.fullPipeline.endTime)}
                                                    </div>
                                                )}
                                            </div>
                                            <div className="flex items-center justify-center gap-2">
                                                {getStatusBadge(status.fullPipeline)}
                                                {status.fullPipeline.logFile && (
                                                    <button
                                                        onClick={() => {
                                                            setActiveLog({
                                                                file: status.fullPipeline!.logFile!,
                                                                title: 'Full Pipeline Log',
                                                                taskKey: 'full'
                                                            });
                                                            setLogPollingActive(status.fullPipeline?.status === 'running');
                                                        }}
                                                        className="px-2 py-0.5 bg-slate-100 dark:bg-slate-800 rounded text-[9px] hover:bg-slate-200"
                                                    >
                                                        View Log
                                                    </button>
                                                )}
                                            </div>
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
                                        {status?.stages?.ingestion?.startTime && (
                                            <div className="flex items-center gap-1 text-[9px] text-slate-400 mt-1">
                                                <Clock size={9} />
                                                {formatTime(status.stages.ingestion.startTime)}
                                            </div>
                                        )}
                                    </div>

                                    <div className="flex-1 w-full grid grid-cols-1 md:grid-cols-3 gap-3">
                                        <TaskButton stage="ingestion" task="planner" label="Planner" isExecuting={executing === 'ingestion-planner'} />
                                        <TaskButton stage="ingestion" task="cmes" label="CMES / MES" isExecuting={executing === 'ingestion-cmes'} />
                                        <TaskButton stage="ingestion" task="labor" label="Labor Records" isExecuting={executing === 'ingestion-labor'} />
                                    </div>

                                    <div className="flex flex-col gap-2">
                                        <button
                                            onClick={() => executeTask('ingestion', 'stage')}
                                            disabled={loading || executing?.startsWith('ingestion')}
                                            className="shrink-0 px-4 py-2 bg-blue-500 text-white rounded-lg text-xs font-black uppercase tracking-widest hover:bg-blue-600 transition-colors disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2 justify-center"
                                        >
                                            {executing === 'ingestion-stage' && <Loader2 size={14} className="animate-spin" />}
                                            Run Stage
                                        </button>
                                        {status?.stages?.ingestion?.status && (
                                            <div className="flex items-center justify-between text-[9px] font-bold">
                                                <span className={status.stages.ingestion.status === 'success' ? 'text-emerald-500' : 'text-blue-500'}>
                                                    {status.stages.ingestion.status.toUpperCase()}
                                                </span>
                                                <span className="text-slate-400">{calculateDuration(status.stages.ingestion.startTime, status.stages.ingestion.endTime)}</span>
                                            </div>
                                        )}
                                    </div>
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
                                        {status?.stages?.cleaning?.startTime && (
                                            <div className="flex items-center gap-1 text-[9px] text-slate-400 mt-1">
                                                <Clock size={9} />
                                                Active: {formatTime(status.stages.cleaning.startTime)}
                                            </div>
                                        )}
                                    </div>

                                    <div className="flex-1 w-full grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-3">
                                        <TaskButton stage="cleaning" task="sfc" label="SFC Processing" isExecuting={executing === 'cleaning-sfc'} />
                                        <TaskButton stage="cleaning" task="mes" label="MES Processing" isExecuting={executing === 'cleaning-mes'} />
                                        <TaskButton stage="cleaning" task="sap" label="SAP Processing" isExecuting={executing === 'cleaning-sap'} />
                                        <TaskButton stage="cleaning" task="others" label="Dims & Others" isExecuting={executing === 'cleaning-others'} />
                                    </div>

                                    <div className="flex flex-col gap-2">
                                        <button
                                            onClick={() => executeTask('cleaning', 'stage')}
                                            disabled={loading || executing?.startsWith('cleaning')}
                                            className="shrink-0 px-4 py-2 bg-purple-500 text-white rounded-lg text-xs font-black uppercase tracking-widest hover:bg-purple-600 transition-colors disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2 justify-center"
                                        >
                                            {executing === 'cleaning-stage' && <Loader2 size={14} className="animate-spin" />}
                                            Run Stage
                                        </button>

                                        {status?.stages?.cleaning?.status && (
                                            <div className="flex items-center justify-between text-[9px] font-bold">
                                                <span className={status.stages.cleaning.status === 'success' ? 'text-emerald-500' : 'text-purple-500'}>
                                                    {status.stages.cleaning.status.toUpperCase()}
                                                </span>
                                                <span className="text-slate-400">{calculateDuration(status.stages.cleaning.startTime, status.stages.cleaning.endTime)}</span>
                                            </div>
                                        )}
                                    </div>
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
                                        {status?.stages?.output?.startTime && (
                                            <div className="flex items-center gap-1 text-[9px] text-slate-400 mt-1">
                                                <Clock size={9} />
                                                Active: {formatTime(status.stages.output.startTime)}
                                            </div>
                                        )}
                                    </div>

                                    <div className="flex-1 w-full grid grid-cols-1 md:grid-cols-2 gap-3">
                                        <TaskButton stage="output" task="parquet" label="Partitioned Parquet" isExecuting={executing === 'output-parquet'} />
                                        <TaskButton stage="output" task="validation" label="Validation Check" isExecuting={executing === 'output-validation'} />
                                    </div>

                                    <div className="flex flex-col gap-2">
                                        <button
                                            onClick={() => executeTask('output', 'stage')}
                                            disabled={loading || executing?.startsWith('output')}
                                            className="shrink-0 px-4 py-2 bg-orange-500 text-white rounded-lg text-xs font-black uppercase tracking-widest hover:bg-orange-600 transition-colors disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2 justify-center"
                                        >
                                            {executing === 'output-stage' && <Loader2 size={14} className="animate-spin" />}
                                            Run Stage
                                        </button>
                                        {status?.stages?.output?.status && (
                                            <div className="flex items-center justify-between text-[9px] font-bold">
                                                <span className={status.stages.output.status === 'success' ? 'text-emerald-500' : 'text-orange-500'}>
                                                    {status.stages.output.status.toUpperCase()}
                                                </span>
                                                <span className="text-slate-400">{calculateDuration(status.stages.output.startTime, status.stages.output.endTime)}</span>
                                            </div>
                                        )}
                                    </div>
                                </div>
                            </div>

                            {/* Stage 4: Reports */}
                            <div className="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-6 shadow-sm">
                                <div className="flex flex-col md:flex-row gap-6 items-start md:items-center">
                                    <div className="min-w-[200px]">
                                        <h3 className="text-sm font-black text-slate-800 dark:text-white uppercase tracking-widest flex items-center gap-2 mb-1">
                                            <div className="w-2 h-2 rounded-full bg-emerald-500" />
                                            Reports & Dashboards
                                        </h3>
                                        <p className="text-[10px] text-slate-400 font-bold">STAGE 4</p>
                                        {status?.stages?.reports?.startTime && (
                                            <div className="flex items-center gap-1 text-[9px] text-slate-400 mt-1">
                                                <Clock size={9} />
                                                Active: {formatTime(status.stages.reports.startTime)}
                                            </div>
                                        )}
                                    </div>

                                    <div className="flex-1 w-full grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                                        <TaskButton stage="reports" task="powerbi" label="PowerBI Sync/Refresh" isExecuting={executing === 'reports-powerbi'} />

                                        {/* Dashboard Service Info Card */}
                                        <div className="flex flex-col items-start px-4 py-3 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-xl relative overflow-hidden group">
                                            <div className="flex items-center justify-between w-full mb-2">
                                                <span className="text-[11px] font-black text-slate-800 dark:text-slate-100 uppercase tracking-widest">Dashboard Service</span>
                                                <div className="flex items-center gap-1.5 px-2 py-0.5 bg-emerald-500/10 rounded-md">
                                                    <div className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
                                                    <span className="text-[9px] font-black text-emerald-600 uppercase tracking-widest">Online</span>
                                                </div>
                                            </div>

                                            <div className="flex items-center justify-between w-full h-5">
                                                <div className="flex items-center gap-1.5 text-[10px] text-slate-400 font-bold">
                                                    <Globe size={10} className="text-blue-500" />
                                                    {typeof window !== 'undefined' ? window.location.host : 'Detecting...'}
                                                </div>
                                                <div className="text-[9px] text-slate-300 dark:text-slate-700 font-bold uppercase tracking-widest">
                                                    Static Info
                                                </div>
                                            </div>

                                            {/* Decorative background pulse */}
                                            <div className="absolute -right-4 -bottom-4 w-16 h-16 bg-emerald-500/5 rounded-full blur-2xl group-hover:bg-emerald-500/10 transition-colors" />
                                        </div>

                                        {/* SQL Connectivity Card */}
                                        <div className={`flex flex-col items-start px-4 py-3 bg-white dark:bg-slate-900 border rounded-xl relative overflow-hidden group transition-all duration-300 ${sqlInfo?.status === 'offline' ? 'border-red-500/50 shadow-lg shadow-red-500/5' : 'border-slate-200 dark:border-slate-800'}`}>
                                            <div className="flex items-center justify-between w-full mb-2">
                                                <span className="text-[11px] font-black text-slate-800 dark:text-slate-100 uppercase tracking-widest">SQL Server</span>
                                                {sqlInfo?.status === 'online' ? (
                                                    <div className="flex items-center gap-1.5 px-2 py-0.5 bg-emerald-500/10 rounded-md">
                                                        <div className="w-1.5 h-1.5 rounded-full bg-emerald-500" />
                                                        <span className="text-[9px] font-black text-emerald-600 uppercase tracking-widest">Online</span>
                                                    </div>
                                                ) : (
                                                    <div className="flex items-center gap-1.5 px-2 py-0.5 bg-red-500/10 rounded-md">
                                                        <div className="w-1.5 h-1.5 rounded-full bg-red-500 animate-pulse" />
                                                        <span className="text-[9px] font-black text-red-600 uppercase tracking-widest">Offline</span>
                                                    </div>
                                                )}
                                            </div>

                                            <div className="flex items-center justify-between w-full h-5 min-w-0">
                                                <div className="flex items-center gap-1.5 text-[10px] text-slate-400 font-bold truncate">
                                                    <Database size={10} className={sqlInfo?.status === 'online' ? "text-emerald-500" : "text-red-500"} />
                                                    {sqlInfo?.status === 'online' ? (
                                                        <span className="truncate">{sqlInfo.server} / {sqlInfo.database}</span>
                                                    ) : (
                                                        <span className="text-red-500 truncate">{sqlInfo?.error || 'Connection Failed'}</span>
                                                    )}
                                                </div>
                                                {!sqlInfo && <Loader2 size={10} className="animate-spin text-slate-400" />}
                                            </div>

                                            <div className="absolute -right-4 -bottom-4 w-16 h-16 bg-blue-500/5 rounded-full blur-2xl group-hover:bg-blue-500/10 transition-colors" />
                                        </div>
                                    </div>

                                    <div className="flex flex-col gap-2">
                                        <button
                                            onClick={() => executeTask('reports', 'stage')}
                                            disabled={loading || executing?.startsWith('reports')}
                                            className="shrink-0 px-4 py-2 bg-emerald-500 text-white rounded-lg text-xs font-black uppercase tracking-widest hover:bg-emerald-600 transition-colors disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2 justify-center"
                                        >
                                            {executing === 'reports-stage' && <Loader2 size={14} className="animate-spin" />}
                                            Run Stage
                                        </button>
                                        {status?.stages?.reports?.status && (
                                            <div className="flex items-center justify-between text-[9px] font-bold">
                                                <span className={status.stages.reports.status === 'success' ? 'text-emerald-500' : 'text-blue-500'}>
                                                    {status.stages.reports.status.toUpperCase()}
                                                </span>
                                                <span className="text-slate-400">{calculateDuration(status.stages.reports.startTime, status.stages.reports.endTime)}</span>
                                            </div>
                                        )}
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>

                    {/* Log Console Overlay */}
                    {activeLog && (
                        <div className="fixed inset-0 z-[100] flex items-center justify-center p-6 bg-slate-900/40 backdrop-blur-sm animate-in fade-in duration-300">
                            <div className="bg-[#0f172a] border border-slate-700 shadow-2xl rounded-2xl w-full max-w-4xl h-[80vh] flex flex-col overflow-hidden">
                                <div className="p-4 border-b border-slate-800 flex items-center justify-between bg-slate-900/50">
                                    <div className="flex items-center gap-3">
                                        <div className="w-3 h-3 rounded-full bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.5)]" />
                                        <div className="w-3 h-3 rounded-full bg-yellow-500" />
                                        <div className="w-3 h-3 rounded-full bg-green-500" />
                                        <h3 className="ml-4 text-xs font-black text-slate-400 uppercase tracking-widest">{activeLog.title}</h3>
                                    </div>
                                    <div className="flex items-center gap-2">
                                        {countdown !== null ? (
                                            <div className="flex items-center gap-2 px-3 py-1 bg-emerald-500/10 border border-emerald-500/20 rounded-full animate-in fade-in zoom-in duration-300">
                                                <div className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
                                                <span className="text-[10px] font-black text-emerald-500 uppercase tracking-widest">
                                                    Execution Finished - Closing in {countdown}s
                                                </span>
                                                <button
                                                    onClick={() => setCountdown(null)}
                                                    className="ml-2 px-2 py-0.5 bg-emerald-500/20 hover:bg-emerald-500/30 text-emerald-600 rounded text-[9px] transition-colors border border-emerald-500/20"
                                                >
                                                    CANCEL
                                                </button>
                                            </div>
                                        ) : (
                                            logPollingActive && <Loader2 size={14} className="animate-spin text-blue-500" />
                                        )}
                                        <button
                                            onClick={() => {
                                                setActiveLog(null);
                                                setCountdown(null);
                                            }}
                                            className="p-1.5 hover:bg-slate-800 rounded-lg text-slate-400 transition-colors"
                                        >
                                            <XCircle size={18} />
                                        </button>
                                    </div>
                                </div>
                                <div className="flex-1 p-6 font-mono text-[11px] leading-relaxed overflow-y-auto bg-[#020617] text-slate-300 selection:bg-blue-500/30 no-scrollbar">
                                    <pre className="whitespace-pre-wrap break-all">
                                        {logContent || 'Initializing log stream...'}
                                        {logPollingActive && (
                                            <span className="inline-block w-1.5 h-4 bg-blue-500 ml-1 animate-pulse align-middle" />
                                        )}
                                    </pre>
                                </div>
                                <div className="p-3 border-t border-slate-800 bg-slate-900/30 flex items-center justify-between px-6">
                                    <div className="text-[10px] text-slate-500 font-bold">
                                        FILE: {activeLog.file}
                                    </div>
                                    <label className="flex items-center gap-2 cursor-pointer">
                                        <input
                                            type="checkbox"
                                            checked={logPollingActive}
                                            onChange={(e) => setLogPollingActive(e.target.checked)}
                                            className="rounded border-slate-700 bg-slate-800 text-blue-500"
                                        />
                                        <span className="text-[10px] text-slate-400 font-bold uppercase tracking-wider">Auto Scroll / Update</span>
                                    </label>
                                </div>
                            </div>
                        </div>
                    )}

                    {/* Flow Diagram Notice */}
                    <div className="mt-4 p-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-xl flex items-start gap-3">
                        <AlertCircle size={16} className="text-blue-600 dark:text-blue-400 shrink-0 mt-0.5" />
                        <div className="text-xs text-blue-600 dark:text-blue-400">
                            <strong className="font-bold">Data Flow:</strong> Ingestion → Cleaning → Output → PowerBI Refresh → System Ready
                        </div>
                    </div>
                </div>
            )}
        </StandardPageLayout>
    );
}
