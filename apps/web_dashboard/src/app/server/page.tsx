
'use client';

import React, { useEffect, useState } from 'react';
import { Database, HardDrive, List, Activity, RefreshCw } from 'lucide-react';

interface ServerStats {
    summary: {
        total_tables: number;
        total_rows: number;
        db_size_mb: number;
    };
    tables: {
        table_name: string;
        schema_name: string;
        row_count: number;
        total_space_mb: number;
        used_space_mb: number;
        last_access: string | null;
    }[];
}

export default function ServerPage() {
    const [stats, setStats] = useState<ServerStats | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    const fetchStats = async () => {
        setLoading(true);
        try {
            const res = await fetch('/api/server/stats');
            if (!res.ok) throw new Error('Failed to fetch data');
            const data = await res.json();
            setStats(data);
            setError(null);
        } catch (err) {
            setError('Failed to load server statistics');
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchStats();
    }, []);

    if (loading && !stats) {
        return <div className="p-8 text-slate-500 flex items-center gap-2"><RefreshCw className="animate-spin" /> Loading Server Health...</div>;
    }

    if (error) {
        return <div className="p-8 text-red-500 flex items-center gap-2"><Activity /> {error}</div>;
    }

    return (
        <div className="p-6 max-w-[1600px] mx-auto space-y-6">
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-bold text-slate-800 dark:text-slate-100 flex items-center gap-2">
                        <Database className="text-medtronic" />
                        Server Health Monitor
                    </h1>
                    <p className="text-slate-500 dark:text-slate-400 mt-1">Real-time status of the SQL Server database.</p>
                </div>
                <button
                    onClick={fetchStats}
                    className="p-2 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-full transition-colors"
                    title="Refresh Data"
                >
                    <RefreshCw size={20} className={loading ? 'animate-spin text-medtronic' : 'text-slate-500'} />
                </button>
            </div>

            {/* KPI Cards */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <KPICard
                    icon={<List size={24} />}
                    label="Total Tables"
                    value={stats?.summary.total_tables.toLocaleString() || '-'}
                    subtext="Base tables in schema"
                    color="bg-blue-500"
                />
                <KPICard
                    icon={<Activity size={24} />}
                    label="Total Rows"
                    value={(stats?.summary.total_rows || 0).toLocaleString()}
                    subtext="Across all partitions"
                    color="bg-green-500"
                />
                <KPICard
                    icon={<HardDrive size={24} />}
                    label="Database Size"
                    value={`${stats?.summary.db_size_mb.toLocaleString()} MB`}
                    subtext="Data + Log files"
                    color="bg-purple-500"
                />
            </div>

            {/* Top Tables Grid */}
            <div className="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-800 shadow-sm overflow-hidden">
                <div className="px-6 py-4 border-b border-slate-100 dark:border-slate-800 flex items-center justify-between">
                    <h3 className="font-semibold text-slate-800 dark:text-slate-200">Top 20 Tables by Size & Volume</h3>
                    <span className="text-xs text-slate-400">Sorted by Space Usage</span>
                </div>
                <div className="overflow-x-auto">
                    <table className="w-full text-sm text-left">
                        <thead className="text-xs text-slate-500 uppercase bg-slate-50/50 dark:bg-slate-800/50 border-b border-slate-100 dark:border-slate-800">
                            <tr>
                                <th className="px-6 py-3 font-medium">Table Name</th>
                                <th className="px-6 py-3 font-medium text-right">Row Count</th>
                                <th className="px-6 py-3 font-medium text-right">Total Size (MB)</th>
                                <th className="px-6 py-3 font-medium text-right">Used Space (MB)</th>
                                <th className="px-6 py-3 font-medium text-right hidden md:table-cell">Last Access</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                            {stats?.tables.map((table, i) => (
                                <tr key={i} className="hover:bg-slate-50/50 dark:hover:bg-slate-800/50 transition-colors">
                                    <td className="px-6 py-3 font-medium text-slate-700 dark:text-slate-300">
                                        <span className="text-slate-400 mr-1">{table.schema_name}.</span>
                                        {table.table_name}
                                    </td>
                                    <td className="px-6 py-3 text-right font-mono text-slate-600 dark:text-slate-400">
                                        {table.row_count.toLocaleString()}
                                    </td>
                                    <td className="px-6 py-3 text-right font-mono text-slate-600 dark:text-slate-400">
                                        {table.total_space_mb.toLocaleString()}
                                    </td>
                                    <td className="px-6 py-3 text-right font-mono">
                                        <div className="w-full bg-slate-100 dark:bg-slate-800 rounded-full h-1.5 mt-2 relative overflow-hidden">
                                            <div
                                                className="absolute left-0 top-0 h-full bg-blue-500 rounded-full"
                                                style={{ width: `${Math.min((table.used_space_mb / table.total_space_mb) * 100, 100)}%` }}
                                            />
                                        </div>
                                        <div className="text-[10px] text-slate-400 mt-1">
                                            {table.used_space_mb.toLocaleString()} used
                                        </div>
                                    </td>
                                    <td className="px-6 py-3 text-right text-slate-400 hidden md:table-cell">
                                        {table.last_access ? new Date(table.last_access).toLocaleDateString() : '-'}
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    );
}

function KPICard({ icon, label, value, subtext, color }: { icon: React.ReactNode, label: string, value: string, subtext: string, color: string }) {
    return (
        <div className="bg-white dark:bg-slate-900 p-6 rounded-xl border border-slate-200 dark:border-slate-800 shadow-sm hover:shadow-md transition-shadow">
            <div className="flex items-start justify-between">
                <div>
                    <p className="text-sm font-medium text-slate-500 mb-1">{label}</p>
                    <h3 className="text-3xl font-bold text-slate-800 dark:text-slate-100 tracking-tight">{value}</h3>
                    <p className="text-xs text-slate-400 mt-2">{subtext}</p>
                </div>
                <div className={`p-3 rounded-lg text-white shadow-lg shadow-blue-500/10 ${color}`}>
                    {icon}
                </div>
            </div>
        </div>
    );
}
