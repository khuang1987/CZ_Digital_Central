'use client';

import React, { useEffect, useState, useMemo } from 'react';
import { Database, HardDrive, List, Activity, RefreshCw, BarChart3, PieChart } from 'lucide-react';
import {
    BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell, Legend
} from 'recharts';

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

    // Derived Data for Charts
    const chartData = useMemo(() => {
        if (!stats) return { bySize: [], byRows: [] };

        const bySize = [...stats.tables]
            .sort((a, b) => b.total_space_mb - a.total_space_mb)
            .slice(0, 10)
            .map(t => ({
                name: t.table_name,
                value: t.total_space_mb,
                schema: t.schema_name
            }));

        const byRows = [...stats.tables]
            .sort((a, b) => b.row_count - a.row_count)
            .slice(0, 10)
            .map(t => ({
                name: t.table_name,
                value: t.row_count,
                schema: t.schema_name
            }));

        return { bySize, byRows };
    }, [stats]);

    if (loading && !stats) {
        return (
            <div className="flex flex-col items-center justify-center h-[60vh] text-slate-400 gap-4">
                <RefreshCw className="animate-spin text-medtronic" size={32} />
                <p className="text-sm font-bold uppercase tracking-widest">Loading Server Health...</p>
            </div>
        );
    }

    if (error) {
        return (
            <div className="flex flex-col items-center justify-center h-[60vh] text-red-500 gap-4">
                <div className="p-4 bg-red-50 dark:bg-red-900/20 rounded-full">
                    <Activity size={32} />
                </div>
                <p className="text-lg font-bold">{error}</p>
                <button
                    onClick={fetchStats}
                    className="px-6 py-2 bg-slate-100 dark:bg-slate-800 rounded-lg text-sm font-bold hover:bg-slate-200 dark:hover:bg-slate-700 transition-colors text-slate-600 dark:text-slate-300"
                >
                    Try Again
                </button>
            </div>
        );
    }

    return (
        <div className="space-y-8 animate-in fade-in duration-500">
            {/* KPI Cards */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <KPICard
                    icon={<List size={22} />}
                    label="Total Tables"
                    value={stats?.summary.total_tables.toLocaleString() || '-'}
                    subtext="Base tables in schema"
                    color="bg-purple-500"
                />
                <KPICard
                    icon={<Activity size={22} />}
                    label="Total Rows"
                    value={(stats?.summary.total_rows || 0).toLocaleString()}
                    subtext="Across all partitions"
                    color="bg-emerald-500"
                />
                <KPICard
                    icon={<HardDrive size={22} />}
                    label="Database Size"
                    value={`${stats?.summary.db_size_mb.toLocaleString()} MB`}
                    subtext="Data + Log files"
                    color="bg-blue-500"
                />
            </div>

            {/* Charts Section */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Size Chart */}
                <div className="ios-widget p-6 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl shadow-sm">
                    <div className="flex items-center justify-between mb-6">
                        <h3 className="text-sm font-bold text-slate-800 dark:text-slate-200 flex items-center gap-2">
                            <PieChart size={16} className="text-medtronic" />
                            Top 10 Tables by Size (MB)
                        </h3>
                    </div>
                    <div className="h-[300px] w-full">
                        <ResponsiveContainer width="100%" height="100%">
                            <BarChart data={chartData.bySize} layout="vertical" margin={{ top: 5, right: 30, left: 40, bottom: 5 }}>
                                <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="#e2e8f0" opacity={0.5} />
                                <XAxis type="number" hide />
                                <YAxis
                                    type="category"
                                    dataKey="name"
                                    tick={{ fontSize: 10, fill: '#64748b', fontWeight: 600 }}
                                    width={100}
                                    tickFormatter={(val) => val.length > 15 ? val.substring(0, 15) + '...' : val}
                                />
                                <Tooltip
                                    cursor={{ fill: '#f1f5f9', opacity: 0.5 }}
                                    contentStyle={{ borderRadius: '12px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                                />
                                <Bar dataKey="value" fill="#3b82f6" radius={[0, 4, 4, 0]} barSize={20}>
                                    {chartData.bySize.map((entry, index) => (
                                        <Cell key={`cell-${index}`} fill={index < 3 ? '#3b82f6' : '#93c5fd'} />
                                    ))}
                                </Bar>
                            </BarChart>
                        </ResponsiveContainer>
                    </div>
                </div>

                {/* Rows Chart */}
                <div className="ios-widget p-6 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl shadow-sm">
                    <div className="flex items-center justify-between mb-6">
                        <h3 className="text-sm font-bold text-slate-800 dark:text-slate-200 flex items-center gap-2">
                            <BarChart3 size={16} className="text-emerald-500" />
                            Top 10 Tables by Rows
                        </h3>
                    </div>
                    <div className="h-[300px] w-full">
                        <ResponsiveContainer width="100%" height="100%">
                            <BarChart data={chartData.byRows} layout="vertical" margin={{ top: 5, right: 30, left: 40, bottom: 5 }}>
                                <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="#e2e8f0" opacity={0.5} />
                                <XAxis type="number" hide />
                                <YAxis
                                    type="category"
                                    dataKey="name"
                                    tick={{ fontSize: 10, fill: '#64748b', fontWeight: 600 }}
                                    width={100}
                                    tickFormatter={(val) => val.length > 15 ? val.substring(0, 15) + '...' : val}
                                />
                                <Tooltip
                                    cursor={{ fill: '#f1f5f9', opacity: 0.5 }}
                                    contentStyle={{ borderRadius: '12px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                                />
                                <Bar dataKey="value" fill="#10b981" radius={[0, 4, 4, 0]} barSize={20}>
                                    {chartData.byRows.map((entry, index) => (
                                        <Cell key={`cell-${index}`} fill={index < 3 ? '#10b981' : '#6ee7b7'} />
                                    ))}
                                </Bar>
                            </BarChart>
                        </ResponsiveContainer>
                    </div>
                </div>
            </div>

            {/* Top Tables Detail Grid */}
            <div className="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm overflow-hidden">
                <div className="px-6 py-5 border-b border-slate-100 dark:border-slate-800 flex items-center justify-between bg-slate-50/50 dark:bg-slate-800/50">
                    <div>
                        <h3 className="text-sm font-black text-slate-800 dark:text-slate-200">System Tables Detail</h3>
                        <p className="text-[10px] uppercase tracking-widest text-slate-400 mt-1">Full Inventory sorted by storage</p>
                    </div>
                </div>
                <div className="overflow-x-auto">
                    <table className="w-full text-sm text-left">
                        <thead className="text-[10px] text-slate-500 uppercase bg-slate-50 dark:bg-slate-800 border-b border-slate-100 dark:border-slate-800 tracking-wider font-bold">
                            <tr>
                                <th className="px-6 py-4">Table Name</th>
                                <th className="px-6 py-4 text-right">Row Count</th>
                                <th className="px-6 py-4 text-right">Total Size (MB)</th>
                                <th className="px-6 py-4 text-right">Used Space (MB)</th>
                                <th className="px-6 py-4 text-right hidden md:table-cell">Last Access</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                            {stats?.tables.map((table, i) => (
                                <tr key={i} className="hover:bg-slate-50/80 dark:hover:bg-slate-800/50 transition-colors group">
                                    <td className="px-6 py-3 font-bold text-slate-700 dark:text-slate-300">
                                        <span className="text-slate-400 font-normal mr-1 text-xs">{table.schema_name}.</span>
                                        {table.table_name}
                                    </td>
                                    <td className="px-6 py-3 text-right font-mono text-xs text-slate-600 dark:text-slate-400 font-bold">
                                        {table.row_count.toLocaleString()}
                                    </td>
                                    <td className="px-6 py-3 text-right font-mono text-xs text-slate-600 dark:text-slate-400 font-bold">
                                        {table.total_space_mb.toLocaleString()}
                                    </td>
                                    <td className="px-6 py-3 text-right font-mono">
                                        <div className="w-full bg-slate-100 dark:bg-slate-800 rounded-full h-1.5 mt-2 relative overflow-hidden">
                                            <div
                                                className="absolute left-0 top-0 h-full bg-blue-500 group-hover:bg-medtronic transition-colors rounded-full"
                                                style={{ width: `${Math.min((table.used_space_mb / table.total_space_mb) * 100, 100)}%` }}
                                            />
                                        </div>
                                        <div className="text-[9px] text-slate-400 mt-1 font-semibold text-right">
                                            {table.used_space_mb.toLocaleString()} used
                                        </div>
                                    </td>
                                    <td className="px-6 py-3 text-right text-xs text-slate-400 font-medium hidden md:table-cell">
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
        <div className="bg-white dark:bg-slate-900 p-6 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm hover:shadow-lg hover:translate-y-[-2px] transition-all cursor-default group relative overflow-hidden">
            <div className={`absolute top-0 right-0 w-20 h-20 rounded-bl-full opacity-10 ${color} -mr-10 -mt-10 transition-transform group-hover:scale-150`} />

            <div className="flex items-start justify-between relative z-10">
                <div>
                    <p className="text-[10px] font-black uppercase tracking-widest text-slate-400 mb-2">{label}</p>
                    <h3 className="text-3xl font-black text-slate-800 dark:text-white tracking-tighter">{value}</h3>
                    <p className="text-[10px] font-bold text-slate-400 mt-2 flex items-center gap-1">
                        <span className="w-1.5 h-1.5 rounded-full bg-slate-300 inline-block" />
                        {subtext}
                    </p>
                </div>
                <div className={`p-3 rounded-xl text-white shadow-lg ${color} bg-opacity-90 group-hover:scale-110 transition-transform`}>
                    {icon}
                </div>
            </div>
        </div>
    );
}
