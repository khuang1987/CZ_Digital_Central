'use client';

import React, { useState, useEffect } from 'react';
import {
    Calendar, Filter, RefreshCw, BarChart3, Flame, Clock, Table as TableIcon, Box, MoreHorizontal, AlertCircle, Loader2
} from 'lucide-react';
import { useUI } from '@/context/UIContext';

// --- Types ---
interface CalendarData {
    years: string[];
    months: Record<string, string[]>;
    weeks: Record<string, number[]>;
}

interface LaborData {
    summary: {
        avgHours: number | null;
        totalEH: number | null;
        daysCount: number | null;
    };
    trend: Array<{
        PostingDate: string;
        dailyEH: number;
    }>;
    distribution: Array<{
        area: string | null;
        areaEH: number;
    }>;
    details: Array<{
        PostingDate: string;
        OrderNumber: string;
        eh: number;
        Operation: string;
        Material: string;
    }>;
}

export default function LaborEhPage() {
    // --- Calendar State ---
    const [calendar, setCalendar] = useState<CalendarData | null>(null);
    const [selectedYear, setSelectedYear] = useState('FY26');
    const [selectedMonth, setSelectedMonth] = useState('');
    const [selectedWeek, setSelectedWeek] = useState<number | null>(null);

    // --- Data State ---
    const [data, setData] = useState<LaborData | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const { isFilterOpen } = useUI();

    // 1. Initial Load: Fetch Calendar Options
    useEffect(() => {
        async function fetchCalendar() {
            try {
                const res = await fetch('/api/production/calendar');
                if (!res.ok) {
                    const errorJson = await res.json();
                    throw new Error(errorJson.error || 'Failed to fetch calendar');
                }
                const json = await res.json();
                setCalendar(json);

                // Set defaults if data exists
                if (json.years && json.years.length > 0) {
                    const latestYear = json.years[0];
                    setSelectedYear(latestYear);
                    const firstMonth = json.months[latestYear]?.[0] || '';
                    setSelectedMonth(firstMonth);
                    const firstWeek = json.weeks[firstMonth]?.[0] || null;
                    setSelectedWeek(firstWeek);
                }
            } catch (err: any) {
                console.error("Calendar fetch error:", err);
                setError(`Calendar Error: ${err.message}`);
            }
        }
        fetchCalendar();
    }, []);

    // 2. Fetch Report Data when filters change
    useEffect(() => {
        if (!selectedWeek) return;

        async function fetchData() {
            setLoading(true);
            setError(null);
            try {
                const res = await fetch(`/api/production/labor-eh?week=${selectedWeek}&year=${selectedYear}`);
                if (!res.ok) throw new Error('Failed to fetch SAP data');
                const json = await res.json();
                setData(json);
            } catch (err: any) {
                console.error(err);
                setError(err.message);
            } finally {
                setLoading(false);
            }
        }
        fetchData();
    }, [selectedYear, selectedWeek]);

    const handleMonthChange = (month: string) => {
        setSelectedMonth(month);
        if (calendar?.weeks[month]) {
            setSelectedWeek(calendar.weeks[month][0]);
        }
    };

    return (
        <div className="flex w-full h-full overflow-hidden bg-white dark:bg-transparent">
            {/* 报表内次级筛选器 */}
            <aside className={`${isFilterOpen ? 'w-72 opacity-100' : 'w-0 opacity-0 overflow-hidden'} border-r border-slate-200 dark:border-slate-800 bg-slate-50/80 dark:bg-slate-900/30 backdrop-blur-sm flex flex-col transition-all duration-300 shrink-0`}>
                <div className="p-6 space-y-8 w-72">
                    <section>
                        <h3 className="text-[10px] font-bold text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-4 flex items-center gap-2">
                            <Calendar size={12} /> 财历周期选择
                        </h3>
                        <div className="space-y-5">
                            <div>
                                <label className="text-[11px] font-bold text-slate-700 dark:text-slate-300 mb-2 block px-1">财年 / Fiscal Year</label>
                                <select
                                    value={selectedYear}
                                    onChange={(e) => setSelectedYear(e.target.value)}
                                    className="w-full bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl px-3 py-2 text-xs outline-none focus:ring-2 focus:ring-medtronic text-slate-800 dark:text-slate-200 shadow-sm transition-all focus:border-medtronic"
                                >
                                    {calendar?.years?.map(y => <option key={y} value={y}>{y}</option>)}
                                    {!calendar && <option>Loading...</option>}
                                </select>
                            </div>
                            <div>
                                <label className="text-[11px] font-bold text-slate-700 dark:text-slate-300 mb-2 block px-1">财月 / Fiscal Month</label>
                                <select
                                    value={selectedMonth}
                                    onChange={(e) => handleMonthChange(e.target.value)}
                                    className="w-full bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl px-3 py-2 text-xs outline-none focus:ring-2 focus:ring-medtronic text-slate-800 dark:text-slate-200 shadow-sm transition-all focus:border-medtronic"
                                >
                                    {calendar?.months?.[selectedYear]?.map(m => <option key={m} value={m}>{m}</option>)}
                                    {!calendar && <option>Loading...</option>}
                                </select>
                            </div>
                            <div>
                                <label className="text-[11px] font-bold text-slate-700 dark:text-slate-300 mb-2 block px-1">财周 / Week</label>
                                <div className="grid grid-cols-4 gap-2">
                                    {calendar?.weeks?.[selectedMonth]?.map(wk => (
                                        <button
                                            key={wk}
                                            onClick={() => setSelectedWeek(wk)}
                                            className={`py-2 rounded-xl text-[11px] font-black transition-all shadow-sm ${selectedWeek === wk
                                                ? 'ios-widget-active'
                                                : 'bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 text-slate-600 dark:text-slate-400 hover:border-medtronic hover:text-medtronic'
                                                }`}
                                        >
                                            {wk}
                                        </button>
                                    ))}
                                    {!calendar && <div className="col-span-4 text-[10px] text-slate-400 italic">正在查询日历...</div>}
                                </div>
                            </div>
                        </div>
                    </section>
                </div>
                <div className="mt-auto p-6 border-t border-slate-200 dark:border-slate-800 w-72">
                    <button className="w-full flex items-center justify-center gap-2 py-3 rounded-xl bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 text-[11px] font-bold text-slate-700 dark:text-slate-300 hover:bg-slate-50 dark:hover:bg-slate-700 shadow-sm transition-all active:scale-95">
                        <RefreshCw size={14} /> 重置所有筛选
                    </button>
                </div>
            </aside>

            {/* 主报表内容 */}
            <div className="flex-1 p-8 overflow-y-auto space-y-7 relative bg-slate-50/30 dark:bg-transparent">
                {error && (
                    <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-2xl p-4 flex items-center gap-3 text-red-600 dark:text-red-400">
                        <AlertCircle size={20} />
                        <div>
                            <p className="text-sm font-bold truncate max-w-xl">SQL Connectivity Error: {error}</p>
                            <p className="text-[10px] opacity-70 mt-1">请尝试在本地命令行运行 `node test-db-manual.js` 检查连接。</p>
                        </div>
                    </div>
                )}

                {loading && !data && (
                    <div className="absolute inset-0 flex items-center justify-center bg-white/50 dark:bg-slate-900/50 backdrop-blur-sm z-50">
                        <div className="flex flex-col items-center gap-4">
                            <Loader2 className="animate-spin text-medtronic" size={40} />
                            <p className="text-sm font-bold text-slate-500 animate-pulse">正在从 SAP 数据库获取实时数据...</p>
                        </div>
                    </div>
                )}

                {/* KPI Row */}
                <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-6">
                    <KpiTile
                        title="Average Hours"
                        value={data?.summary?.avgHours?.toFixed(1) || '0.0'}
                        sub="Daily Avg"
                        icon={<Clock className="text-blue-500" size={18} />}
                    />
                    <KpiTile
                        title="Daily Target"
                        value="3,967"
                        sub="AOP Goal"
                        icon={<Flame className="text-orange-500" size={18} />}
                    />
                    <KpiTile
                        title="MTD EH Performance"
                        value={data?.summary?.totalEH?.toLocaleString() || '0'}
                        sub="vs MAE"
                        status={data?.summary?.totalEH && data.summary.totalEH < 0 ? "negative" : "positive"}
                        icon={<BarChart3 className="text-red-500" size={18} />}
                    />
                    <KpiTile
                        title="Days Count"
                        value={data?.summary?.daysCount?.toString() || '0'}
                        sub="Active Days"
                        icon={<Box className="text-emerald-500" size={18} />}
                    />
                </div>

                {/* Chart Section */}
                <div className="ios-widget p-8 min-h-[450px] flex flex-col bg-white dark:bg-slate-900 border-slate-200/60 dark:border-slate-800 shadow-xl shadow-slate-200/40 dark:shadow-none">
                    <div className="flex justify-between items-center mb-8">
                        <h2 className="text-sm font-bold text-slate-800 dark:text-slate-200">A-工时累计趋势 (Daily EH Output) - Week {selectedWeek}</h2>
                        <div className="flex items-center gap-4 text-[10px] font-bold uppercase tracking-wider">
                            <span className="flex items-center gap-1.5 text-medtronic"><div className="w-3 h-3 rounded bg-medtronic" /> 实际工时</span>
                            <span className="flex items-center gap-1.5 text-slate-400"><div className="w-3 h-3 rounded-full bg-slate-200 dark:bg-slate-700" /> 目标线</span>
                        </div>
                    </div>
                    <div className="flex-1 flex items-end gap-3 pb-4">
                        {data?.trend?.map((d, i) => {
                            const maxEH = Math.max(...data.trend.map(t => t.dailyEH), 1000);
                            const dateObj = new Date(d.PostingDate);
                            return (
                                <div key={i} className="flex-1 flex flex-col items-center gap-4 group">
                                    <div className="w-full bg-slate-100 dark:bg-slate-800 rounded-t-xl relative overflow-hidden h-full flex items-end">
                                        <div
                                            className="w-full bg-medtronic rounded-t-xl transition-all duration-500 group-hover:scale-y-105 group-hover:brightness-110 shadow-lg shadow-blue-500/10"
                                            style={{ height: `${(d.dailyEH / maxEH) * 100}%` }}
                                        />
                                        <div className="absolute top-[-24px] left-1/2 -translate-x-1/2 opacity-0 group-hover:opacity-100 transition-opacity text-[10px] font-black text-medtronic">
                                            {d.dailyEH.toFixed(0)}
                                        </div>
                                    </div>
                                    <div className="text-[10px] font-bold text-slate-400 dark:text-slate-500 whitespace-nowrap">
                                        {dateObj.getMonth() + 1}-{dateObj.getDate()}
                                    </div>
                                </div>
                            );
                        })}
                        {(!data || data.trend.length === 0) && !loading && (
                            <div className="flex-1 self-center text-center text-slate-400 py-20 italic">
                                此财周暂无数据记录
                            </div>
                        )}
                    </div>
                </div>

                {/* Distribution Section */}
                <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
                    <section className="ios-widget p-8 min-h-[350px] bg-white dark:bg-slate-900 border-slate-200/60 dark:border-slate-800">
                        <h3 className="text-sm font-bold mb-6 uppercase tracking-[0.15em] text-slate-400 dark:text-slate-500">Area Distribution</h3>
                        <div className="space-y-5">
                            {data?.distribution?.map((item, i) => {
                                const total = data.summary.totalEH || 1;
                                const percentage = Math.round((item.areaEH / total) * 100);
                                return (
                                    <div key={item.area || i} className="flex items-center gap-4 group cursor-pointer">
                                        <div className="text-[11px] font-bold w-40 truncate text-slate-700 dark:text-slate-300 group-hover:text-medtronic transition-colors">
                                            {item.area || 'Unknown Area'}
                                        </div>
                                        <div className="flex-1 h-2.5 bg-slate-100 dark:bg-slate-800 rounded-full overflow-hidden shadow-inner">
                                            <div className="h-full bg-medtronic transition-all duration-700 group-hover:brightness-110 shadow-[0_0_12px_rgba(0,75,135,0.2)]" style={{ width: `${percentage}%` }} />
                                        </div>
                                        <div className="text-[11px] font-black w-14 text-right text-slate-900 dark:text-white">{percentage}%</div>
                                        <div className="text-[10px] text-slate-400 w-16 text-right font-bold">{item.areaEH?.toFixed(0)}h</div>
                                    </div>
                                );
                            })}
                        </div>
                    </section>

                    <section className="ios-widget p-8 min-h-[350px] bg-white dark:bg-slate-900 border-slate-200/60 dark:border-slate-800">
                        <h3 className="text-sm font-bold mb-6 uppercase tracking-[0.15em] text-slate-400 dark:text-slate-500">Latest Order Audit</h3>
                        <div className="overflow-x-auto">
                            <table className="w-full text-left text-[11px] font-semibold">
                                <thead>
                                    <tr className="text-slate-400 border-b border-slate-100 dark:border-slate-800">
                                        <th className="pb-3">Order</th>
                                        <th className="pb-3">Material</th>
                                        <th className="pb-3 text-right">EH</th>
                                    </tr>
                                </thead>
                                <tbody className="text-slate-600 dark:text-slate-300">
                                    {data?.details?.map((d, i) => (
                                        <tr key={i} className="border-b border-slate-50 dark:border-slate-800/50 last:border-0">
                                            <td className="py-3 font-black text-medtronic">{d.OrderNumber}</td>
                                            <td className="py-3">{d.Material}</td>
                                            <td className="py-3 text-right font-black">{d.eh?.toFixed(1)}</td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    </section>
                </div>
            </div>
        </div>
    );
}

function KpiTile({ title, value, sub, icon, status }: { title: string, value: string, sub: string, icon: React.ReactNode, status?: 'positive' | 'negative' }) {
    return (
        <div className="ios-widget p-6 group hover:translate-y-[-4px] bg-white dark:bg-slate-900 border-slate-200/60 dark:border-slate-800 shadow-lg shadow-slate-200/30 dark:shadow-none transition-all cursor-pointer overflow-hidden relative">
            <div className="absolute top-0 right-0 w-24 h-24 bg-slate-50/50 dark:bg-slate-800/20 rounded-bl-full -mr-12 -mt-12 transition-all group-hover:-mr-8 group-hover:-mt-8" />
            <div className="flex justify-between items-start mb-5 relative z-10">
                <div className="p-2.5 rounded-2xl bg-slate-50 dark:bg-slate-800 text-slate-600 dark:text-slate-400 group-hover:bg-medtronic group-hover:text-white shadow-sm transition-all duration-300">
                    {icon}
                </div>
                <MoreHorizontal size={14} className="text-slate-300 dark:text-slate-600 hover:text-medtronic transition-colors" />
            </div>
            <h4 className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-1 shadow-inner inline-block">
                {title}
            </h4>
            <div className="flex items-baseline gap-2 mt-1 relative z-10">
                <span className={`text-2xl font-black tracking-tight ${status === 'negative' ? 'text-red-500 dark:text-red-400' : 'text-slate-900 dark:text-white'}`}>
                    {value}
                </span>
                <span className="text-[10px] font-bold text-slate-400 dark:text-slate-500">{sub}</span>
            </div>
        </div>
    )
}
