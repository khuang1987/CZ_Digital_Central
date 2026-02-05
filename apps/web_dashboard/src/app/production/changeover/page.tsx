'use client';

import React, { useState, useEffect } from 'react';
import {
    Calendar, Filter, Clock, Zap, AlertCircle, Table as TableIcon, MoreHorizontal, Loader2, Activity, BarChart3, Package
} from 'lucide-react';
import { useUI } from '@/context/UIContext';
import StandardPageLayout, { PageTab } from '@/components/StandardPageLayout';


// --- Types ---
interface CalendarData {
    years: string[];
    months: Record<string, string[]>;
    weeks: Record<string, number[]>;
}

interface ChangeoverData {
    summary: {
        setupCount: number | null;
        avgSetupDuration: number | null;
        onTimeCount: number | null;
    };
    trend: Array<{
        fiscal_week: number;
        setups: number;
    }>;
    topCfns: Array<{
        CFN: string;
        totalDuration: number;
    }>;
}

export default function ChangeoverPage() {
    // --- Calendar State ---
    const [calendar, setCalendar] = useState<CalendarData | null>(null);
    const [selectedYear, setSelectedYear] = useState('FY26');
    const [selectedMonth, setSelectedMonth] = useState('');
    const [selectedWeek, setSelectedWeek] = useState<number | null>(null);

    // --- Data State ---
    const [data, setData] = useState<ChangeoverData | null>(null);
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
                const res = await fetch(`/api/production/changeover?week=${selectedWeek}&year=${selectedYear}`);
                if (!res.ok) throw new Error('Failed to fetch setup data');
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

    const productionTabs: PageTab[] = [
        { label: '工时分析 (Labor EH)', href: '/production/labor-eh', icon: <Activity size={14} /> },
        { label: '调试换型 (Changeover)', href: '/production/changeover', icon: <Zap size={14} />, active: true },
        { label: '排程 (Schedule)', href: '/production/schedule', icon: <Calendar size={14} /> },
        { label: 'OEE 看板', href: '/production/oee', icon: <BarChart3 size={14} />, disabled: true },
        { label: '物料查询', href: '/production/material', icon: <Package size={14} />, disabled: true },
    ];

    return (
        <StandardPageLayout
            title="Changeover Analysis"
            description="Monitor setup times and optimize line transitions."
            icon={<Zap size={24} />}
            tabs={productionTabs}
        >
            <div className="flex flex-1 overflow-hidden h-full rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm bg-white dark:bg-slate-900">
                {/* 筛选面板 */}
                <aside className={`${isFilterOpen ? 'w-72 opacity-100' : 'w-0 opacity-0 overflow-hidden'} border-r border-slate-200 dark:border-slate-800 bg-slate-50/30 dark:bg-slate-800/20 flex flex-col transition-all duration-300 shrink-0`}>
                    <div className="p-6 space-y-8 w-72">
                        <section>
                            <h3 className="text-[10px] font-bold text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-4 flex items-center gap-2">
                                <Calendar size={12} /> 日期与财周
                            </h3>
                            <div className="space-y-4">
                                <div>
                                    <label className="text-[11px] font-bold text-slate-700 dark:text-slate-300 mb-2 block px-1">财年 / Fiscal Year</label>
                                    <select
                                        value={selectedYear}
                                        onChange={(e) => setSelectedYear(e.target.value)}
                                        className="w-full bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl px-3 py-2 text-xs outline-none focus:ring-2 focus:ring-medtronic text-slate-800 dark:text-slate-200 shadow-sm"
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
                                        className="w-full bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl px-3 py-2 text-xs outline-none focus:ring-2 focus:ring-medtronic text-slate-800 dark:text-slate-200 shadow-sm"
                                    >
                                        {calendar?.months?.[selectedYear]?.map(m => <option key={m} value={m}>{m}</option>)}
                                        {!calendar && <option>Loading...</option>}
                                    </select>
                                </div>
                                <div>
                                    <label className="text-[11px] font-bold text-slate-700 dark:text-slate-300 mb-2 block px-1">财周 / Week</label>
                                    <div className="grid grid-cols-5 gap-1.5">
                                        {calendar?.weeks?.[selectedMonth]?.map(wk => (
                                            <button
                                                key={wk}
                                                onClick={() => setSelectedWeek(wk)}
                                                className={`py-2 rounded-lg text-[10px] font-black transition-all shadow-sm ${wk === selectedWeek
                                                    ? 'ios-widget-active'
                                                    : 'bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 text-slate-600 dark:text-slate-400 hover:border-medtronic'
                                                    }`}
                                            >
                                                {wk}
                                            </button>
                                        ))}
                                        {!calendar && <div className="col-span-5 text-[10px] text-slate-400 italic">正在查询日历...</div>}
                                    </div>
                                </div>
                            </div>
                        </section>

                        <section>
                            <h3 className="text-[10px] font-bold text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-4 flex items-center gap-2">
                                <Filter size={12} /> 区域 / Mach#
                            </h3>
                            <div className="grid grid-cols-2 gap-2 mb-5">
                                {['Spine 纵切', 'SUP 纵切', '数控铣', '线切割'].map(area => (
                                    <label key={area} className="flex items-center gap-2 p-2 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-800/50 border border-transparent hover:border-slate-200 dark:hover:border-slate-700 transition-all cursor-pointer group">
                                        <input type="checkbox" className="w-3.5 h-3.5 rounded-md text-medtronic focus:ring-medtronic cursor-pointer" />
                                        <span className="text-[11px] font-bold text-slate-700 dark:text-slate-400 group-hover:text-medtronic truncate">{area}</span>
                                    </label>
                                ))}
                            </div>
                        </section>
                    </div>
                </aside>

                {/* Main Content */}
                <div className="flex-1 overflow-y-auto min-w-0 bg-white dark:bg-slate-900 rounded-r-2xl border-l border-slate-100 dark:border-slate-800">
                    <div className="p-8 space-y-8 min-h-full">
                        {error && (
                            <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-2xl p-4 flex items-center gap-3 text-red-600 dark:text-red-400">
                                <AlertCircle size={20} />
                                <div>
                                    <p className="text-sm font-bold">SQL Analytics Error: {error}</p>
                                    <p className="text-[10px] opacity-70 mt-1">请验证 SQL Server 已开启 TCP/IP 协议并允许 Windows 身份验证。</p>
                                </div>
                            </div>
                        )}

                        {loading && !data && (
                            <div className="absolute inset-0 flex items-center justify-center bg-white/50 dark:bg-slate-900/50 backdrop-blur-sm z-50">
                                <div className="flex flex-col items-center gap-4">
                                    <Loader2 className="animate-spin text-amber-500" size={40} />
                                    <p className="text-sm font-bold text-slate-500 animate-pulse">正在从 MES/v_metrics 计算换型效率...</p>
                                </div>
                            </div>
                        )}

                        {/* KPI Row */}
                        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-6">
                            <KpiTile
                                title="总换型次数"
                                value={data?.summary?.setupCount?.toString() || '0'}
                                sub="Total Cycles"
                                icon={<Zap className="text-amber-500" size={18} />}
                            />
                            <KpiTile
                                title="按时完成"
                                value={data?.summary?.onTimeCount?.toString() || '0'}
                                sub={`${data?.summary?.setupCount ? Math.round((data.summary.onTimeCount! / data.summary.setupCount) * 100) : 0}% Rate`}
                                status="positive"
                                icon={<Clock className="text-emerald-500" size={18} />}
                            />
                            <KpiTile
                                title="平均耗时"
                                value={`${data?.summary?.avgSetupDuration || '0'}h`}
                                sub="Avg Duration"
                                icon={<Clock className="text-blue-500" size={18} />}
                            />
                            <KpiTile
                                title="换型效率异常"
                                value="10"
                                sub="Setup Delays"
                                status="negative"
                                icon={<AlertCircle className="text-red-500" size={18} />}
                            />
                        </div>

                        {/* Charts Row */}
                        <div className="grid grid-cols-1 xl:grid-cols-5 gap-6 h-[450px]">
                            <section className="xl:col-span-3 ios-widget p-8 flex flex-col bg-white dark:bg-slate-900 border-slate-200/60 dark:border-slate-800 shadow-xl shadow-slate-200/40 dark:shadow-none">
                                <h3 className="text-sm font-bold text-slate-800 dark:text-slate-200 mb-8">调试次数趋势统计 (by FW)</h3>
                                <div className="flex-1 flex items-end gap-3 px-2 pb-6">
                                    {data?.trend?.map((v, i) => {
                                        const maxSetups = Math.max(...data.trend.map(t => t.setups), 5);
                                        return (
                                            <div key={i} className="flex-1 flex flex-col items-center gap-4 group">
                                                <div className="w-full bg-slate-50 dark:bg-slate-800/50 rounded-t-xl relative overflow-hidden h-full flex items-end">
                                                    <div
                                                        className={`w-full bg-amber-500 rounded-t-xl transition-all duration-500 group-hover:scale-y-105 shadow-lg shadow-amber-500/10 ${v.fiscal_week === selectedWeek ? 'brightness-125' : 'opacity-60'}`}
                                                        style={{ height: `${(v.setups / maxSetups) * 100}%` }}
                                                    >
                                                        <div className="absolute top-[-26px] left-1/2 -translate-x-1/2 opacity-0 group-hover:opacity-100 transition-opacity text-[10px] font-black text-amber-600">{v.setups}</div>
                                                    </div>
                                                </div>
                                                <span className="text-[10px] font-bold text-slate-400 dark:text-slate-500">{v.fiscal_week}FW</span>
                                            </div>
                                        );
                                    })}
                                    {(!data || data.trend.length === 0) && !loading && (
                                        <div className="flex-1 self-center text-center text-slate-400 py-10 italic">暂无年度趋势数据</div>
                                    )}
                                </div>
                            </section>

                            <section className="xl:col-span-2 ios-widget p-8 flex flex-col bg-white dark:bg-slate-900 border-slate-200/60 dark:border-slate-800 shadow-xl shadow-slate-200/40 dark:shadow-none">
                                <h3 className="text-sm font-bold text-slate-800 dark:text-slate-200 mb-8">调试耗时对比 (Top CFN)</h3>
                                <div className="flex-1 space-y-6 overflow-y-auto pr-3">
                                    {data?.topCfns?.map((item) => (
                                        <div key={item.CFN} className="space-y-2 group cursor-pointer">
                                            <div className="flex justify-between text-[11px] font-bold">
                                                <span className="text-slate-700 dark:text-slate-300 group-hover:text-medtronic transition-colors">{item.CFN}</span>
                                                <span className="text-slate-900 dark:text-white font-black">{item.totalDuration}h</span>
                                            </div>
                                            <div className="h-2.5 w-full bg-slate-100 dark:bg-slate-800 rounded-full overflow-hidden shadow-inner relative">
                                                <div className="h-full bg-blue-500 group-hover:brightness-110 transition-all duration-700" style={{ width: `${(item.totalDuration / 50) * 100}%` }} />
                                            </div>
                                        </div>
                                    ))}
                                    {(!data || data.topCfns.length === 0) && !loading && (
                                        <div className="text-center text-slate-400 py-10 italic">本周无换型记录</div>
                                    )}
                                </div>
                            </section>
                        </div>

                        {/* Detailed Table (Placeholder for dynamic audit logs) */}
                        <section className="ios-widget p-8 bg-white dark:bg-slate-900 border-slate-200/60 dark:border-slate-800">
                            <h3 className="text-sm font-bold text-slate-800 dark:text-slate-200 mb-6 flex items-center gap-2"><TableIcon size={16} className="text-medtronic" /> 调试详情审计表</h3>
                            <div className="overflow-x-auto">
                                <table className="w-full text-left text-[11px] font-semibold border-collapse">
                                    <thead>
                                        <tr className="border-b border-slate-100 dark:border-slate-800 text-slate-400 uppercase tracking-wider">
                                            <th className="py-4 px-3">Lot #</th>
                                            <th className="py-4 px-3">产品图号</th>
                                            <th className="py-4 px-3 text-center">机库</th>
                                            <th className="py-4 px-3 text-center">状态</th>
                                            <th className="py-4 px-3 text-center">实际(H)</th>
                                            <th className="py-4 px-3">调试问题 / 备注</th>
                                        </tr>
                                    </thead>
                                    <tbody className="text-slate-600 dark:text-slate-400">
                                        <tr className="border-b border-slate-50 dark:border-slate-800/50 hover:bg-slate-50/50 dark:hover:bg-slate-800/20 transition-all group">
                                            <td className="py-5 px-3 italic" colSpan={6}>数据同步中，请稍后...</td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                        </section>
                    </div>
                </div>
            </div>
        </StandardPageLayout>
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
                <MoreHorizontal size={14} className="text-slate-300 dark:text-slate-600" />
            </div>
            <h4 className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-1 shadow-inner inline-block">
                {title}
            </h4>
            <div className="flex items-baseline gap-2 mt-1 relative z-10">
                <span className={`text-2xl font-black tracking-tight ${status === 'positive' ? 'text-emerald-500' : status === 'negative' ? 'text-red-500' : 'text-slate-900 dark:text-white'
                    }`}>
                    {value}
                </span>
                <span className="text-[10px] font-bold text-slate-400 dark:text-slate-500">{sub}</span>
            </div>
        </div>
    )
}
