'use client';

import React, { useState, useEffect, useCallback, useMemo } from 'react';
import {
    BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
    Cell, PieChart, Pie, Legend, CartesianGrid, LineChart, Line, AreaChart, Area as RechartsArea
} from 'recharts';
import {
    LayoutDashboard, Activity, ClipboardList, Package,
    Factory, Layers, Info, Filter, X, ChevronLeft, ChevronRight,
    Search, Download, TrendingUp, BarChart2
} from 'lucide-react';
import { useUI } from '@/context/UIContext';
import StandardPageLayout, { PageTab } from '@/components/StandardPageLayout';
import { t } from '@/lib/translations';
import FilterDropdown from '@/components/common/FilterDropdown';

// --- Types ---
interface WIPData {
    snapshotDate: string;
    stats: {
        totalBatches: number;
        totalQty: number;
        notStartedCount: number;
        overdueCount: number;
        overdueNotStartedCount: number;
        avgLT: number;
    };
    areaDistribution: Array<{ Area: string; qty: number; count: number; overdue_qty?: number }>;
    opDistribution: Array<{ operation: string; Area: string; qty: number; overdue_qty: number }>;
    trendHistory: Array<{ date: string; qty: number }>;
    records: Array<{
        Plant: string;
        product_no: string;
        batch_no: string;
        operation: string;
        qty: number;
        status: string;
        Area: string;
        track_in_time?: string;
        standard_lt?: number;
        lt_days?: number;
        overdue_status?: string;
        is_not_started?: number;
    }>;
    filterOptions: {
        dates: string[];
        factories: string[];
        areas: string[];
        vsms: string[];
    };
}

export default function DeliveryWIPPage() {
    const { language } = useUI();
    const [activeSection, setActiveSection] = useState<'overview' | 'analytics' | 'records'>('overview');

    // --- Refs for Sections ---
    const overviewRef = React.useRef<HTMLDivElement>(null);
    const analyticsRef = React.useRef<HTMLDivElement>(null);
    const recordsRef = React.useRef<HTMLDivElement>(null);

    // --- State: Filters ---
    const [selectedDate, setSelectedDate] = useState<string>('');
    const [selectedVsms, setSelectedVsms] = useState<string[]>([]);
    const [selectedFactories, setSelectedFactories] = useState<string[]>([]);
    const [selectedAreas, setSelectedAreas] = useState<string[]>([]);
    const [searchTerm, setSearchTerm] = useState('');

    const [data, setData] = useState<WIPData | null>(null);
    const [loading, setLoading] = useState(true);

    // --- Logic: Fetch Data ---
    const fetchData = useCallback(async () => {
        setLoading(true);
        try {
            const params = new URLSearchParams({
                date: selectedDate,
                vsm: selectedVsms.join(','),
                factories: selectedFactories.join(','),
                areas: selectedAreas.join(',')
            });
            const res = await fetch(`/api/delivery/wip?${params}`);
            const json = await res.json();
            setData(json);
            if (!selectedDate && json.snapshotDate) {
                setSelectedDate(json.snapshotDate);
            }
        } catch (err) {
            console.error('Fetch WIP Error:', err);
        } finally {
            setLoading(false);
        }
    }, [selectedDate, selectedVsms, selectedFactories, selectedAreas]);

    useEffect(() => {
        fetchData();
    }, [fetchData]);

    const handleReset = useCallback(() => {
        setSelectedVsms([]);
        setSelectedFactories([]);
        setSelectedAreas([]);
        if (data?.filterOptions?.dates?.[0]) {
            setSelectedDate(data.filterOptions.dates[0]);
        }
    }, [data?.filterOptions?.dates]);

    // --- Scroll Tracking ---
    useEffect(() => {
        const options = {
            root: null,
            rootMargin: '-20% 0px -70% 0px',
            threshold: 0
        };

        const observer = new IntersectionObserver((entries) => {
            entries.forEach((entry) => {
                if (entry.isIntersecting) {
                    setActiveSection(entry.target.id as any);
                }
            });
        }, options);

        if (overviewRef.current) observer.observe(overviewRef.current);
        if (recordsRef.current) observer.observe(recordsRef.current);

        return () => observer.disconnect();
    }, [loading]);

    // --- Scroll Snap Injection ---
    useEffect(() => {
        const main = document.querySelector('main');
        if (main) {
            main.classList.add('snap-y', 'snap-mandatory', 'scroll-smooth');
        }
        return () => {
            if (main) {
                main.classList.remove('snap-y', 'snap-mandatory', 'scroll-smooth');
            }
        };
    }, []);

    const scrollTo = (id: string) => {
        const element = document.getElementById(id);
        if (element) {
            element.scrollIntoView({ behavior: 'smooth' });
        }
    };

    // --- Render Helpers ---
    const tabs: PageTab[] = useMemo(() => [
        {
            label: "WIP",
            href: '#',
            icon: <Layers size={14} />,
            active: true
        }
    ], []);

    const filters = useMemo(() => (
        <div className="space-y-6">
            <section>
                <h3 className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-4 flex items-center gap-2">
                    <Filter size={12} /> {t('time_selection', language)}
                </h3>
                <div className="relative group">
                    <select
                        value={selectedDate}
                        onChange={(e) => setSelectedDate(e.target.value)}
                        className="w-full bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-700 rounded-xl px-4 py-2.5 text-xs font-bold outline-none appearance-none cursor-pointer focus:border-medtronic transition-all"
                    >
                        {data?.filterOptions?.dates?.map(d => (
                            <option key={d} value={d}>{d}</option>
                        ))}
                    </select>
                </div>
            </section>

            <div className="h-px bg-slate-200 dark:bg-slate-800 mx-2" />

            <div className="space-y-4">
                <FilterDropdown
                    title="VSM"
                    options={data?.filterOptions?.vsms || []}
                    selected={selectedVsms}
                    onChange={setSelectedVsms}
                />
                <FilterDropdown
                    title={t('factory', language)}
                    options={data?.filterOptions?.factories || []}
                    selected={selectedFactories}
                    onChange={setSelectedFactories}
                />
                <FilterDropdown
                    title={t('area', language)}
                    options={data?.filterOptions?.areas || []}
                    selected={selectedAreas}
                    onChange={setSelectedAreas}
                />
            </div>

            <button
                onClick={handleReset}
                className="w-full mt-6 py-2.5 bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-400 rounded-xl text-xs font-bold hover:bg-slate-200 dark:hover:bg-slate-700 transition-all flex items-center justify-center gap-2"
            >
                <X size={14} /> {t('reset_filters', language)}
            </button>
        </div>
    ), [language, selectedDate, data?.filterOptions, selectedVsms, selectedFactories, selectedAreas, handleReset]);

    // Filtered Records for Search
    const filteredRecords = useMemo(() => {
        if (!data?.records) return [];
        if (!searchTerm) return data.records;
        const s = searchTerm.toLowerCase();
        return data.records.filter(r =>
            r.batch_no?.toLowerCase().includes(s) ||
            r.product_no?.toLowerCase().includes(s) ||
            r.operation?.toLowerCase().includes(s)
        );
    }, [data?.records, searchTerm]);

    return (
        <StandardPageLayout
            title={t('wip_dashboard_title', language)}
            description={t('wip_dashboard_desc', language)}
            icon={<Layers size={20} />}
            tabs={tabs}
            filters={filters}
            onReset={handleReset}
        >
            {loading ? (
                <div className="flex items-center justify-center h-64">
                    <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-medtronic"></div>
                </div>
            ) : (
                <div className="flex items-start gap-4 min-h-full">
                    {/* Floating Dot Nav */}
                    <div className="sticky top-0 flex flex-col gap-5 py-8 z-10 w-4 shrink-0 border-r border-slate-100 dark:border-slate-800 h-[calc(100vh-200px)] justify-center -ml-2">
                        <DotNavItem active={activeSection === 'overview'} onClick={() => scrollTo('overview')} label="KPI & Pareto" />
                        <DotNavItem active={activeSection === 'records'} onClick={() => scrollTo('records')} label="Details" />
                    </div>

                    {/* Content Area */}
                    <div className="flex-1 space-y-12 pb-24 px-2">
                        {/* 1. Overview Section */}
                        <div id="overview" ref={overviewRef} className="space-y-4 snap-start scroll-mt-6">
                            {/* KPI Grid */}
                            <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
                                <KpiCard
                                    title="总批次数量"
                                    value={data?.stats?.totalBatches?.toLocaleString() || '0'}
                                    subtitle="Active Shop Floor Batches"
                                    icon={<Layers className="text-medtronic" size={20} />}
                                />
                                <KpiCard
                                    title="未开工批次"
                                    value={data?.stats?.notStartedCount?.toLocaleString() || '0'}
                                    subtitle="Not Started (WIP)"
                                    icon={<Activity className="text-amber-500" size={20} />}
                                />
                                <KpiCard
                                    title="超期批次数量"
                                    value={data?.stats?.overdueCount?.toLocaleString() || '0'}
                                    subtitle="Batches > Standard LT"
                                    icon={<Info className="text-red-500" size={20} />}
                                    trendDown={true}
                                />
                                <KpiCard
                                    title="超期未开工批次"
                                    value={data?.stats?.overdueNotStartedCount?.toLocaleString() || '0'}
                                    subtitle="Critical Overdue"
                                    icon={<Activity className="text-red-600" size={20} />}
                                />
                                <KpiCard
                                    title="平均LT (天)"
                                    value={data?.stats?.avgLT?.toFixed(2) || '0.00'}
                                    subtitle="Avg. Lead Time Across Areas"
                                    icon={<Activity className="text-medtronic" size={20} />}
                                />
                            </div>

                            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                                {/* Area Distribution */}
                                <div className="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl p-4 shadow-sm">
                                    <h3 className="text-[10px] font-black text-slate-800 dark:text-white uppercase tracking-widest mb-4 flex items-center gap-2">
                                        <BarChart2 size={13} className="text-medtronic" />
                                        Area Distribution (Qty)
                                    </h3>
                                    <div className="h-[210px]">
                                        <ResponsiveContainer width="100%" height="100%">
                                            <BarChart data={data?.areaDistribution || []}>
                                                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                                                <XAxis dataKey="Area" fontSize={9} axisLine={false} tickLine={false} />
                                                <YAxis fontSize={9} axisLine={false} tickLine={false} />
                                                <Tooltip
                                                    contentStyle={{ borderRadius: '12px', border: 'none', boxShadow: '0 10px 15px -3px rgb(0 0 0 / 0.1)' }}
                                                />
                                                <Bar dataKey="qty" name="Qty" fill="#002554" radius={[4, 4, 0, 0]} barSize={24} />
                                            </BarChart>
                                        </ResponsiveContainer>
                                    </div>
                                </div>

                                {/* WIP Trend */}
                                <div className="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl p-4 shadow-sm">
                                    <h3 className="text-[10px] font-black text-slate-800 dark:text-white uppercase tracking-widest mb-4 flex items-center gap-2">
                                        <TrendingUp size={13} className="text-emerald-500" />
                                        Total WIP Trend (Qty)
                                    </h3>
                                    <div className="h-[210px]">
                                        <ResponsiveContainer width="100%" height="100%">
                                            <AreaChart data={data?.trendHistory || []}>
                                                <defs>
                                                    <linearGradient id="colorQty" x1="0" y1="0" x2="0" y2="1">
                                                        <stop offset="5%" stopColor="#10b981" stopOpacity={0.1} />
                                                        <stop offset="95%" stopColor="#10b981" stopOpacity={0} />
                                                    </linearGradient>
                                                </defs>
                                                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                                                <XAxis dataKey="date" fontSize={8} axisLine={false} tickLine={false} />
                                                <YAxis fontSize={9} axisLine={false} tickLine={false} />
                                                <Tooltip
                                                    contentStyle={{ borderRadius: '12px', border: 'none', boxShadow: '0 10px 15px -3px rgb(0 0 0 / 0.1)' }}
                                                />
                                                <RechartsArea type="monotone" dataKey="qty" name="Total Qty" stroke="#10b981" fillOpacity={1} fill="url(#colorQty)" strokeWidth={2} />
                                            </AreaChart>
                                        </ResponsiveContainer>
                                    </div>
                                </div>
                            </div>

                            {/* Pareto Chart in First Page */}
                            <div className="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl p-5 shadow-sm">
                                <h3 className="text-[10px] font-black text-slate-800 dark:text-white uppercase tracking-widest mb-6 flex items-center gap-2">
                                    <div className="w-1 h-3 bg-amber-500 rounded-full" />
                                    Operations Living Pareto (By Qty)
                                </h3>
                                <div className="h-[360px]">
                                    <ResponsiveContainer width="100%" height="100%">
                                        <BarChart
                                            data={data?.opDistribution || []}
                                            margin={{ top: 10, right: 20, left: 10, bottom: 65 }}
                                        >
                                            <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                                            <XAxis
                                                dataKey="operation"
                                                fontSize={8}
                                                angle={-45}
                                                textAnchor="end"
                                                interval={0}
                                                height={90}
                                                axisLine={false}
                                                tickLine={false}
                                            />
                                            <YAxis fontSize={9} axisLine={false} tickLine={false} />
                                            <Tooltip
                                                contentStyle={{ borderRadius: '12px', border: 'none', boxShadow: '0 10px 15px -3px rgb(0 0 0 / 0.1)' }}
                                            />
                                            <Legend verticalAlign="top" align="right" wrapperStyle={{ paddingBottom: '10px', fontSize: '9px' }} />
                                            <Bar dataKey="qty" name="Batch Qty" fill="#002554" radius={[3, 3, 0, 0]} barSize={20} />
                                            <Bar dataKey="overdue_qty" name="Overdue Qty" fill="#ef4444" radius={[3, 3, 0, 0]} barSize={20} />
                                        </BarChart>
                                    </ResponsiveContainer>
                                </div>
                            </div>
                        </div>

                        {/* 2. Records Section (Moved to second page) */}
                        <div id="records" ref={recordsRef} className="space-y-6 snap-start scroll-mt-6">
                            <div className="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl shadow-sm overflow-hidden flex flex-col">
                                <div className="p-4 border-b border-slate-100 dark:border-slate-800 flex flex-col md:flex-row justify-between items-center gap-4">
                                    <div className="relative w-full md:w-96">
                                        <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" size={14} />
                                        <input
                                            type="text"
                                            placeholder="Search Batch or Product..."
                                            value={searchTerm}
                                            onChange={(e) => setSearchTerm(e.target.value)}
                                            className="w-full bg-slate-50 dark:bg-slate-800 border-none rounded-xl pl-10 pr-4 py-2 text-xs font-bold outline-none focus:ring-2 focus:ring-medtronic/20 transition-all"
                                        />
                                    </div>
                                    <button className="flex items-center gap-2 px-4 py-2 bg-medtronic text-white rounded-xl text-[10px] font-black uppercase tracking-wider shadow-lg shadow-medtronic/20 hover:scale-105 transition-all">
                                        <Download size={14} /> {t('export_xlsx', language)}
                                    </button>
                                </div>

                                <div className="overflow-x-auto">
                                    <table className="w-full text-left border-collapse min-w-[1200px]">
                                        <thead className="bg-slate-50 dark:bg-slate-800/50 text-slate-400">
                                            <tr>
                                                <th className="px-3 py-3 text-[9px] font-black uppercase tracking-widest border-b border-slate-100 dark:border-slate-800">批次</th>
                                                <th className="px-3 py-3 text-[9px] font-black uppercase tracking-widest border-b border-slate-100 dark:border-slate-800">生产流转卡号</th>
                                                <th className="px-3 py-3 text-[9px] font-black uppercase tracking-widest border-b border-slate-100 dark:border-slate-800">工序号</th>
                                                <th className="px-3 py-3 text-[9px] font-black uppercase tracking-widest border-b border-slate-100 dark:border-slate-800">工序名称</th>
                                                <th className="px-3 py-3 text-[9px] font-black uppercase tracking-widest border-b border-slate-100 dark:border-slate-800 text-right">批数量</th>
                                                <th className="px-3 py-3 text-[9px] font-black uppercase tracking-widest border-b border-slate-100 dark:border-slate-800">产品代码</th>
                                                <th className="px-3 py-3 text-[9px] font-black uppercase tracking-widest border-b border-slate-100 dark:border-slate-800">产品名称</th>
                                                <th className="px-3 py-3 text-[9px] font-black uppercase tracking-widest border-b border-slate-100 dark:border-slate-800">机台号</th>
                                                <th className="px-3 py-3 text-[9px] font-black uppercase tracking-widest border-b border-slate-100 dark:border-slate-800">进工序时间</th>
                                                <th className="px-3 py-3 text-[9px] font-black uppercase tracking-widest border-b border-slate-100 dark:border-slate-800 text-right">进工序天数</th>
                                                <th className="px-3 py-3 text-[9px] font-black uppercase tracking-widest border-b border-slate-100 dark:border-slate-800 text-right">标准LT</th>
                                                <th className="px-3 py-3 text-[9px] font-black uppercase tracking-widest border-b border-slate-100 dark:border-slate-800">超期状态</th>
                                                <th className="px-3 py-3 text-[9px] font-black uppercase tracking-widest border-b border-slate-100 dark:border-slate-800">开工日期</th>
                                            </tr>
                                        </thead>
                                        <tbody className="divide-y divide-slate-50 dark:divide-slate-800/50">
                                            {filteredRecords.map((r, i) => (
                                                <tr key={i} className="hover:bg-slate-50 dark:hover:bg-slate-800 transition-colors group">
                                                    <td className="px-3 py-3 text-[10px] font-bold text-slate-500">{r.batch_no}</td>
                                                    <td className="px-3 py-3 text-[10px] font-bold text-slate-500">{r.batch_no}</td>
                                                    <td className="px-3 py-3 text-[10px] font-bold text-slate-900 dark:text-white">{r.operation.split(' ')[0]}</td>
                                                    <td className="px-3 py-3 text-[10px] font-bold text-slate-600 dark:text-slate-400">{r.operation}</td>
                                                    <td className="px-3 py-3 text-[11px] font-black text-right text-medtronic">{r.qty.toLocaleString()}</td>
                                                    <td className="px-3 py-3 text-[10px] font-bold text-slate-900 dark:text-white">{r.product_no}</td>
                                                    <td className="px-3 py-3 text-[10px] font-bold text-slate-400 truncate max-w-[150px]">{r.product_no}</td>
                                                    <td className="px-3 py-3 text-[10px] font-bold text-slate-400">N/A</td>
                                                    <td className="px-3 py-3 text-[10px] font-bold text-slate-500">
                                                        {r.track_in_time ? new Date(r.track_in_time).toLocaleString() : '-'}
                                                    </td>
                                                    <td className={`px-3 py-3 text-[10px] font-black text-right ${r.overdue_status === 'Overdue' ? 'text-red-500' : 'text-slate-700 dark:text-slate-300'}`}>
                                                        {r.lt_days?.toFixed(2) || '0.00'}
                                                    </td>
                                                    <td className="px-3 py-3 text-[10px] font-bold text-right text-slate-400">{r.standard_lt?.toFixed(1) || '5.0'}</td>
                                                    <td className="px-3 py-3">
                                                        <div className="flex items-center gap-1.5">
                                                            <div className={`w-3 h-3 rounded-full flex items-center justify-center ${r.overdue_status === 'Overdue' ? 'bg-red-50 text-red-500' : 'bg-emerald-50 text-emerald-500'}`}>
                                                                {r.overdue_status === 'Overdue' ? '!' : '✓'}
                                                            </div>
                                                            <span className={`text-[9px] font-black uppercase ${r.overdue_status === 'Overdue' ? 'text-red-500' : 'text-emerald-500'}`}>
                                                                {r.overdue_status === 'Overdue' ? '超期' : '正常'}
                                                            </span>
                                                        </div>
                                                    </td>
                                                    <td className="px-3 py-3 text-[10px] font-bold text-slate-400">
                                                        {r.track_in_time ? new Date(r.track_in_time).toLocaleDateString() : '-'}
                                                    </td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            )}
        </StandardPageLayout>
    );
}

function DotNavItem({ active, onClick, label }: { active: boolean, onClick: () => void, label: string }) {
    return (
        <button
            onClick={onClick}
            className="group relative flex items-center justify-center p-1 outline-none"
            title={label}
        >
            <div className={`transition-all duration-300 rounded-full ${active
                ? 'w-2.5 h-2.5 bg-medtronic ring-4 ring-medtronic/10 scale-110'
                : 'w-1.5 h-1.5 bg-slate-200 hover:bg-slate-400 scale-100 hover:scale-125'
                }`}
            />
            {!active && (
                <span className="absolute left-5 px-1.5 py-0.5 bg-slate-800 text-white text-[8px] rounded opacity-0 group-hover:opacity-100 transition-opacity whitespace-nowrap pointer-events-none uppercase font-black tracking-widest">
                    {label}
                </span>
            )}
        </button>
    );
}

function KpiCard({ title, value, subtitle, icon, trend, trendDown }: {
    title: string, value: string, subtitle: string, icon: React.ReactNode, trend?: string, trendDown?: boolean
}) {
    return (
        <div className="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-xl p-3 shadow-sm hover:shadow-md transition-all group">
            <div className="flex justify-between items-start mb-2">
                <div className="p-2 bg-slate-50 dark:bg-slate-800 rounded-lg group-hover:scale-110 transition-transform">
                    {icon}
                </div>
                {trend && (
                    <span className={`text-[9px] font-black px-1.5 py-0.5 rounded-full ${trendDown ? 'bg-red-50 text-red-600' : 'bg-emerald-50 text-emerald-600'}`}>
                        {trend}
                    </span>
                )}
            </div>
            <div className="space-y-0.5">
                <h4 className="text-[9px] font-black text-slate-400 uppercase tracking-widest">{title}</h4>
                <div className="text-lg font-black text-slate-900 dark:text-white tracking-tight leading-none">{value}</div>
                <p className="text-[8px] font-black text-slate-400 whitespace-nowrap overflow-hidden text-ellipsis">{subtitle}</p>
            </div>
        </div>
    );
}
