'use client';

import React, { useState, useEffect, useCallback } from 'react';
import {
    BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
    Cell, ComposedChart, Line, CartesianGrid, Legend, PieChart, Pie
} from 'recharts';
import {
    Activity, FileCheck, ClipboardList, TrendingUp,
    AlertCircle, CheckCircle2, Factory, ShieldCheck,
    Calendar as CalendarIcon, ChevronLeft, ChevronRight, X,
    LayoutDashboard
} from 'lucide-react';
import { useUI } from '@/context/UIContext';
import FilterDropdown from '@/components/common/FilterDropdown';
import StandardPageLayout, { PageTab } from '@/components/StandardPageLayout';
import { t } from '@/lib/translations';
import { UnifiedDatePicker } from '@/components/ui/UnifiedDatePicker';

// --- Types ---
interface CalendarData {
    years: string[];
    months: Record<string, string[]>;
    weeks: Record<string, number[]>;
    currentFiscalInfo: {
        fiscal_year: string;
        fiscal_month: string;
        fiscal_week: number;
    };
}

// --- Static Data ---
const yieldTrendData = [
    { month: '2024-08', yield: 98.2, ncmr: 12 },
    { month: '2024-09', yield: 98.8, ncmr: 8 },
    { month: '2024-10', yield: 97.5, ncmr: 15 },
    { month: '2024-11', yield: 99.1, ncmr: 5 },
    { month: '2024-12', yield: 98.4, ncmr: 10 },
    { month: '2025-01', yield: 98.6, ncmr: 7 },
    { month: '2025-02', yield: 99.0, ncmr: 4 },
];

const areaPerformanceData = [
    { area: 'Machining', yield: 98.5, ncmr: 22 },
    { area: 'Assembly', yield: 99.2, ncmr: 12 },
    { area: 'Packaging', yield: 99.8, ncmr: 4 },
    { area: 'Sterilization', yield: 100, ncmr: 0 },
    { area: 'Warehouse', yield: 99.5, ncmr: 2 },
];

const capaStatusData = [
    { name: 'Open', value: 5, color: '#f59e0b' },
    { name: 'In Progress', value: 8, color: '#3b82f6' },
    { name: 'Completed', value: 15, color: '#10b981' },
    { name: 'Late', value: 2, color: '#ef4444' },
];

export default function QualityOverviewDashboard() {
    const { language, toggleFilter, setResetHandler, setHasFilters } = useUI();

    // --- State: Calendar & Filters ---
    const [calendar, setCalendar] = useState<CalendarData | null>(null);
    const [granularity, setGranularity] = useState<'week' | 'month' | 'year' | 'custom'>('month');
    const [selectedYear, setSelectedYear] = useState('FY26');
    const [selectedMonth, setSelectedMonth] = useState('');
    const [selectedWeeks, setSelectedWeeks] = useState<number[]>([]);
    const [selectedFactories, setSelectedFactories] = useState<string[]>([]);
    const [selectedAreas, setSelectedAreas] = useState<string[]>([]);
    const [customRange, setCustomRange] = useState({ start: '', end: '' });

    // --- Logic: Fetch Calendar ---
    useEffect(() => {
        async function fetchCalendar() {
            try {
                const res = await fetch('/api/production/calendar');
                if (!res.ok) throw new Error('Failed to fetch calendar');
                const json = await res.json();
                setCalendar(json);

                if (json.currentFiscalInfo) {
                    setSelectedYear(json.currentFiscalInfo.fiscal_year || 'FY26');
                    setSelectedMonth(json.currentFiscalInfo.fiscal_month || 'Apr');
                    setSelectedWeeks([json.currentFiscalInfo.fiscal_week || 1]);
                } else if (json.years && json.years.length > 0) {
                    const firstYear = json.years[0];
                    setSelectedYear(firstYear);
                    const m = json.months[firstYear]?.[0] || 'Apr';
                    setSelectedMonth(m);
                    if (m && json.weeks[m]) {
                        setSelectedWeeks([json.weeks[m][0] || 1]);
                    }
                }
            } catch (err: any) {
                console.error('Calendar Error:', err);
                setSelectedYear('FY26');
                setSelectedMonth('Apr');
            }
        }
        fetchCalendar();
    }, []);

    const handleReset = useCallback(() => {
        if (calendar?.currentFiscalInfo) {
            setSelectedYear(calendar.currentFiscalInfo.fiscal_year);
            setSelectedMonth(calendar.currentFiscalInfo.fiscal_month);
            setSelectedWeeks([calendar.currentFiscalInfo.fiscal_week]);
        }
        setGranularity('month');
        setSelectedFactories([]);
        setSelectedAreas([]);
        setCustomRange({ start: '', end: '' });
    }, [calendar]);

    const handleYearNavigate = (delta: number) => {
        if (!calendar) return;
        const idx = calendar.years.indexOf(selectedYear);
        const nextIdx = idx + delta;
        if (nextIdx >= 0 && nextIdx < calendar.years.length) {
            setSelectedYear(calendar.years[nextIdx]);
        }
    };

    const handleMonthNavigate = (delta: number) => {
        if (!calendar || !selectedYear) return;
        const months = calendar.months[selectedYear];
        if (!months) return;
        const idx = months.indexOf(selectedMonth);
        const nextIdx = idx + delta;
        if (nextIdx >= 0 && nextIdx < months.length) {
            setSelectedMonth(months[nextIdx]);
        }
    };

    const toggleWeek = (week: number) => {
        setSelectedWeeks(prev => {
            if (prev.includes(week)) {
                return prev.filter(w => w !== week);
            } else {
                return [...prev, week].sort((a, b) => a - b);
            }
        });
    };

    const tabs: PageTab[] = [
        {
            label: 'Overview',
            href: '/quality',
            icon: <LayoutDashboard size={14} />,
            active: true
        },
        {
            label: t('summary_analytics', language),
            href: '/quality/nc',
            icon: <Activity size={14} />,
            active: false
        },
        {
            label: t('detailed_records', language),
            href: '/quality/nc',
            icon: <ClipboardList size={14} />,
            active: false,
            onClick: () => {
                // This triggers the sub-tab selection via navigation in NC page
                window.location.href = '/quality/nc?tab=records';
            }
        }
    ];

    const filters = (
        <div className="space-y-6">
            <section tabIndex={0}>
                <h3 className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-4 flex items-center gap-2">
                    {t('granularity', language)}
                </h3>
                <div className="flex flex-wrap gap-2">
                    {(['week', 'month', 'year', 'custom'] as const).map(g => (
                        <button
                            key={g}
                            onClick={() => setGranularity(g)}
                            className={`flex-1 py-2 px-2 rounded-lg text-[10px] font-black uppercase tracking-tighter transition-all ${granularity === g
                                ? 'bg-medtronic text-white shadow-lg'
                                : 'bg-slate-50 dark:bg-slate-800 text-slate-500 hover:bg-slate-100 dark:hover:bg-slate-700'}`}
                        >
                            {g === 'week' ? t('by_week', language) : g === 'month' ? t('by_month', language) : g === 'year' ? t('by_year', language) : t('custom_range', language)}
                        </button>
                    ))}
                </div>
            </section>

            <section tabIndex={0}>
                <h3 className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-4 flex items-center gap-2">
                    <CalendarIcon size={12} /> {t('time_selection', language)}
                </h3>
                <div className="space-y-4">
                    {granularity !== 'custom' && (
                        <div className="flex items-center bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-700 rounded-xl group transition-all hover:border-medtronic/30">
                            <button onClick={() => handleYearNavigate(1)} className="p-3 text-slate-700 dark:text-slate-300 hover:text-medtronic transition-colors border-r border-slate-200/50 dark:border-slate-700 focus:outline-none"><ChevronLeft size={14} /></button>
                            <div className="flex-1 relative">
                                <select value={selectedYear} onChange={(e) => setSelectedYear(e.target.value)} className="w-full bg-transparent text-slate-800 dark:text-slate-200 px-4 py-2 text-xs font-bold outline-none appearance-none cursor-pointer text-center">
                                    {calendar?.years?.map(y => <option key={y} value={y}>{y}</option>)}
                                </select>
                            </div>
                            <button onClick={() => handleYearNavigate(-1)} className="p-3 text-slate-700 dark:text-slate-300 hover:text-medtronic transition-colors border-l border-slate-200/50 dark:border-slate-700 focus:outline-none"><ChevronRight size={14} /></button>
                        </div>
                    )}

                    {(granularity === 'month' || granularity === 'week') && (
                        <div className="flex items-center bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-700 rounded-xl group transition-all hover:border-medtronic/30">
                            <button onClick={() => handleMonthNavigate(-1)} className="p-3 text-slate-700 dark:text-slate-300 hover:text-medtronic transition-colors border-r border-slate-200/50 dark:border-slate-700 focus:outline-none"><ChevronLeft size={14} /></button>
                            <div className="flex-1 relative">
                                <select value={selectedMonth} onChange={(e) => setSelectedMonth(e.target.value)} className="w-full bg-transparent text-slate-800 dark:text-slate-200 px-4 py-2 text-xs font-bold outline-none appearance-none cursor-pointer text-center">
                                    <option value="" disabled>选择月份</option>
                                    {calendar?.months?.[selectedYear]?.map(m => <option key={m} value={m}>{m}</option>)}
                                </select>
                            </div>
                            <button onClick={() => handleMonthNavigate(1)} className="p-3 text-slate-700 dark:text-slate-300 hover:text-medtronic transition-colors border-l border-slate-200/50 dark:border-slate-700 focus:outline-none"><ChevronRight size={14} /></button>
                        </div>
                    )}

                    {granularity === 'week' && (
                        <div className="grid grid-cols-5 gap-1.5 mt-2">
                            {calendar?.weeks?.[selectedMonth]?.map(w => (
                                <button
                                    key={w}
                                    onClick={() => toggleWeek(w)}
                                    className={`py-2 rounded-lg text-[10px] font-black transition-all ${selectedWeeks.includes(w)
                                        ? 'bg-medtronic text-white shadow-md'
                                        : 'bg-slate-50 dark:bg-slate-800 text-slate-400 hover:bg-slate-100 dark:hover:bg-slate-700 hover:text-slate-600'}`}
                                >
                                    W{w}
                                </button>
                            ))}
                        </div>
                    )}

                    {granularity === 'custom' && (
                        <div className="space-y-3">
                            <UnifiedDatePicker
                                label="Start Date"
                                value={customRange.start}
                                onChange={(e) => setCustomRange(p => ({ ...p, start: e.target.value }))}
                            />
                            <UnifiedDatePicker
                                label="End Date"
                                value={customRange.end}
                                onChange={(e) => setCustomRange(p => ({ ...p, end: e.target.value }))}
                            />
                        </div>
                    )}
                </div>
            </section>

            <div className="h-px bg-slate-200 dark:bg-slate-800 mx-2" />

            <div className="space-y-4">
                <FilterDropdown
                    title={t('factory', language)}
                    options={[]} // Dynamic options not yet available on this page's static-heavy model, but placeholder for UI consistency
                    selected={selectedFactories}
                    onChange={setSelectedFactories}
                />
                <FilterDropdown
                    title={t('area', language)}
                    options={[]}
                    selected={selectedAreas}
                    onChange={setSelectedAreas}
                />
            </div>

            <button
                onClick={handleReset}
                className="w-full mt-6 py-2.5 bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-400 rounded-xl text-xs font-bold hover:bg-slate-200 dark:hover:bg-slate-700 transition-all flex items-center justify-center gap-2"
            >
                <X size={14} />
                {t('reset_filters', language)}
            </button>
        </div>
    );

    return (
        <StandardPageLayout
            title={t('quality_overview_title', language)}
            description={t('quality_overview_desc', language)}
            icon={<FileCheck size={20} />}
            tabs={tabs}
            filters={filters}
            onReset={handleReset}
        >
            <div className="space-y-4 pb-4">
                {/* --- Row 1: KPI Cards --- */}
                <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
                    <KpiCard
                        title={t('yield_rate', language)}
                        value="98.8%"
                        subtitle={`${t('target_yield', language)}: 97.5%`}
                        icon={<ShieldCheck size={16} className="text-emerald-500" />}
                        trend="+0.3%"
                    />
                    <KpiCard
                        title={t('ncmr_count', language)}
                        value="42"
                        subtitle="Current Month"
                        icon={<AlertCircle size={16} className="text-amber-500" />}
                        trend="-15%"
                        trendDown
                    />
                    <KpiCard
                        title="Open CAPA"
                        value="13"
                        subtitle="5 Pending Review"
                        icon={<TrendingUp size={16} className="text-blue-500" />}
                    />
                    <KpiCard
                        title={t('scrap_cost', language)}
                        value="$12.4K"
                        subtitle="Fiscal YTD"
                        icon={<Factory size={16} className="text-slate-500" />}
                        trend="-4%"
                        trendDown
                    />
                </div>

                {/* --- Row 2: Trend Chart & Trend List (Higher Density) --- */}
                <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
                    {/* Trend Chart (Col Span 2) */}
                    <div className="lg:col-span-2 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl p-4 shadow-sm">
                        <div className="flex items-center justify-between mb-4">
                            <h3 className="text-[10px] font-black text-slate-800 dark:text-white uppercase tracking-widest flex items-center gap-2">
                                <div className="w-1 h-3 bg-medtronic rounded-full" />
                                {t('yield_trend', language)}
                            </h3>
                        </div>
                        <div className="h-[250px] w-full">
                            <ResponsiveContainer width="100%" height="100%">
                                <ComposedChart data={yieldTrendData} margin={{ top: 5, right: 5, bottom: 0, left: 0 }}>
                                    <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                                    <XAxis dataKey="month" fontSize={9} axisLine={false} tickLine={false} />
                                    <YAxis yAxisId="left" domain={[95, 100]} fontSize={9} axisLine={false} tickLine={false} unit="%" />
                                    <YAxis yAxisId="right" orientation="right" fontSize={9} axisLine={false} tickLine={false} />
                                    <Tooltip
                                        contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)', fontSize: '10px', padding: '8px' }}
                                    />
                                    <Legend verticalAlign="top" align="right" height={25} iconType="circle" wrapperStyle={{ fontSize: '9px', textTransform: 'uppercase', fontWeight: 'bold' }} />
                                    <Bar yAxisId="right" dataKey="ncmr" name="NCMR" fill="#f59e0b" radius={[2, 2, 0, 0]} barSize={16} />
                                    <Line yAxisId="left" dataKey="yield" name="Yield" stroke="#002554" strokeWidth={2} dot={{ r: 3, fill: '#002554' }} activeDot={{ r: 5 }} />
                                </ComposedChart>
                            </ResponsiveContainer>
                        </div>
                    </div>

                    {/* Trend List Table (Col Span 1) */}
                    <div className="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl p-4 shadow-sm overflow-hidden flex flex-col">
                        <h3 className="text-[10px] font-black text-slate-800 dark:text-white uppercase tracking-widest mb-4 flex items-center gap-2">
                            <div className="w-1 h-3 bg-emerald-500 rounded-full" />
                            {t('fy_summary', language)}
                        </h3>
                        <div className="flex-1 overflow-y-auto custom-scrollbar pr-1 shadow-inner rounded-lg">
                            <table className="w-full text-left border-collapse">
                                <thead className="sticky top-0 bg-white dark:bg-slate-900 z-10">
                                    <tr className="border-b border-slate-100 dark:border-slate-800">
                                        <th className="pb-2 text-[9px] font-black text-slate-400 uppercase tracking-wider">Period</th>
                                        <th className="pb-2 text-[9px] font-black text-slate-400 uppercase tracking-wider text-right">Yield</th>
                                        <th className="pb-2 text-[9px] font-black text-slate-400 uppercase tracking-wider text-right">NCMR</th>
                                    </tr>
                                </thead>
                                <tbody className="divide-y divide-slate-50 dark:divide-slate-800/50">
                                    {[...yieldTrendData].reverse().map((row, i) => (
                                        <tr key={i} className="hover:bg-slate-50 dark:hover:bg-slate-800 transition-colors">
                                            <td className="py-1.5 text-[10px] font-bold text-slate-600 dark:text-slate-400">{row.month}</td>
                                            <td className="py-1.5 text-[10px] font-black text-slate-900 dark:text-white text-right">
                                                <span className={row.yield >= 98.5 ? 'text-emerald-500' : 'text-amber-500'}>
                                                    {row.yield}%
                                                </span>
                                            </td>
                                            <td className="py-1.5 text-[10px] font-black text-slate-900 dark:text-white text-right">{row.ncmr}</td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>

                {/* --- Row 3: Area Breakdown & CAPA Pie --- */}
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                    {/* Area Performance */}
                    <div className="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl p-4 shadow-sm">
                        <h3 className="text-[10px] font-black text-slate-800 dark:text-white uppercase tracking-widest mb-4 flex items-center gap-2">
                            <div className="w-1 h-3 bg-emerald-500 rounded-full" />
                            {t('area_performance', language)}
                        </h3>
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-3">
                            {areaPerformanceData.map((item) => (
                                <div key={item.area} className="group">
                                    <div className="flex justify-between mb-1">
                                        <span className="text-[10px] font-bold text-slate-600 dark:text-slate-400">{item.area}</span>
                                        <span className="text-[10px] font-black text-slate-900 dark:text-white">{item.yield}%</span>
                                    </div>
                                    <div className="h-1.5 w-full bg-slate-100 dark:bg-slate-800 rounded-full overflow-hidden">
                                        <div
                                            className={`h-full transition-all duration-1000 ${item.yield >= 99 ? 'bg-emerald-500' : 'bg-amber-500'
                                                }`}
                                            style={{ width: `${item.yield}%` }}
                                        />
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>

                    {/* CAPA Status Pie */}
                    <div className="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl p-4 shadow-sm">
                        <h3 className="text-[10px] font-black text-slate-800 dark:text-white uppercase tracking-widest mb-2 flex items-center gap-2">
                            <div className="w-1 h-3 bg-blue-500 rounded-full" />
                            {t('capa_status', language)}
                        </h3>
                        <div className="h-[140px] w-full flex items-center">
                            <div className="w-1/2 h-full">
                                <ResponsiveContainer width="100%" height="100%">
                                    <PieChart>
                                        <Pie
                                            data={capaStatusData}
                                            innerRadius={30}
                                            outerRadius={50}
                                            paddingAngle={4}
                                            dataKey="value"
                                            stroke="none"
                                        >
                                            {capaStatusData.map((entry, index) => (
                                                <Cell key={`cell-${index}`} fill={entry.color} />
                                            ))}
                                        </Pie>
                                        <Tooltip
                                            contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)', fontSize: '10px' }}
                                        />
                                    </PieChart>
                                </ResponsiveContainer>
                            </div>
                            <div className="w-1/2 space-y-1">
                                {capaStatusData.map((item, idx) => (
                                    <div key={idx} className="flex items-center justify-between">
                                        <div className="flex items-center gap-2">
                                            <div className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: item.color }} />
                                            <span className="text-[10px] font-bold text-slate-500 dark:text-slate-400">{item.name}</span>
                                        </div>
                                        <span className="text-[10px] font-black text-slate-900 dark:text-white">{item.value}</span>
                                    </div>
                                ))}
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </StandardPageLayout>
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
