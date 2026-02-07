'use client';

import React, { useEffect, useState, useCallback, useMemo } from 'react';
import {
    BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
    Cell, ComposedChart, Line, CartesianGrid, Legend
} from 'recharts';
import {
    AlertOctagon, Activity, FileText, Calendar, Filter,
    ChevronLeft, ChevronRight, CheckCircle2, X,
    Copy, User, Tag, LayoutDashboard, Database,
    ClipboardList, TrendingUp, AlertTriangle
} from 'lucide-react';
import { useUI } from '@/context/UIContext';
import FilterDropdown from '@/components/common/FilterDropdown';
import StandardPageLayout, { PageTab } from '@/components/StandardPageLayout';
import { t, Language } from '@/lib/translations';

import { UnifiedDatePicker } from '@/components/ui/UnifiedDatePicker';

interface NCRecord {
    date: string;
    batch: string;
    product: string;
    process: string;
    machine: string;
    qty: number;
    reason: string;
    recorder: string;
    action_taken: string;
    status: string;
    feature_name: string;
    nc_description: string;
}

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

interface NCData {
    stats: {
        totalRecords: number;
        totalQty: number;
        openNCs: number;
        topReason: string;
    };
    paretoData: {
        reason: string;
        count: number;
        qty: number;
        percentage: number;
        cumulativePercentage: number;
    }[];
    trendData: {
        month: string;
        [reason: string]: number | string;
    }[];
    records: NCRecord[];
    filterOptions: {
        products: string[];
        reasons: string[];
        processes: string[];
        recorders: string[];
        factories: string[];
        areas: string[];
    };
}

const chartGlobalStyles = `
  .recharts-wrapper, .recharts-wrapper *, .recharts-surface, .recharts-surface *, .recharts-layer, .recharts-layer *, .recharts-rectangle {
    outline: none !important;
    box-shadow: none !important;
    border: none !important;
    -webkit-tap-highlight-color: transparent !important;
  }
  rect.recharts-rectangle:focus, path.recharts-rectangle:focus, .recharts-bar-rectangle:focus {
    outline: none !important;
    border: none !important;
  }
`;

export default function NCAnalysisDashboard() {
    const [data, setData] = useState<NCData | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [activeTab, setActiveTab] = useState<'analytics' | 'records'>('analytics');

    // Calendar & Transition State
    const [calendar, setCalendar] = useState<CalendarData | null>(null);
    const [granularity, setGranularity] = useState<'week' | 'month' | 'year' | 'custom'>('month');
    const [selectedYear, setSelectedYear] = useState('FY26');
    const [selectedMonth, setSelectedMonth] = useState('');
    const [selectedWeeks, setSelectedWeeks] = useState<number[]>([]);
    const [customRange, setCustomRange] = useState({ start: '', end: '' });

    const { toggleFilter, setResetHandler, setHasFilters, language } = useUI();

    // Filter State
    const [selectedProducts, setSelectedProducts] = useState<string[]>([]);
    const [selectedReasons, setSelectedReasons] = useState<string[]>([]);
    const [selectedProcesses, setSelectedProcesses] = useState<string[]>([]);
    const [selectedRecorders, setSelectedRecorders] = useState<string[]>([]);
    const [selectedFactories, setSelectedFactories] = useState<string[]>([]);
    const [selectedAreas, setSelectedAreas] = useState<string[]>([]);

    const [selectedRecord, setSelectedRecord] = useState<NCRecord | null>(null);

    // 1. Fetch Calendar Data
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
                // Hard fallbacks
                setSelectedYear('FY26');
                setSelectedMonth('Apr');
            }
        }
        fetchCalendar();
    }, []);

    const fetchData = useCallback(async () => {
        // Guard for required time parameters
        if (granularity === 'week' && selectedWeeks.length === 0) return;
        if (granularity === 'month' && !selectedMonth) return;
        if (granularity === 'custom' && (!customRange.start || !customRange.end)) return;

        setLoading(true);
        setError(null);
        try {
            const params = new URLSearchParams({
                granularity,
                year: selectedYear,
                month: selectedMonth,
                week: selectedWeeks.join(','),
                startDate: customRange.start,
                endDate: customRange.end,
                products: selectedProducts.join(','),
                reasons: selectedReasons.join(','),
                processes: selectedProcesses.join(','),
                recorders: selectedRecorders.join(','),
                factories: selectedFactories.join(','),
                areas: selectedAreas.join(',')
            });

            const res = await fetch(`/api/quality/nc?${params}`);
            const result = await res.json();
            if (!res.ok) throw new Error(result.error || 'Failed to fetch NC data');
            setData(result);
        } catch (err: any) {
            setError(err.message);
            console.error('Fetch Error:', err);
        } finally {
            setLoading(false);
        }
    }, [granularity, selectedYear, selectedMonth, selectedWeeks, customRange, selectedProducts, selectedReasons, selectedProcesses, selectedRecorders]);

    useEffect(() => {
        fetchData();
    }, [fetchData]);

    const handleReset = useCallback(() => {
        if (calendar?.currentFiscalInfo) {
            setSelectedYear(calendar.currentFiscalInfo.fiscal_year);
            setSelectedMonth(calendar.currentFiscalInfo.fiscal_month);
            setSelectedWeeks([calendar.currentFiscalInfo.fiscal_week]);
        }
        setGranularity('month');
        setSelectedProducts([]);
        setSelectedReasons([]);
        setSelectedProcesses([]);
        setSelectedRecorders([]);
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
            active: false
        },
        {
            label: t('summary_analytics', language),
            href: '#analytics',
            icon: <Activity size={14} />,
            active: activeTab === 'analytics',
            onClick: () => setActiveTab('analytics')
        },
        {
            label: t('detailed_records', language),
            href: '#records',
            icon: <ClipboardList size={14} />,
            active: activeTab === 'records',
            onClick: () => setActiveTab('records')
        }
    ];

    const filters = (
        <div className="space-y-6">
            {/* 1. Time Granularity */}
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

            {/* 2. Time Selection */}
            <section tabIndex={0}>
                <h3 className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-4 flex items-center gap-2">
                    <Calendar size={12} /> {t('time_selection', language)}
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

            {/* 3. Dimension Filters */}
            <div className="space-y-4">
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
                <FilterDropdown
                    title="Products"
                    options={data?.filterOptions?.products || []}
                    selected={selectedProducts}
                    onChange={setSelectedProducts}
                />
                <FilterDropdown
                    title="Defect Reasons"
                    options={data?.filterOptions?.reasons || []}
                    selected={selectedReasons}
                    onChange={setSelectedReasons}
                />
                <FilterDropdown
                    title="Processes"
                    options={data?.filterOptions?.processes || []}
                    selected={selectedProcesses}
                    onChange={setSelectedProcesses}
                />
                <FilterDropdown
                    title="Recorders"
                    options={data?.filterOptions?.recorders || []}
                    selected={selectedRecorders}
                    onChange={setSelectedRecorders}
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

    if (error) return (
        <StandardPageLayout title={t('nc_analysis_title', language)} description={t('nc_analysis_desc', language)}>
            <div className="flex flex-col items-center justify-center h-[60vh] text-red-500">
                <AlertTriangle size={48} className="mb-4" />
                <h3 className="text-xl font-bold">Error Loading Data</h3>
                <p className="text-sm opacity-80">{error}</p>
                <button onClick={() => fetchData()} className="mt-4 px-6 py-2 bg-red-50 text-red-600 rounded-xl font-bold focus:outline-none">Retry</button>
            </div>
        </StandardPageLayout>
    );

    return (
        <StandardPageLayout
            title={t('nc_analysis_title', language)}
            description={t('nc_analysis_desc', language)}
            icon={<AlertOctagon size={20} />}
            filters={filters}
            tabs={tabs}
            onReset={handleReset}
        >
            <style dangerouslySetInnerHTML={{ __html: chartGlobalStyles }} />

            <div className="space-y-6">
                {/* 1. Header Navigation Override Logic */}
                <div onClick={(e) => {
                    const target = (e.target as HTMLElement).closest('a');
                    if (target) {
                        e.preventDefault();
                        const href = target.getAttribute('href');
                        if (href === '#analytics') setActiveTab('analytics');
                        else if (href === '#records') setActiveTab('records');
                    }
                }}>
                    {/* The tabs are handled by StandardPageLayout, we just listen to clicks if needed or use state */}
                </div>

                {activeTab === 'analytics' ? (
                    <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
                        {/* KPI Row */}
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
                            <Card
                                icon={<AlertOctagon className="text-medtronic" />}
                                title={t('total_nc_records', language)}
                                value={data?.stats?.totalRecords || 0}
                                subtitle="NC Events tracked"
                            />
                            <Card
                                icon={<ClipboardList className="text-emerald-500" />}
                                title={t('total_nc_qty', language)}
                                value={data?.stats?.totalQty || 0}
                                subtitle="Pieces non-conforming"
                            />
                            <Card
                                icon={<Activity className="text-amber-500" />}
                                title={t('open_nc_count', language)}
                                value={data?.stats?.openNCs || 0}
                                subtitle="Currently pending"
                            />
                            <Card
                                icon={<Tag className="text-indigo-500" />}
                                title={t('top_defect_reason', language)}
                                value={data?.stats?.topReason || 'N/A'}
                                subtitle="Most frequent issue"
                                isLabel
                            />
                        </div>

                        {/* Charts Area */}
                        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                            {/* Pareto Chart */}
                            <div className="bg-white dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-[24px] p-6 shadow-sm overflow-hidden">
                                <div className="flex items-center justify-between mb-6">
                                    <h3 className="text-sm font-black text-slate-800 dark:text-white flex items-center gap-2">
                                        <div className="w-1 h-4 bg-medtronic rounded-full" />
                                        {t('pareto_title', language)}
                                    </h3>
                                </div>
                                <div className="h-[350px] w-full relative min-w-0 min-h-0">
                                    <ResponsiveContainer width="100%" height="100%">
                                        <ComposedChart data={data?.paretoData || []}>
                                            <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                                            <XAxis
                                                dataKey="reason"
                                                fontSize={10}
                                                tickLine={false}
                                                axisLine={false}
                                                tick={{ fill: '#64748b', fontWeight: 600 }}
                                                interval={0}
                                                angle={-45}
                                                textAnchor="end"
                                                height={80}
                                            />
                                            <YAxis
                                                yAxisId="left"
                                                fontSize={10}
                                                tickLine={false}
                                                axisLine={false}
                                                tick={{ fill: '#64748b' }}
                                            />
                                            <YAxis
                                                yAxisId="right"
                                                orientation="right"
                                                fontSize={10}
                                                tickLine={false}
                                                axisLine={false}
                                                tick={{ fill: '#64748b' }}
                                                domain={[0, 100]}
                                                unit="%"
                                            />
                                            <Tooltip
                                                contentStyle={{ borderRadius: '12px', border: 'none', boxShadow: '0 10px 15px -3px rgb(0 0 0 / 0.1)' }}
                                            />
                                            <Bar yAxisId="left" dataKey="qty" fill="#002554" radius={[4, 4, 0, 0]} name="Qty" />
                                            <Line yAxisId="right" type="monotone" dataKey="cumulativePercentage" stroke="#ef4444" strokeWidth={2} dot={{ r: 4, fill: '#ef4444' }} name="Cumulative %" />
                                        </ComposedChart>
                                    </ResponsiveContainer>
                                </div>
                            </div>

                            {/* Trend Chart */}
                            <div className="bg-white dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-[24px] p-6 shadow-sm overflow-hidden">
                                <div className="flex items-center justify-between mb-6">
                                    <h3 className="text-sm font-black text-slate-800 dark:text-white flex items-center gap-2">
                                        <div className="w-1 h-4 bg-emerald-500 rounded-full" />
                                        {t('trend_title', language)}
                                    </h3>
                                </div>
                                <div className="h-[350px] w-full relative min-w-0 min-h-0">
                                    <ResponsiveContainer width="100%" height="100%">
                                        <BarChart data={data?.trendData || []}>
                                            <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                                            <XAxis
                                                dataKey="month"
                                                fontSize={10}
                                                tickLine={false}
                                                axisLine={false}
                                                tick={{ fill: '#64748b', fontWeight: 600 }}
                                            />
                                            <YAxis
                                                fontSize={10}
                                                tickLine={false}
                                                axisLine={false}
                                                tick={{ fill: '#64748b' }}
                                            />
                                            <Tooltip
                                                cursor={{ fill: '#f8fafc' }}
                                                contentStyle={{ borderRadius: '12px', border: 'none', boxShadow: '0 10px 15px -3px rgb(0 0 0 / 0.1)' }}
                                            />
                                            <Legend iconType="circle" wrapperStyle={{ fontSize: '10px', paddingTop: '20px' }} />
                                            {data?.filterOptions?.reasons.slice(0, 5).map((reason, idx) => (
                                                <Bar
                                                    key={reason}
                                                    dataKey={reason}
                                                    stackId="a"
                                                    fill={['#002554', '#10b981', '#f59e0b', '#6366f1', '#ec4899'][idx % 5]}
                                                    radius={idx === 0 ? [0, 0, 0, 0] : [0, 0, 0, 0]}
                                                />
                                            ))}
                                        </BarChart>
                                    </ResponsiveContainer>
                                </div>
                            </div>
                        </div>
                    </div>
                ) : (
                    <div className="animate-in fade-in slide-in-from-bottom-4 duration-500">
                        <div className="bg-white dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-[24px] shadow-sm overflow-hidden">
                            <div className="overflow-x-auto custom-scrollbar">
                                <table className="w-full text-left border-collapse min-w-[1200px]">
                                    <thead>
                                        <tr className="bg-slate-50 dark:bg-slate-900/50 border-b border-slate-100 dark:border-slate-800">
                                            <th className="px-6 py-4 text-[10px] font-black text-slate-400 uppercase tracking-widest w-24">Date</th>
                                            <th className="px-6 py-4 text-[10px] font-black text-slate-400 uppercase tracking-widest w-32">Product</th>
                                            <th className="px-6 py-4 text-[10px] font-black text-slate-400 uppercase tracking-widest flex-1">Reason / Description</th>
                                            <th className="px-6 py-4 text-[10px] font-black text-slate-400 uppercase tracking-widest w-20 text-center">Qty</th>
                                            <th className="px-6 py-4 text-[10px] font-black text-slate-400 uppercase tracking-widest w-32">Process</th>
                                            <th className="px-6 py-4 text-[10px] font-black text-slate-400 uppercase tracking-widest w-24">Status</th>
                                        </tr>
                                    </thead>
                                    <tbody className="divide-y divide-slate-50 dark:divide-slate-800">
                                        {data?.records?.map((record, i) => (
                                            <tr
                                                key={i}
                                                onClick={() => setSelectedRecord(record)}
                                                className="group hover:bg-slate-50/80 dark:hover:bg-slate-900/40 cursor-pointer transition-all border-b border-transparent hover:border-slate-100 dark:hover:border-slate-800"
                                            >
                                                <td className="px-6 py-4">
                                                    <span className="text-[11px] font-bold text-slate-500 dark:text-slate-400">
                                                        {record.date}
                                                    </span>
                                                </td>
                                                <td className="px-6 py-4">
                                                    <span className="text-xs font-black text-slate-800 dark:text-white group-hover:text-medtronic transition-colors">
                                                        {record.product}
                                                    </span>
                                                </td>
                                                <td className="px-6 py-4">
                                                    <div className="flex flex-col">
                                                        <span className="text-xs font-bold text-slate-700 dark:text-slate-200 line-clamp-1">
                                                            {record.reason}
                                                        </span>
                                                        <span className="text-[10px] font-medium text-slate-400 dark:text-slate-500 line-clamp-1">
                                                            {record.nc_description || record.feature_name || 'No description'}
                                                        </span>
                                                    </div>
                                                </td>
                                                <td className="px-6 py-4 text-center">
                                                    <span className="px-2 py-1 bg-red-50 dark:bg-red-500/10 text-red-600 dark:text-red-400 text-[10px] font-black rounded-lg">
                                                        {record.qty}
                                                    </span>
                                                </td>
                                                <td className="px-6 py-4">
                                                    <span className="text-[10px] font-bold text-slate-500 dark:text-slate-400 uppercase tracking-wider">
                                                        {record.process}
                                                    </span>
                                                </td>
                                                <td className="px-6 py-4">
                                                    <StatusBadge status={record.status} />
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                )}
            </div>

            {/* Detail Modal */}
            {selectedRecord && (
                <div
                    className="fixed inset-0 z-[120] flex items-center justify-center p-4 bg-slate-950/40 backdrop-blur-[6px] animate-in fade-in duration-300"
                    onClick={(e) => {
                        if (e.target === e.currentTarget) setSelectedRecord(null);
                    }}
                >
                    <div className="bg-white dark:bg-slate-900 w-full max-w-2xl rounded-[32px] shadow-2xl overflow-hidden border border-slate-200 dark:border-slate-800 animate-in zoom-in-95 duration-300 flex flex-col max-h-[95vh]">
                        <div className="h-1.5 w-full bg-red-500" />
                        <div className="p-10 overflow-y-auto custom-scrollbar">
                            <div className="flex items-start justify-between mb-8">
                                <div className="space-y-1">
                                    <div className="flex items-center gap-2 mb-2">
                                        <span className="px-3 py-1 bg-red-50 dark:bg-red-500/10 text-red-600 dark:text-red-400 text-[10px] font-black rounded-full uppercase tracking-wider">
                                            {selectedRecord.reason}
                                        </span>
                                        <span className="text-[10px] font-bold text-slate-400">•</span>
                                        <span className="text-[10px] font-bold text-slate-400">{selectedRecord.date}</span>
                                    </div>
                                    <h2 className="text-2xl font-black text-slate-900 dark:text-white tracking-tight">
                                        NC Detail: {selectedRecord.product}
                                    </h2>
                                </div>
                                <button
                                    onClick={() => setSelectedRecord(null)}
                                    className="p-2 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-full transition-colors text-slate-400"
                                >
                                    <X size={20} />
                                </button>
                            </div>

                            <div className="space-y-8">
                                <div className="p-6 bg-slate-50 dark:bg-slate-800/50 rounded-2xl border border-slate-100 dark:border-slate-800">
                                    <h3 className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-3">Description / Features</h3>
                                    <p className="text-sm font-bold text-slate-700 dark:text-slate-200 leading-relaxed italic">
                                        "{selectedRecord.nc_description || selectedRecord.feature_name || 'No detailed description'}"
                                    </p>
                                </div>

                                <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
                                    <DetailItem label="Batch Number" value={selectedRecord.batch} icon={<Database size={12} />} />
                                    <DetailItem label="Machine" value={selectedRecord.machine} icon={<Activity size={12} />} />
                                    <DetailItem label="Recorder" value={selectedRecord.recorder} icon={<User size={12} />} />
                                    <DetailItem label="Operation" value={selectedRecord.process} icon={<Tag size={12} />} />
                                    <DetailItem label="Quantity" value={selectedRecord.qty.toString()} icon={<AlertTriangle size={12} />} />
                                    <DetailItem label="Status" value={selectedRecord.status} icon={<CheckCircle2 size={12} />} />
                                </div>

                                {selectedRecord.action_taken && (
                                    <div className="space-y-3 pt-4 border-t border-slate-100 dark:border-slate-800">
                                        <h3 className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Action Taken / 纠正措施</h3>
                                        <div className="p-4 bg-emerald-50/50 dark:bg-emerald-500/5 rounded-xl border border-emerald-100/50 dark:border-emerald-500/10">
                                            <p className="text-sm font-bold text-emerald-700 dark:text-emerald-400">
                                                {selectedRecord.action_taken}
                                            </p>
                                        </div>
                                    </div>
                                )}
                            </div>
                        </div>
                    </div>
                </div>
            )}
        </StandardPageLayout>
    );
}

function Card({ icon, title, value, subtitle, isLabel = false }: { icon: React.ReactNode, title: string, value: string | number, subtitle: string, isLabel?: boolean }) {
    return (
        <div className="bg-white dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-[24px] p-6 shadow-sm hover:shadow-md transition-all group">
            <div className="flex items-center gap-4 mb-4">
                <div className="p-2.5 bg-slate-50 dark:bg-slate-900 rounded-xl group-hover:scale-110 transition-transform">
                    {icon}
                </div>
                <h4 className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest">
                    {title}
                </h4>
            </div>
            <div className="space-y-1">
                <div className={`text-2xl font-black text-slate-900 dark:text-white tracking-tight ${isLabel ? 'text-lg truncate' : ''}`}>
                    {value}
                </div>
                <p className="text-[10px] font-bold text-slate-400">
                    {subtitle}
                </p>
            </div>
        </div>
    );
}

function DetailItem({ label, value, icon }: { label: string, value: string, icon: React.ReactNode }) {
    return (
        <div className="space-y-1.5">
            <h4 className="text-[9px] font-black text-slate-400 uppercase tracking-widest flex items-center gap-1.5">
                {icon}
                {label}
            </h4>
            <p className="text-xs font-bold text-slate-800 dark:text-slate-100 truncate">
                {value || 'N/A'}
            </p>
        </div>
    );
}

function StatusBadge({ status }: { status: string }) {
    const isCompleted = status === '完工' || status === 'Closed' || status === 'Completed';
    const isInProgress = status === '进行中' || status === 'In Progress' || status === '作业中';

    return (
        <div className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full border ${isCompleted
            ? 'bg-emerald-50 dark:bg-emerald-500/10 border-emerald-100 dark:border-emerald-500/20 text-emerald-600 dark:text-emerald-400'
            : isInProgress
                ? 'bg-blue-50 dark:bg-blue-500/10 border-blue-100 dark:border-blue-500/20 text-blue-600 dark:text-blue-400'
                : 'bg-slate-50 dark:bg-slate-800 border-slate-100 dark:border-slate-700 text-slate-500'
            }`}>
            <div className={`w-1.5 h-1.5 rounded-full ${isCompleted ? 'bg-emerald-500' : isInProgress ? 'bg-blue-500' : 'bg-slate-400'}`} />
            <span className="text-[10px] font-black uppercase tracking-wider">{status || 'Unknown'}</span>
        </div>
    );
}
