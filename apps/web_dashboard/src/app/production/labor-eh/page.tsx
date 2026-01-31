'use client';

import React, { useState, useEffect, useRef } from 'react';
import {
    Calendar, RefreshCw, BarChart3, Clock, Table as TableIcon,
    AlertCircle, ArrowRight, TrendingUp, TrendingDown, Factory, Activity, Download, ChevronLeft, ChevronRight,
    ChevronDown, ChevronUp, ChevronsDown, ChevronsUp
} from 'lucide-react';
import { useUI } from '@/context/UIContext';

// --- Types ---
interface CalendarData {
    years: string[];
    months: Record<string, string[]>;
    weeks: Record<string, number[]>;
    currentFiscalInfo?: {
        fiscal_year: string;
        fiscal_month: string;
        fiscal_week: number;
    };
}

interface LaborData {
    summary: {
        actualEH: number;
        targetEH: number;
        actualDays: number;
        targetDays: number;
        ytdActualEH: number;
        ytdTargetEH: number;
        actualAvgEH: number;
        targetAvgEH: number;
    };
    trend: Array<{
        Label: string;
        SDate: string;
        actualEH: number;
        targetEH: number;
    }>;
    anomalies: Array<{
        Material: string;
        actualEH: number;
        orderCount: number;
    }>;
    details: any[];
    areaOperationDetail?: Array<{
        area: string;
        totalHours: number;
        percentage: number;
        operations: Array<{
            operationName: string;
            yesterday: number;
            weeklyData: Array<{
                fiscalWeek: number;
                hours: number;
            }>;
        }>;
    }>;
    filterOptions?: {
        areas: string[];
        operations: string[];
        schedulers: string[];
    };
};


// --- Components ---
function FilterDropdown({ title, options, selected, onChange, placeholder = "Select...", emptyText = "No options" }: {
    title: string;
    options: string[];
    selected: string[];
    onChange: (selected: string[]) => void;
    placeholder?: string;
    emptyText?: string;
}) {
    const [isOpen, setIsOpen] = useState(false);
    const containerRef = useRef<HTMLDivElement>(null);
    const [searchTerm, setSearchTerm] = useState('');

    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
                setIsOpen(false);
            }
        };
        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);

    const filteredOptions = options.filter(opt =>
        opt.toLowerCase().includes(searchTerm.toLowerCase())
    );

    const toggleOption = (opt: string) => {
        if (selected.includes(opt)) {
            onChange(selected.filter(s => s !== opt));
        } else {
            onChange([...selected, opt]);
        }
    };

    return (
        <div className="relative group" ref={containerRef}>
            <label className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-1.5 block ml-1">{title}</label>
            <button
                onClick={() => setIsOpen(!isOpen)}
                className={`w-full flex items-center justify-between px-3 py-2 rounded-xl text-left transition-all border ${selected.length > 0 || isOpen
                    ? 'bg-white dark:bg-slate-800 border-medtronic/50 shadow-sm ring-1 ring-medtronic/10'
                    : 'bg-slate-50 dark:bg-slate-800/50 border-slate-200 dark:border-slate-700 hover:border-slate-300 dark:hover:border-slate-600'
                    }`}
            >
                <div className="flex-1 truncate mr-2">
                    {selected.length === 0 ? (
                        <span className="text-xs text-slate-400 font-semibold">{placeholder}</span>
                    ) : (
                        <span className="text-xs font-bold text-slate-700 dark:text-slate-200">
                            已选 {selected.length} 项 (Selected)
                        </span>
                    )}
                </div>
                <ChevronDown size={14} className={`text-slate-400 transition-transform duration-200 ${isOpen ? 'rotate-180' : ''}`} />
            </button>

            {isOpen && (
                <div className="absolute left-0 top-full mt-1 w-full bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl shadow-xl z-50 overflow-hidden flex flex-col max-h-[300px]">
                    <div className="p-2 border-b border-slate-100 dark:border-slate-800 bg-slate-50/50 dark:bg-slate-900/50">
                        <input
                            type="text"
                            placeholder="搜索..."
                            value={searchTerm}
                            onChange={(e) => setSearchTerm(e.target.value)}
                            className="w-full bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg px-2 py-1.5 text-xs outline-none focus:border-medtronic transition-colors"
                            autoFocus
                        />
                    </div>
                    <div className="overflow-y-auto p-1.5 space-y-0.5 flex-1">
                        <button
                            onClick={() => onChange(selected.length === options.length ? [] : [...options])}
                            className="w-full flex items-center gap-2 px-2 py-1.5 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-800 transition-colors text-left"
                        >
                            <div className={`w-3.5 h-3.5 rounded border-2 flex items-center justify-center transition-colors ${selected.length === options.length && options.length > 0
                                ? 'bg-medtronic border-medtronic text-white'
                                : 'border-slate-300 dark:border-slate-600'
                                }`}>
                                {selected.length === options.length && options.length > 0 && <span className="text-[10px]">✓</span>}
                            </div>
                            <span className="text-xs font-bold text-medtronic">全选 (Select All)</span>
                        </button>
                        <div className="h-px bg-slate-100 dark:bg-slate-800 my-1 mx-2" />
                        {filteredOptions.length > 0 ? (
                            filteredOptions.map(opt => (
                                <button
                                    key={opt}
                                    onClick={() => toggleOption(opt)}
                                    className="w-full flex items-center gap-2 px-2 py-1.5 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-800 transition-colors text-left group/item"
                                >
                                    <div className={`w-3.5 h-3.5 rounded border-2 flex items-center justify-center transition-colors ${selected.includes(opt)
                                        ? 'bg-medtronic border-medtronic text-white'
                                        : 'border-slate-300 dark:border-slate-600 group-hover/item:border-medtronic/50'
                                        }`}>
                                        {selected.includes(opt) && <span className="text-[8px]">✓</span>}
                                    </div>
                                    <span className={`text-xs ${selected.includes(opt) ? 'font-bold text-slate-800 dark:text-slate-100' : 'font-medium text-slate-600 dark:text-slate-400'}`}>
                                        {opt}
                                    </span>
                                </button>
                            ))
                        ) : (
                            <div className="px-2 py-4 text-center text-xs text-slate-400 italic">
                                {options.length === 0 ? emptyText : 'No matches'}
                            </div>
                        )}
                    </div>
                </div>
            )}
        </div>
    );

}

// --- Main Component ---
export default function LaborEhPage() {
    // --- Refs ---
    const diagnosticRef = useRef<HTMLDivElement>(null);

    // --- Calendar & Filter State ---
    const [calendar, setCalendar] = useState<CalendarData | null>(null);
    const [granularity, setGranularity] = useState<'week' | 'month' | 'year' | 'custom'>('month');
    const [selectedYear, setSelectedYear] = useState('FY26');
    const [selectedMonth, setSelectedMonth] = useState('');
    const [selectedWeeks, setSelectedWeeks] = useState<number[]>([]);
    const [selectedPlant, setSelectedPlant] = useState('1303');
    const [expandedAreas, setExpandedAreas] = useState<Set<string>>(new Set());
    const [customRange, setCustomRange] = useState({ start: '', end: '' });
    const [selectedAreas, setSelectedAreas] = useState<string[]>([]);
    const [selectedProcesses, setSelectedProcesses] = useState<string[]>([]);
    const [selectedSchedulers, setSelectedSchedulers] = useState<string[]>([]);

    // --- Handlers for Smart Filter Logic ---
    const handlePlantChange = (p: string) => {
        if (selectedPlant !== p) {
            setSelectedPlant(p);
            // Clear dependent filters
            setSelectedAreas([]);
            setSelectedProcesses([]);
            setSelectedSchedulers([]);
        }
    };

    const handleAreaChange = (areas: string[]) => {
        setSelectedAreas(areas);
        // If areas change, process selection might be invalid or should be cleared
        // User requested: "Area switches... need to clear Process and Product Line"
        // Clearing Product Line might be too aggressive if Product Line is "above" Area in hierarchy?
        // But user explicitly said: "Area switches... need to clear Process and Product Line"
        // Wait, User said: "当工厂切换时 区域切换的时候 需要清除工序和产品线"
        // This could mean:
        // 1. Factory Switch -> Clear Area, Process, Product Line
        // 2. Area Switch -> Clear Process, Product Line
        // I will follow instruction.
        setSelectedProcesses([]);
        setSelectedSchedulers([]);
    };

    const handleResetFilters = () => {
        setSelectedAreas([]);
        setSelectedProcesses([]);
        setSelectedSchedulers([]);
        // We don't reset Date/Plant usually, but user said "Reset Filters" (Screening bottom).
        // Usually resets detailed filters.
    };

    // --- Data State ---
    const [data, setData] = useState<LaborData | null>(null);
    const [error, setError] = useState<string | null>(null);
    const { isFilterOpen } = useUI();

    // 1. Initial Load: Fetch Calendar Options & Set Default Month
    useEffect(() => {
        async function fetchCalendar() {
            try {
                const res = await fetch('/api/production/calendar');
                if (!res.ok) throw new Error('Failed to fetch calendar');
                const json = await res.json();
                setCalendar(json);

                if (json.currentFiscalInfo) {
                    setSelectedYear(json.currentFiscalInfo.fiscal_year);
                    // Default to month view if it's the start of the week? 
                    // Actually, let's just use what they are in.
                    handleMonthChange(json.currentFiscalInfo.fiscal_month);

                    // If current week has no data, maybe default to previous? 
                    // For now, keep it simple but ensure we have at least one week selected
                    setSelectedWeeks([json.currentFiscalInfo.fiscal_week]);
                } else if (json.years && json.years.length > 0) {
                    const now = new Date();
                    const currentMonthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
                    const currentMonthName = currentMonthNames[now.getMonth()];
                    const currentYearShort = now.getFullYear().toString().slice(-2);

                    const matchedYear = json.years.find((y: string) => y.includes(currentYearShort)) || json.years[0];
                    setSelectedYear(matchedYear);
                    const m = json.months[matchedYear]?.find((month: string) => month.includes(currentMonthName)) || json.months[matchedYear]?.[0] || '';
                    setSelectedMonth(m);
                    if (m && json.weeks[m]) {
                        setSelectedWeeks([json.weeks[m][0]]);
                    }
                }
            } catch (err: any) {
                setError(`Calendar Error: ${err.message}`);
            }
        }
        fetchCalendar();
    }, []);

    useEffect(() => {
        if (granularity === 'week' && selectedWeeks.length === 0) return;
        if (granularity === 'month' && !selectedMonth) return;
        if (granularity === 'custom' && (!customRange.start || !customRange.end)) return;

        async function fetchLaborEHData() {
            setError(null);
            try {
                let url = `/api/production/labor-eh?granularity=${granularity}&year=${selectedYear}&plant=${selectedPlant}`;
                if (granularity === 'week') url += `&week=${selectedWeeks.join(',')}`;
                if (granularity === 'month') url += `&month=${selectedMonth}`;
                if (granularity === 'custom') url += `&startDate=${customRange.start}&endDate=${customRange.end}`;
                if (selectedAreas.length > 0) url += `&areas=${selectedAreas.join(',')}`;
                if (selectedProcesses.length > 0) url += `&processes=${selectedProcesses.join(',')}`;
                if (selectedSchedulers.length > 0) url += `&productSchedulers=${selectedSchedulers.join(',')}`;

                console.log('🔍 Fetching Labor EH data from:', url);
                const res = await fetch(url);
                const json = await res.json();
                console.log('📊 API Response:', json);
                console.log('📈 Trend data length:', json.trend?.length);
                console.log('📈 First trend item:', json.trend?.[0]);

                if (!res.ok) throw new Error(json.error || 'Failed to fetch SQL data');
                setData(json);
            } catch (err: any) {
                console.error('❌ Fetch error:', err);
                setError(err.message);
            }
        }
        fetchLaborEHData();
    }, [selectedYear, selectedMonth, selectedWeeks, selectedPlant, granularity, customRange, selectedAreas, selectedProcesses, selectedSchedulers]);

    // Debug effect to log data changes
    useEffect(() => {
        console.log('🎨 Data state updated:', data);
        console.log('📈 Trend array:', data?.trend);
        console.log('📈 Trend length:', data?.trend?.length);
    }, [data]);

    // Clear incompatible processes when areas change
    useEffect(() => {
        if (selectedAreas.length > 0 && selectedProcesses.length > 0) {
            const availableOps = data?.filterOptions?.operations || [];
            const validProcesses = selectedProcesses.filter(p => availableOps.includes(p));
            if (validProcesses.length !== selectedProcesses.length) {
                setSelectedProcesses(validProcesses);
            }
        }
    }, [selectedAreas, data?.filterOptions?.operations]);

    const handleExportCSV = () => {
        if (!data?.details) return;
        const headers = ["PostingDate", "OrderNumber", "Material", "actualEH", "WorkCenter", "Plant"];
        const csvContent = [
            headers.join(","),
            ...data.details.map(row => [
                new Date(row.PostingDate).toLocaleDateString(),
                row.OrderNumber,
                row.Material,
                row.actualEH.toFixed(2),
                row.WorkCenter,
                row.Plant
            ].join(","))
        ].join("\n");
        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.setAttribute("href", url);
        link.setAttribute("download", `Labor_EH_Export_${new Date().toISOString().split('T')[0]}.csv`);
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    };

    const scrollToDiagnostic = () => {
        diagnosticRef.current?.scrollIntoView({ behavior: 'smooth' });
    };

    const handleMonthChange = (month: string) => {
        setSelectedMonth(month);
        if (calendar?.weeks[month] && selectedWeeks.length === 0) {
            setSelectedWeeks([calendar.weeks[month][0]]);
        }
    };

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
            handleMonthChange(months[nextIdx]);
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

    const getDiff = (actual: number, target: number) => {
        const diff = actual - target;
        const percent = target > 0 ? (Math.abs(diff) / target * 100).toFixed(1) : '0';
        return {
            val: diff.toFixed(1),
            percent: percent + '%',
            isPositive: diff >= 0
        };
    };

    return (
        <div className="flex w-full h-full overflow-hidden bg-slate-50 dark:bg-transparent">
            {/* Sidebar Filters */}
            <aside className={`${isFilterOpen ? 'w-72 opacity-100' : 'w-0 opacity-0 overflow-hidden'} border-r border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 flex flex-col transition-all duration-300 shrink-0 shadow-sm z-20`}>
                <div className="p-6 space-y-6 w-72 flex flex-col h-full overflow-hidden">
                    {/* 1. Time Granularity */}
                    <section>
                        <h3 className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-4 flex items-center gap-2">
                            时间粒度 (Granularity)
                        </h3>
                        <div className="flex flex-wrap gap-2">
                            {['week', 'month', 'year', 'custom'].map(g => (
                                <button
                                    key={g}
                                    onClick={() => setGranularity(g as any)}
                                    className={`flex-1 py-2 px-2 rounded-lg text-[10px] font-black uppercase tracking-tighter transition-all ${granularity === g
                                        ? 'bg-medtronic text-white shadow-lg'
                                        : 'bg-slate-50 dark:bg-slate-800 text-slate-500'}`}
                                >
                                    {g === 'week' ? '按周' : g === 'month' ? '按月' : g === 'year' ? '按年' : '自由'}
                                </button>
                            ))}
                        </div>
                    </section>

                    {/* 2. Time Selection (Moved Up) */}
                    <section>
                        <h3 className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-4 flex items-center gap-2">
                            <Calendar size={12} /> 时间选择
                        </h3>
                        <div className="space-y-4">
                            {granularity !== 'custom' && (
                                <div className="flex items-center bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-700 rounded-xl group transition-all hover:border-medtronic/30">
                                    <button onClick={() => handleYearNavigate(1)} className="p-3 text-slate-700 dark:text-slate-300 hover:text-medtronic transition-colors border-r border-slate-200/50 dark:border-slate-700"><ChevronLeft size={14} /></button>
                                    <div className="flex-1 relative">
                                        <select value={selectedYear} onChange={(e) => setSelectedYear(e.target.value)} className="w-full bg-transparent text-slate-800 dark:text-slate-200 px-4 py-2 text-xs font-bold outline-none appearance-none cursor-pointer">
                                            {calendar?.years?.map(y => <option key={y} value={y}>{y}</option>)}
                                        </select>
                                        <div className="absolute right-3 top-1/2 -translate-y-1/2 pointer-events-none text-slate-300 dark:text-slate-600"><ChevronRight size={10} className="rotate-90" /></div>
                                    </div>
                                    <button onClick={() => handleYearNavigate(-1)} className="p-3 text-slate-700 dark:text-slate-300 hover:text-medtronic transition-colors border-l border-slate-200/50 dark:border-slate-700"><ChevronRight size={14} /></button>
                                </div>
                            )}

                            {(granularity === 'month' || granularity === 'week') && (
                                <div className="flex items-center bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-700 rounded-xl group transition-all hover:border-medtronic/30">
                                    <button onClick={() => handleMonthNavigate(-1)} className="p-3 text-slate-700 dark:text-slate-300 hover:text-medtronic transition-colors border-r border-slate-200/50 dark:border-slate-700"><ChevronLeft size={14} /></button>
                                    <div className="flex-1 relative">
                                        <select value={selectedMonth} onChange={(e) => handleMonthChange(e.target.value)} className="w-full bg-transparent text-slate-800 dark:text-slate-200 px-4 py-2 text-xs font-bold outline-none appearance-none cursor-pointer">
                                            <option value="" disabled>选择月份</option>
                                            {calendar?.months?.[selectedYear]?.map(m => <option key={m} value={m}>{m}</option>)}
                                        </select>
                                        <div className="absolute right-3 top-1/2 -translate-y-1/2 pointer-events-none text-slate-300 dark:text-slate-600"><ChevronRight size={10} className="rotate-90" /></div>
                                    </div>
                                    <button onClick={() => handleMonthNavigate(1)} className="p-3 text-slate-700 dark:text-slate-300 hover:text-medtronic transition-colors border-l border-slate-200/50 dark:border-slate-700"><ChevronRight size={14} /></button>
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
                                                : 'bg-slate-50 dark:bg-slate-800 text-slate-400 hover:bg-slate-100 hover:text-slate-600'}`}
                                        >
                                            W{w}
                                        </button>
                                    ))}
                                </div>
                            )}

                            {granularity === 'custom' && (
                                <div className="space-y-3">
                                    <input type="date" value={customRange.start} onChange={(e) => setCustomRange(p => ({ ...p, start: e.target.value }))} className="w-full bg-slate-50 dark:bg-slate-800 border-none rounded-xl px-4 py-3 text-sm font-black outline-none focus:ring-2 focus:ring-medtronic" />
                                    <input type="date" value={customRange.end} onChange={(e) => setCustomRange(p => ({ ...p, end: e.target.value }))} className="w-full bg-slate-50 dark:bg-slate-800 border-none rounded-xl px-4 py-3 text-sm font-black outline-none focus:ring-2 focus:ring-medtronic" />
                                </div>
                            )}
                        </div>
                    </section>

                    <div className="h-px bg-slate-200 dark:bg-slate-800 mx-2" />

                    {/* 3. Factory Selection */}
                    <section>
                        <h3 className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-4 flex items-center gap-2">
                            <Factory size={12} /> 工厂选择
                        </h3>
                        <div className="flex gap-2">
                            {['1303', '9997'].map(p => (
                                <button
                                    key={p}
                                    onClick={() => handlePlantChange(p)}
                                    className={`flex-1 py-2 px-3 rounded-xl text-xs font-bold transition-all ${selectedPlant === p
                                        ? 'bg-medtronic text-white shadow-lg shadow-blue-500/20'
                                        : 'bg-slate-50 dark:bg-slate-800 text-slate-600 dark:text-slate-400 border border-slate-200 dark:border-slate-700'}`}
                                >
                                    {p === '1303' ? '常州 1303' : '康辉 9997'}
                                </button>
                            ))}
                        </div>
                    </section>

                    {/* 4. Product Line (Scheduler) Dropdown */}
                    <FilterDropdown
                        title="产品线 (Product Line)"
                        options={data?.filterOptions?.schedulers || []}
                        selected={selectedSchedulers}
                        onChange={setSelectedSchedulers}
                        placeholder="选择产品线..."
                        emptyText="无可用产品线"
                    />

                    {/* 5. Area Dropdown */}
                    <FilterDropdown
                        title="区域选择 (Area)"
                        options={data?.filterOptions?.areas || []}
                        selected={selectedAreas}
                        onChange={handleAreaChange}
                        placeholder="选择区域..."
                        emptyText="无可用区域"
                    />

                    {/* 6. Process Dropdown */}
                    <FilterDropdown
                        title="工序选择 (Operation)"
                        options={data?.filterOptions?.operations || []}
                        selected={selectedProcesses}
                        onChange={setSelectedProcesses}
                        placeholder="选择工序..."
                        emptyText="无可用工序"
                    />

                    <div className="flex-1" />

                    {/* Reset Button */}
                    <button
                        onClick={handleResetFilters}
                        className="w-full flex items-center justify-center gap-2 py-3 mt-4 rounded-xl border border-slate-200 dark:border-slate-700 text-slate-500 hover:text-medtronic hover:bg-slate-50 dark:hover:bg-slate-800 transition-all text-xs font-bold group"
                    >
                        <RefreshCw size={14} className="group-hover:rotate-180 transition-transform duration-500" />
                        重置筛选器 (Reset Filters)
                    </button>
                </div>
            </aside>

            {/* Main Content (Scrollable Area) */}
            <div className="flex-1 overflow-y-auto scroll-smooth">
                {/* --- FOLD 1: EXECUTIVE OVERVIEW --- */}
                <div className="min-h-screen p-8 space-y-6 flex flex-col">

                    {/* KPI Tiles */}
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                        <KpiTile
                            title="Average Hours"
                            value={data?.summary?.actualAvgEH || 0}
                            target={data?.summary?.targetAvgEH}
                            ratio={data?.summary && data.summary.targetAvgEH > 0 ? (data.summary.actualAvgEH / data.summary.targetAvgEH) * 100 : undefined}
                            diff={data ? data.summary.actualAvgEH - data.summary.targetAvgEH : undefined}
                            unit="H"
                            color="blue"
                            icon={<Clock size={24} />}
                        />
                        <KpiTile
                            title="Days Progress"
                            value={data?.summary?.actualDays || 0}
                            target={data?.summary?.targetDays}
                            ratio={data?.summary && data.summary.targetDays > 0 ? (data.summary.actualDays / data.summary.targetDays) * 100 : undefined}
                            diff={data ? data.summary.actualDays - data.summary.targetDays : undefined}
                            unit="D"
                            color="emerald"
                            showStatusColor={false} // Neutral for Days Progress
                            icon={<Calendar size={24} />}
                        />
                        <KpiTile
                            title="Period Total EH"
                            value={data?.summary?.actualEH || 0}
                            target={data?.summary?.targetEH}
                            ratio={data?.summary && data.summary.targetEH > 0 ? (data.summary.actualEH / data.summary.targetEH) * 100 : undefined}
                            // Total Diff = Actual - (TargetAvg * ActualDays)
                            diff={data?.summary ? data.summary.actualEH - (data.summary.targetAvgEH * data.summary.actualDays) : undefined}
                            unit="H"
                            color="amber"
                            icon={<BarChart3 size={24} />}
                        />
                        <KpiTile
                            title="Fiscal Year EH (YTD)"
                            value={data?.summary?.ytdActualEH || 0}
                            target={data?.summary?.ytdTargetEH}
                            ratio={data?.summary && data.summary.ytdTargetEH > 0 ? (data.summary.ytdActualEH / data.summary.ytdTargetEH) * 100 : undefined}
                            // YTD Diff = ActualYTD - ((AnnualTarget / 300) * ElapsedDays)
                            diff={data?.summary ? data.summary.ytdActualEH - ((data.summary.ytdTargetEH / 300) * data.summary.actualDays) : undefined}
                            unit="H"
                            color="violet"
                            icon={<TrendingUp size={24} />}
                        />
                    </div>

                    {/* Main Trend Chart - Compressed to Half Height */}
                    <div className="ios-widget bg-white dark:bg-slate-900 p-6 flex flex-col shadow-xl shadow-slate-200/50 dark:shadow-none border border-slate-200/50 dark:border-slate-800">
                        <div className="flex justify-between items-center mb-6">
                            <div>
                                <h3 className="text-sm font-black text-slate-800 dark:text-slate-100 uppercase tracking-widest">Daily & Trend EH Analysis</h3>
                                <p className="text-[10px] text-slate-400 font-bold uppercase tracking-[0.2em] mt-1">工时产出对标分布图 ({granularity})</p>
                            </div>
                            <div className="flex gap-6 text-[10px] font-black uppercase tracking-tighter">
                                <span className="flex items-center gap-2 text-medtronic"><div className="w-3 h-3 rounded bg-medtronic" /> 实际记录 (Actual)</span>
                                <span className="flex items-center gap-2 text-slate-300"><div className="w-3 h-3 rounded bg-slate-200 border border-slate-300 border-dashed" /> 计划目标 (Target)</span>
                            </div>
                        </div>
                        <div className="flex items-end gap-3 pb-3 relative h-[140px]">
                            {/* SVG Line Overlay removed per user request */}

                            {!data?.trend || data.trend.length === 0 ? (
                                <div className="flex-1 flex items-center justify-center h-full text-slate-400">
                                    <div className="text-center">
                                        <BarChart3 className="mx-auto mb-2" size={48} />
                                        <p className="text-sm font-bold">No trend data available</p>
                                    </div>
                                </div>
                            ) : (
                                data.trend
                                    .filter(d => {
                                        if (!d.SDate) return false;
                                        const itemDate = new Date(d.SDate);
                                        const today = new Date();
                                        // Simple comparison: year, month, day to avoid timezone issues for "today"
                                        const d1 = new Date(itemDate.getFullYear(), itemDate.getMonth(), itemDate.getDate());
                                        const d2 = new Date(today.getFullYear(), today.getMonth(), today.getDate());
                                        return d1 <= d2;
                                    })
                                    .map((d, i, filteredArray) => {
                                        // Calculate maxVal based ONLY on the filtered items
                                        const allTrendValues = filteredArray.flatMap(t => [Number(t.actualEH) || 0, Number(t.targetEH) || 0]);
                                        const maxVal = Math.max(...allTrendValues, 100) * 1.1;

                                        const label = d.Label;
                                        const actual = Number(d.actualEH) || 0;
                                        const target = Number(d.targetEH) || 0;
                                        const isAchievement = actual >= target;

                                        // Calculate heights as percentages
                                        const actualPct = (actual / maxVal) * 100;
                                        const targetPct = (target / maxVal) * 100;

                                        return (
                                            <div key={i} className="flex-1 flex flex-col items-center gap-1 group relative h-full">
                                                <div className="w-full bg-transparent overflow-visible flex-1 relative">
                                                    {/* Target Bar (Ghost) */}
                                                    <div
                                                        className="absolute bottom-0 w-full bg-slate-300/20 dark:bg-slate-700/10 border-2 border-slate-400/30 border-dashed rounded-t-lg transition-all z-0"
                                                        style={{ height: `${targetPct}%` }}
                                                    />
                                                    {/* Actual Bar */}
                                                    <div
                                                        className={`absolute bottom-0 w-full rounded-t-lg transition-all duration-500 group-hover:brightness-110 z-10 shadow-md ${isAchievement
                                                            ? 'bg-gradient-to-t from-blue-600 via-blue-500 to-medtronic shadow-blue-500/20'
                                                            : 'bg-gradient-to-t from-red-600 via-orange-500 to-amber-500 shadow-red-500/20'
                                                            }`}
                                                        style={{ height: `${actualPct}%`, minHeight: actual > 0 ? '4px' : '0' }}
                                                    >
                                                        {/* Permanent Label */}
                                                        <div className="absolute -top-5 left-1/2 -translate-x-1/2 text-[9px] font-black text-slate-800 dark:text-slate-200 whitespace-nowrap drop-shadow-sm">
                                                            {Math.round(actual)}
                                                        </div>

                                                        {/* Tooltip */}
                                                        <div className="absolute top-[-35px] left-1/2 -translate-x-1/2 opacity-0 group-hover:opacity-100 transition-opacity text-[9px] font-black whitespace-nowrap bg-slate-900 text-white px-2 py-1 rounded shadow-xl z-50 pointer-events-none border border-slate-700">
                                                            ACT: {Math.round(actual)}h | TGT: {Math.round(target)}h
                                                        </div>
                                                    </div>
                                                </div>
                                                <div className="text-[8px] font-black text-slate-500 dark:text-slate-600 uppercase tracking-tighter whitespace-nowrap">{label}</div>
                                            </div>
                                        );
                                    })
                            )}
                        </div>

                        {/* Diagnostic Analysis - Area Distribution & Anomaly Analysis */}
                        <div className="mt-8 pt-6 border-t border-slate-200/50 dark:border-slate-800">
                            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                                {/* Area/Operation Distribution Table */}
                                <div className="bg-slate-50 dark:bg-slate-900/50 p-5 rounded-2xl col-span-full">
                                    <div className="flex justify-between items-center mb-5">
                                        <h3 className="text-xs font-black text-slate-400 uppercase tracking-widest">区域工序分布 (Area/Operation Distribution)</h3>
                                        <div className="flex gap-2">
                                            <button
                                                onClick={() => {
                                                    const allAreas = data?.areaOperationDetail?.map(a => a.area) || [];
                                                    setExpandedAreas(new Set(allAreas));
                                                }}
                                                className="flex items-center gap-1 px-2 py-1 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded text-[10px] font-bold text-slate-500 hover:text-medtronic hover:border-medtronic transition-colors"
                                            >
                                                <ChevronsDown size={12} /> 展开全部
                                            </button>
                                            <button
                                                onClick={() => setExpandedAreas(new Set())}
                                                className="flex items-center gap-1 px-2 py-1 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded text-[10px] font-bold text-slate-500 hover:text-medtronic hover:border-medtronic transition-colors"
                                            >
                                                <ChevronsUp size={12} /> 收起全部
                                            </button>
                                        </div>
                                    </div>

                                    {data?.areaOperationDetail && data.areaOperationDetail.length > 0 ? (
                                        <div className="overflow-x-auto max-h-[600px] overflow-y-auto relative rounded-xl border border-slate-200 dark:border-slate-700">
                                            <table className="w-full text-[10px] border-separate border-spacing-0">
                                                <thead className="sticky top-0 z-20 shadow-sm">
                                                    <tr className="bg-slate-50 dark:bg-slate-900/95">
                                                        <th className="sticky left-0 z-30 bg-slate-50 dark:bg-slate-900/95 text-left py-3 px-3 font-black text-slate-400 uppercase border-b border-slate-200 dark:border-slate-700 min-w-[120px]">区域</th>
                                                        <th className="sticky left-[120px] z-30 bg-slate-50 dark:bg-slate-900/95 text-left py-3 px-3 font-black text-slate-400 uppercase border-b border-slate-200 dark:border-slate-700 min-w-[120px] shadow-r-lg">工序</th>
                                                        <th className="text-right py-3 px-3 font-black text-slate-400 uppercase border-b border-slate-200 dark:border-slate-700 min-w-[80px]">昨天(h)</th>
                                                        {(() => {
                                                            const weeks = new Set<number>();
                                                            data.areaOperationDetail.forEach(area => {
                                                                area.operations.forEach(op => {
                                                                    op.weeklyData.forEach(w => weeks.add(w.fiscalWeek));
                                                                });
                                                            });
                                                            return Array.from(weeks).sort((a, b) => a - b).map(week => (
                                                                <th key={week} className="text-right py-3 px-3 font-black text-slate-400 uppercase whitespace-nowrap border-b border-slate-200 dark:border-slate-700 min-w-[60px]">
                                                                    W{week}
                                                                </th>
                                                            ));
                                                        })()}
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    {(() => {
                                                        const weeks = new Set<number>();
                                                        data.areaOperationDetail?.forEach(a => {
                                                            a.operations.forEach(o => {
                                                                o.weeklyData.forEach(w => weeks.add(w.fiscalWeek));
                                                            });
                                                        });
                                                        const sortedWeeks = Array.from(weeks).sort((a, b) => a - b);

                                                        return data.areaOperationDetail.map((areaData) => {
                                                            const isExpanded = expandedAreas.has(areaData.area);
                                                            // Calculate summaries for the area row
                                                            const yesterdaySum = areaData.operations.reduce((sum, op) => sum + op.yesterday, 0);
                                                            const weeklySums = sortedWeeks.map(week => ({
                                                                week,
                                                                hours: areaData.operations.reduce((sum, op) => {
                                                                    const w = op.weeklyData.find(wd => wd.fiscalWeek === week);
                                                                    return sum + (w ? w.hours : 0);
                                                                }, 0)
                                                            }));

                                                            return (
                                                                <React.Fragment key={areaData.area}>
                                                                    {/* Area Summary Row */}
                                                                    <tr
                                                                        onClick={() => {
                                                                            const newSet = new Set(expandedAreas);
                                                                            if (newSet.has(areaData.area)) newSet.delete(areaData.area);
                                                                            else newSet.add(areaData.area);
                                                                            setExpandedAreas(newSet);
                                                                        }}
                                                                        className="group hover:bg-slate-100/80 dark:hover:bg-slate-800/50 cursor-pointer transition-colors"
                                                                    >
                                                                        <td className="sticky left-0 z-10 bg-white/95 dark:bg-slate-900/95 group-hover:bg-slate-100/95 dark:group-hover:bg-slate-800/95 py-3 px-3 border-b border-slate-100 dark:border-slate-800 align-top">
                                                                            <div className="flex items-start justify-between gap-2">
                                                                                <div className="flex flex-col gap-0.5">
                                                                                    <div className="font-black text-slate-700 dark:text-slate-200 text-xs">{areaData.area}</div>
                                                                                    <div className="text-[10px] font-bold text-slate-500">{Math.round(areaData.totalHours)} h</div>
                                                                                    <div className="text-[10px] font-bold text-medtronic">{areaData.percentage}%</div>
                                                                                </div>
                                                                                <div className="mt-1 text-slate-400 group-hover:text-medtronic transition-colors">
                                                                                    {isExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                                                                                </div>
                                                                            </div>
                                                                        </td>
                                                                        <td className="sticky left-[120px] z-10 bg-white/95 dark:bg-slate-900/95 group-hover:bg-slate-100/95 dark:group-hover:bg-slate-800/95 py-3 px-3 border-b border-slate-100 dark:border-slate-800 font-bold text-slate-400 italic align-middle shadow-r-lg">
                                                                            汇总 (Summary)
                                                                        </td>
                                                                        <td className="text-right py-3 px-3 border-b border-slate-100 dark:border-slate-800 font-black text-slate-700 dark:text-slate-300 align-middle bg-slate-50/30 dark:bg-slate-900/30">
                                                                            {yesterdaySum > 0 ? yesterdaySum.toFixed(1) : '—'}
                                                                        </td>
                                                                        {weeklySums.map(w => (
                                                                            <td key={w.week} className="text-right py-3 px-3 border-b border-slate-100 dark:border-slate-800 font-bold text-slate-600 dark:text-slate-400 align-middle bg-slate-50/30 dark:bg-slate-900/30 whitespace-nowrap">
                                                                                {w.hours > 0 ? w.hours.toFixed(1) : '—'}
                                                                            </td>
                                                                        ))}
                                                                    </tr>

                                                                    {/* Operation Detail Rows (only if expanded) */}
                                                                    {isExpanded && areaData.operations.map((op) => (
                                                                        <tr key={`${areaData.area}-${op.operationName}`} className="hover:bg-slate-50/50 dark:hover:bg-slate-800/30 transition-colors">
                                                                            <td className="sticky left-0 bg-slate-50/50 dark:bg-slate-900/50 border-b border-slate-100 dark:border-slate-800 border-r border-slate-200/50 dark:border-slate-700/50">
                                                                                {/* Empty cell for hierarchy indentation */}
                                                                            </td>
                                                                            <td className="sticky left-[120px] bg-slate-50/80 dark:bg-slate-900/80 py-2 px-3 border-b border-slate-100 dark:border-slate-800 font-semibold text-slate-600 dark:text-slate-400 text-[10px] pl-6 shadow-r-lg">
                                                                                {op.operationName}
                                                                            </td>
                                                                            <td className="text-right py-2 px-3 border-b border-slate-100 dark:border-slate-800 font-medium text-slate-600 dark:text-slate-400">
                                                                                {op.yesterday > 0 ? op.yesterday.toFixed(1) : '—'}
                                                                            </td>
                                                                            {sortedWeeks.map(week => {
                                                                                const weekData = op.weeklyData.find(w => w.fiscalWeek === week);
                                                                                return (
                                                                                    <td key={week} className="text-right py-2 px-3 border-b border-slate-100 dark:border-slate-800 font-medium text-slate-500 dark:text-slate-500 whitespace-nowrap">
                                                                                        {weekData && weekData.hours > 0 ? weekData.hours.toFixed(1) : '—'}
                                                                                    </td>
                                                                                );
                                                                            })}
                                                                        </tr>
                                                                    ))}
                                                                </React.Fragment>
                                                            );
                                                        });
                                                    })()}
                                                </tbody>
                                            </table>
                                        </div>
                                    ) : (
                                        <p className="text-xs text-slate-400 italic">无区域分布数据 (No area distribution data available)</p>
                                    )}
                                </div>

                                {/* Anomaly Table */}
                                <div className="bg-slate-50 dark:bg-slate-900/50 p-5 rounded-2xl">
                                    <h3 className="text-xs font-black text-slate-400 uppercase tracking-widest mb-5">Top Production Anomalies (by Material)</h3>
                                    <div className="space-y-3">
                                        {data?.anomalies?.map((a, i) => (
                                            <div key={i} className="flex items-center justify-between p-3 bg-white dark:bg-slate-900 rounded-xl shadow-sm border border-slate-100 dark:border-slate-800 group hover:border-red-200 transition-all">
                                                <div className="flex items-center gap-3">
                                                    <div className="w-8 h-8 rounded-lg bg-red-50 dark:bg-red-900/20 flex items-center justify-center text-red-500 font-bold text-xs uppercase">
                                                        {a.Material.substring(0, 2)}
                                                    </div>
                                                    <div>
                                                        <div className="text-xs font-black text-slate-800 dark:text-slate-200">{a.Material}</div>
                                                        <div className="text-[10px] text-slate-400 font-bold">{a.orderCount} Orders</div>
                                                    </div>
                                                </div>
                                                <div className="text-right">
                                                    <div className="text-sm font-black text-slate-900 dark:text-white">{a.actualEH.toFixed(1)}h</div>
                                                    <div className="text-[9px] text-red-500 font-black">High Impact</div>
                                                </div>
                                            </div>
                                        ))}
                                        {(!data?.anomalies || data.anomalies.length === 0) && <p className="text-xs text-slate-400 italic">No significant anomalies detected in this period.</p>}
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>



                {/* --- FOLD 3: RECORD DETAILS --- */}
                <div className="min-h-full p-8 bg-slate-50 dark:bg-transparent border-t border-slate-200 dark:border-slate-800">
                    <section className="ios-widget bg-white dark:bg-slate-900 p-8 shadow-2xl shadow-slate-200/50 dark:shadow-none border-none">
                        <div className="flex justify-between items-center mb-8">
                            <div>
                                <h2 className="text-xl font-black text-slate-900 dark:text-white flex items-center gap-3">
                                    <TableIcon className="text-medtronic" /> Operational Record Explorer
                                </h2>
                                <p className="text-[10px] text-slate-400 font-bold uppercase tracking-widest mt-1">流水明细追踪 ({data?.details?.length || 0} Records)</p>
                            </div>
                            <div className="flex gap-3">
                                <button
                                    onClick={handleExportCSV}
                                    className="px-4 py-2 rounded-xl bg-emerald-50 text-emerald-600 text-[10px] font-black tracking-widest uppercase hover:bg-emerald-100 transition-all flex items-center gap-2 border border-emerald-100"
                                >
                                    Export CSV
                                </button>
                                <button className="px-4 py-2 rounded-xl bg-slate-50 dark:bg-slate-800 text-[10px] font-bold text-slate-600 hover:bg-slate-100 transition-all flex items-center gap-2 border border-slate-200">
                                    <RefreshCw size={14} /> Refresh
                                </button>
                            </div>
                        </div>

                        <div className="overflow-x-auto rounded-2xl border border-slate-100 dark:border-slate-800">
                            <table className="w-full text-left border-collapse">
                                <thead>
                                    <tr className="bg-slate-50 dark:bg-slate-800/50">
                                        {['Posting Date', 'Order No.', 'Material', 'Earned Hours', 'Work Center', 'Plant'].map(th => (
                                            <th key={th} className="px-6 py-4 text-[10px] font-black text-slate-400 uppercase tracking-widest border-b border-slate-100 dark:border-slate-800">{th}</th>
                                        ))}
                                    </tr>
                                </thead>
                                <tbody className="text-xs font-bold text-slate-700 dark:text-slate-300">
                                    {data?.details?.map((row, i) => (
                                        <tr key={i} className="hover:bg-slate-50 dark:hover:bg-slate-800/30 transition-colors group">
                                            <td className="px-6 py-4 border-b border-slate-50 dark:border-slate-800/50">{new Date(row.PostingDate).toLocaleDateString()}</td>
                                            <td className="px-6 py-4 border-b border-slate-50 dark:border-slate-800/50 text-medtronic">{row.OrderNumber}</td>
                                            <td className="px-6 py-4 border-b border-slate-50 dark:border-slate-800/50">{row.Material}</td>
                                            <td className="px-6 py-4 border-b border-slate-50 dark:border-slate-800/50 font-black">{Math.round(Number(row.actualEH))}h</td>
                                            <td className="px-6 py-4 border-b border-slate-50 dark:border-slate-800/50">{row.WorkCenter}</td>
                                            <td className="px-6 py-4 border-b border-slate-50 dark:border-slate-800/50 underline decoration-slate-200">{row.Plant}</td>
                                        </tr>
                                    ))}
                                    {(!data?.details || data.details.length === 0) && (
                                        <tr>
                                            <td colSpan={6} className="px-6 py-20 text-center text-slate-400 italic">No detailed records found for this period.</td>
                                        </tr>
                                    )}
                                </tbody>
                            </table>
                        </div>
                    </section>
                </div>
            </div>

            {/* Error Overlay */}
            {error && (
                <div className="fixed bottom-6 right-6 bg-red-500 text-white px-6 py-4 rounded-2xl shadow-2xl flex items-center gap-4 z-50 animate-in slide-in-from-bottom-10">
                    <AlertCircle size={20} />
                    <div>
                        <div className="text-xs font-black">Connection Failure</div>
                        <div className="text-[10px] opacity-90">{error}</div>
                    </div>
                </div>
            )}
        </div>
    );
}

// --- Sub Components ---

function KpiTile({ title, value, target, ratio, diff, unit, icon, color = 'blue', showStatusColor = true }: {
    title: string,
    value: number,
    target?: number,
    ratio?: number,
    diff?: number,
    unit?: string,
    icon: React.ReactNode,
    color?: 'blue' | 'emerald' | 'amber' | 'violet',
    showStatusColor?: boolean
}) {
    // Labor Hours are integers, Ratios are 1 decimal
    const formattedValue = Math.round(value).toLocaleString();
    const formattedTarget = target ? Math.round(target).toLocaleString() : '--';
    const formattedDiff = diff !== undefined ? (diff >= 0 ? '+' : '') + Math.round(diff).toLocaleString() : '--';
    const formattedRatio = ratio !== undefined ? ratio.toFixed(1) + '%' : '--';

    const isPositive = diff !== undefined ? diff >= 0 : true;

    const colorClasses = {
        blue: { border: 'bg-blue-500', icon: 'text-blue-500' },
        emerald: { border: 'bg-emerald-500', icon: 'text-emerald-500' },
        amber: { border: 'bg-amber-500', icon: 'text-amber-500' },
        violet: { border: 'bg-violet-500', icon: 'text-violet-500' }
    };

    return (
        <div className="ios-widget w-full p-6 bg-white dark:bg-slate-900 border border-slate-200/50 dark:border-slate-800 shadow-xl shadow-slate-200/30 dark:shadow-none relative overflow-hidden group hover:translate-y-[-4px] transition-all flex flex-col items-center min-h-[160px]">
            {/* Color accent bar on the left */}
            <div className={`absolute left-0 top-0 bottom-0 w-1.5 ${colorClasses[color].border} opacity-80 group-hover:w-2 transition-all`} />

            {/* Background Icon Watermark - Adjusted for overflow and color */}
            <div className={`absolute top-2 left-2 ${colorClasses[color].icon} opacity-[0.08] dark:opacity-[0.12] group-hover:opacity-[0.15] transition-opacity scale-[2.5] pointer-events-none origin-top-left`}>
                {icon}
            </div>

            <div className="flex flex-col items-center text-center relative z-10 w-full mb-6">
                <h4 className="text-[11px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-2">{title}</h4>
                <div className="flex items-baseline gap-1">
                    <span className="text-4xl font-black text-slate-900 dark:text-white tracking-tighter leading-none">{formattedValue}</span>
                    <span className="text-xs font-black text-slate-400 dark:text-slate-600">{unit}</span>
                </div>
            </div>

            <div className="mt-auto w-full border-t border-slate-100 dark:border-slate-800 pt-4 grid grid-cols-3 gap-0 relative z-10">
                <div className="flex flex-col items-center border-r border-slate-100 dark:border-slate-800 px-1">
                    <span className="text-[9px] font-black text-slate-400 uppercase mb-1">Target</span>
                    <span className="text-sm font-black text-slate-700 dark:text-slate-300">{formattedTarget}</span>
                </div>
                <div className="flex flex-col items-center border-r border-slate-100 dark:border-slate-800 px-1">
                    <span className="text-[9px] font-black text-slate-400 uppercase mb-1">Diff</span>
                    <span className={`text-sm font-black ${!showStatusColor ? 'text-slate-700 dark:text-slate-300' : (isPositive ? 'text-emerald-500' : 'text-red-500')}`}>
                        {formattedDiff}
                    </span>
                </div>
                <div className="flex flex-col items-center px-1">
                    <span className="text-[9px] font-black text-slate-400 uppercase mb-1">Rate</span>
                    <span className="text-sm font-black text-slate-700 dark:text-slate-300">
                        {formattedRatio}
                    </span>
                </div>
            </div>
        </div>
    );
}
