'use client';

import React, { useEffect, useState } from 'react';
import GreenCross, { DailyStatus } from '@/components/ehs/GreenCross';
import { ShieldCheck, AlertOctagon, Activity, HardHat, AlertTriangle, Calendar, Filter, ChevronLeft, ChevronRight, CheckCircle2, X } from 'lucide-react';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts';
import { useUI } from '@/context/UIContext';
import FilterDropdown from '@/components/common/FilterDropdown';

interface EHSData {
    greenCross: { date: string, status: DailyStatus, details?: string }[];
    stats: {
        incidents: number;
        hazards: number;
        safeDays: number;
    };
    areaHazards: { area: string, count: number }[];
    incidents: { title: string, date: string, area: string, classification: string, description: string, status: string, progress: number }[];
    hazardHeatmap: { area: string, month: string, count: number }[];
    filterOptions?: {
        areas: string[];
    };
}

interface CalendarData {
    years: string[];
    months: Record<string, string[]>;
    currentFiscalInfo?: { fiscal_year: string, fiscal_month: string };
}

export default function EHSDashboard() {
    const [data, setData] = useState<EHSData | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const { isFilterOpen, toggleFilter } = useUI();

    // --- Filter Auto-Control Logic ---
    const isFilterOpenRef = React.useRef(isFilterOpen);
    useEffect(() => { isFilterOpenRef.current = isFilterOpen; }, [isFilterOpen]);

    useEffect(() => {
        if (!isFilterOpenRef.current) {
            toggleFilter();
        }

        const timer = setTimeout(() => {
            if (isFilterOpenRef.current) {
                toggleFilter();
            }
        }, 15000);

        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.key.toLowerCase() === 'f' && !e.ctrlKey && !e.altKey && !e.metaKey && document.activeElement?.tagName !== 'INPUT' && document.activeElement?.tagName !== 'TEXTAREA') {
                toggleFilter();
            }
        };
        window.addEventListener('keydown', handleKeyDown);

        return () => {
            clearTimeout(timer);
            window.removeEventListener('keydown', handleKeyDown);
        };
    }, []);

    // --- Master Data (Hardcoded fallback) ---
    const allAreas = ['147加工中心', '128柱+创伤', '104oard', '96-后处理', '65入物纵切', '56+线切割', '44器械单元', '36H后处理', '35器械纵切'];
    const allLabels = ['急救事件', '险兆事故', '不安全行为', '设备伤害', '滑倒/摔伤', '电气伤害', '化学品伤害', '人体工程学', 'PPE'];
    const allProgress = ['Not Started', 'In Progress', 'Completed'];
    const allPriorities = ['Urgent', 'Important', 'Medium', 'Low'];

    // --- Filter State ---
    const [calendar, setCalendar] = useState<CalendarData | null>(null);
    const [selectedYear, setSelectedYear] = useState<string>(''); // Fiscal Year e.g. "FY26"
    const [selectedMonth, setSelectedMonth] = useState<string>(''); // Fiscal Month e.g. "May"
    const [selectedIncident, setSelectedIncident] = useState<any | null>(null);

    // Multi-select filters
    const [selectedAreas, setSelectedAreas] = useState<string[]>([]);
    const [selectedLabels, setSelectedLabels] = useState<string[]>([]);
    const [selectedProgress, setSelectedProgress] = useState<string[]>([]);
    const [selectedPriority, setSelectedPriority] = useState<string[]>([]);

    // --- Hiding Logic ---
    const [hiddenIncidents, setHiddenIncidents] = useState<string[]>([]);
    const [isHiddenListOpen, setIsHiddenListOpen] = useState(false);

    useEffect(() => {
        const saved = localStorage.getItem('ehs_hidden_incidents');
        if (saved) setHiddenIncidents(JSON.parse(saved));
    }, []);

    const toggleHideIncident = (title: string) => {
        const newHidden = hiddenIncidents.includes(title)
            ? hiddenIncidents.filter(t => t !== title)
            : [...hiddenIncidents, title];
        setHiddenIncidents(newHidden);
        localStorage.setItem('ehs_hidden_incidents', JSON.stringify(newHidden));
    };

    // 1. Fetch Calendar & Set Defaults
    useEffect(() => {
        async function fetchCal() {
            try {
                const res = await fetch('/api/production/calendar');
                const json = await res.json();
                setCalendar(json);
                if (json.currentFiscalInfo) {
                    setSelectedYear(json.currentFiscalInfo.fiscal_year);
                    setSelectedMonth(json.currentFiscalInfo.fiscal_month);
                } else if (json.years?.length > 0) {
                    setSelectedYear(json.years[0]);
                    if (json.months?.[json.years[0]]?.length > 0) {
                        setSelectedMonth(json.months[json.years[0]][0]);
                    }
                }
            } catch (e) {
                console.error("Calendar fetch failed", e);
            }
        }
        fetchCal();
    }, []);

    // 2. Fetch Dashboard Data
    const fetchData = async () => {
        if (!selectedYear || !selectedMonth) return;
        setLoading(true);
        try {
            const params = new URLSearchParams();
            params.set('fiscalYear', selectedYear);
            params.set('fiscalMonth', selectedMonth);

            if (selectedAreas.length > 0) params.set('area', selectedAreas[0]);
            if (selectedLabels.length > 0) params.set('labels', selectedLabels.join(','));
            if (selectedProgress.length > 0) params.set('progress', selectedProgress.join(','));
            if (selectedPriority.length > 0) params.set('priority', selectedPriority.join(','));

            const res = await fetch(`/api/production/ehs?${params.toString()}`);
            const json = await res.json();
            if (res.status === 500) {
                setError(json.error || 'Internal Server Error');
            } else {
                setData(json);
                setError(null);

                // === DEBUG LOGGING ===
                console.log('[EHS PAGE] === DEBUGGING FRONTEND DATA ===');
                console.log('[EHS PAGE] API Response:', json);
                console.log('[EHS PAGE] Incidents List Length:', json?.incidents?.length || 0);
                console.log('[EHS PAGE] Incidents Sample (first 3):', json?.incidents?.slice(0, 3));
                console.log('[EHS PAGE] ===========================');
            }
        } catch (e: any) {
            console.error(e);
            setError(e.message || 'Failed to fetch dashboard data');
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchData();
    }, [selectedYear, selectedMonth, selectedAreas, selectedLabels, selectedProgress, selectedPriority]);

    const handleGreenCrossUpdate = async (date: string, status: DailyStatus, details?: string) => {
        try {
            await fetch('/api/production/ehs', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ date, status, details })
            });
            fetchData(); // Refresh to ensure sync
        } catch (e) {
            console.error(e);
        }
    };

    // Transform Green Cross array to Map
    const greenCrossMap: Record<string, any> = {};
    if (data?.greenCross) {
        data.greenCross.forEach(d => {
            greenCrossMap[d.date] = d;
        });
    }

    // Helper to calculate Calendar Year/Month from Fiscal
    const getCalendarInfo = () => {
        if (!selectedMonth || !selectedYear) return { year: new Date().getFullYear(), month: new Date().getMonth() + 1 };

        // Parse fiscal_month format: "FY30 M10 FEB"
        // Extract the month abbreviation (last 3 chars)
        const monthAbbr = selectedMonth.trim().slice(-3);

        // Map month abbreviations to numbers
        const monthMap: Record<string, number> = {
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
            'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
        };

        const monthNum = monthMap[monthAbbr.toUpperCase()] || new Date().getMonth() + 1;

        // Extract fiscal year number from selectedYear (e.g., "FY30" -> 30)
        const fyNum = parseInt(selectedYear.replace(/\D/g, ''));

        // Calculate calendar year: 
        // Fiscal year starts in May (month 5), so:
        // - If current month is May-Dec (5-12): calendar year = FY year - 1 + 2000
        // - If current month is Jan-Apr (1-4): calendar year = FY year + 2000
        const yearNum = monthNum >= 5 ? (fyNum - 1) + 2000 : fyNum + 2000;

        return { year: yearNum, month: monthNum };
    }

    // Navigation Handlers
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

    const { year: displayYear, month: displayMonth } = getCalendarInfo();

    // KPI Component
    const KpiCard = ({ title, value, icon, color, subtext }: any) => (
        <div className={`p-4 rounded-2xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 shadow-sm relative overflow-hidden group hover:translate-y-[-2px] transition-all`}>
            <div className={`absolute top-0 right-0 p-3 opacity-10 ${color}`}>{icon}</div>
            <h4 className="text-[9px] font-black uppercase text-slate-400 tracking-widest mb-1">{title}</h4>
            <div className="text-3xl font-black text-slate-800 dark:text-white mb-0.5 flex items-baseline gap-2">
                {value}
                <span className="text-[10px] font-bold text-slate-400">{subtext}</span>
            </div>
            <div className={`h-1 w-full mt-2 rounded-full bg-slate-100 dark:bg-slate-800 overflow-hidden`}>
                <div className={`h-full ${color.replace('text-', 'bg-')} w-full`} />
            </div>
        </div>
    );

    // Heatmap Component
    const HazardHeatmap = ({ data }: { data: any[] }) => {
        // 1. Process data for top 10 areas
        const areaTotals: Record<string, number> = {};
        data.forEach(item => {
            areaTotals[item.area] = (areaTotals[item.area] || 0) + item.count;
        });

        // Show exactly Top 10 or fewer if not available
        const sortedAreas = Object.keys(areaTotals)
            .sort((a, b) => areaTotals[b] - areaTotals[a])
            .slice(0, 10);

        // 2. Define fixed 12 Fiscal Months (Standard Medtronic FY: May to Apr)
        const sortedMonths = ['MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC', 'JAN', 'FEB', 'MAR', 'APR'];

        const getCount = (area: string, month: string) => {
            // Match month by checking if it ends with the 3-letter abbreviation
            return data.find(item => item.area === area && item.month.trim().toUpperCase().endsWith(month))?.count || 0;
        };

        const getColor = (count: number) => {
            if (count === 0) return 'bg-slate-50 dark:bg-slate-800/20 text-slate-200 dark:text-slate-700';
            if (count < 2) return 'bg-emerald-100 dark:bg-emerald-900/30 text-emerald-600 dark:text-emerald-400';
            if (count < 5) return 'bg-emerald-300 dark:bg-emerald-400 text-white font-bold';
            if (count < 10) return 'bg-emerald-500 dark:bg-emerald-600 text-white font-black';
            return 'bg-emerald-700 dark:bg-emerald-800 text-white font-black ring-1 ring-emerald-300 dark:ring-emerald-900';
        };

        if (sortedAreas.length === 0) return (
            <div className="flex flex-col items-center justify-center h-40 text-slate-400 bg-slate-50/50 dark:bg-slate-800/10 rounded-xl border border-dashed border-slate-200 dark:border-slate-800">
                <AlertTriangle size={24} className="mb-2 opacity-20" />
                <div className="text-xs font-bold uppercase tracking-wider italic">No hazard data available</div>
            </div>
        );

        return (
            <div className="w-full h-full flex flex-col pt-1">
                {/* Header */}
                <div className="flex items-center pb-2 border-b border-slate-100 dark:border-slate-800 shrink-0">
                    <div className="w-28 pl-1 text-[11px] font-black text-slate-400 uppercase tracking-widest shrink-0">Area</div>
                    <div className="flex-1 grid grid-cols-12 gap-0.5">
                        {sortedMonths.map(m => (
                            <div key={m} className="text-center text-[10px] font-black text-slate-400 uppercase tracking-widest">
                                {m}
                            </div>
                        ))}
                    </div>
                </div>

                {/* Body - Flex container to distribute rows evenly */}
                <div className="flex-1 flex flex-col min-h-0">
                    {sortedAreas.map((area, i) => (
                        <div key={area} className="flex-1 flex items-center border-b border-dashed border-slate-100 dark:border-slate-800/50 last:border-0 min-h-0 group hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors">
                            {/* Area Name */}
                            <div className="w-28 pr-2 shrink-0">
                                <div className="text-[11px] font-bold text-slate-600 dark:text-slate-400 truncate" title={area}>
                                    {area}
                                </div>
                            </div>

                            {/* Months Grid */}
                            <div className="flex-1 h-full grid grid-cols-12 gap-0.5 py-[2px]">
                                {sortedMonths.map(month => {
                                    const count = getCount(area, month);
                                    return (
                                        <div key={month} className="relative group/cell h-full w-full">
                                            <div className={`w-full h-full flex items-center justify-center text-[11px] font-black rounded-sm transition-all duration-300 hover:scale-105 hover:z-10 shadow-sm cursor-default ${getColor(count)}`}>
                                                {count > 0 ? count : ''}
                                            </div>
                                            {/* Tooltip */}
                                            <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1 px-2 py-1 bg-slate-900 text-white text-[8px] rounded opacity-0 pointer-events-none group-hover/cell:opacity-100 transition-opacity z-50 whitespace-nowrap shadow-xl">
                                                {area} - {month}: {count} hazards
                                            </div>
                                        </div>
                                    );
                                })}
                            </div>
                        </div>
                    ))}
                </div>
            </div>
        );
    };

    return (
        <div className="flex flex-1 overflow-hidden min-h-[calc(100vh-140px)] rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm bg-white dark:bg-slate-900">
            {/* Sidebar Filters */}
            <aside className={`${isFilterOpen ? 'w-72 opacity-100' : 'w-0 opacity-0 overflow-hidden'} border-r border-slate-200 dark:border-slate-800 bg-slate-50/30 dark:bg-slate-800/20 flex flex-col transition-all duration-300 shrink-0 z-20`}>
                <div className="p-6 space-y-6 w-72 flex flex-col h-full overflow-y-auto">
                    <section>
                        <h3 className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-4 flex items-center gap-2">
                            <Calendar size={12} /> Time Selection
                        </h3>

                        <div className="space-y-4">
                            {/* Fiscal Year Selector */}
                            <div className="flex items-center bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-700 rounded-xl group transition-all hover:border-medtronic/30">
                                <button onClick={() => handleYearNavigate(1)} className="p-3 text-slate-700 dark:text-slate-300 hover:text-medtronic transition-colors border-r border-slate-200/50 dark:border-slate-700"><ChevronLeft size={14} /></button>
                                <div className="flex-1 relative">
                                    <select value={selectedYear} onChange={(e) => setSelectedYear(e.target.value)} className="w-full bg-transparent text-slate-800 dark:text-slate-200 px-4 py-2 text-xs font-bold outline-none appearance-none cursor-pointer text-center">
                                        {(calendar?.years || []).map(y => <option key={y} value={y}>{y}</option>)}
                                    </select>
                                    <div className="absolute right-2 top-1/2 -translate-y-1/2 pointer-events-none text-slate-300 dark:text-slate-600"><ChevronRight size={10} className="rotate-90" /></div>
                                </div>
                                <button onClick={() => handleYearNavigate(-1)} className="p-3 text-slate-700 dark:text-slate-300 hover:text-medtronic transition-colors border-l border-slate-200/50 dark:border-slate-700"><ChevronRight size={14} /></button>
                            </div>

                            {/* Fiscal Month Selector */}
                            <div className="flex items-center bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-700 rounded-xl group transition-all hover:border-medtronic/30">
                                <button onClick={() => handleMonthNavigate(-1)} className="p-3 text-slate-700 dark:text-slate-300 hover:text-medtronic transition-colors border-r border-slate-200/50 dark:border-slate-700"><ChevronLeft size={14} /></button>
                                <div className="flex-1 relative">
                                    <select value={selectedMonth} onChange={(e) => setSelectedMonth(e.target.value)} className="w-full bg-transparent text-slate-800 dark:text-slate-200 px-4 py-2 text-xs font-bold outline-none appearance-none cursor-pointer text-center">
                                        {(calendar?.months[selectedYear] || []).map(m => <option key={m} value={m}>{m}</option>)}
                                    </select>
                                    <div className="absolute right-2 top-1/2 -translate-y-1/2 pointer-events-none text-slate-300 dark:text-slate-600"><ChevronRight size={10} className="rotate-90" /></div>
                                </div>
                                <button onClick={() => handleMonthNavigate(1)} className="p-3 text-slate-700 dark:text-slate-300 hover:text-medtronic transition-colors border-l border-slate-200/50 dark:border-slate-700"><ChevronRight size={14} /></button>
                            </div>
                        </div>
                    </section>

                    <div className="h-px bg-slate-100 dark:bg-slate-800" />

                    <section className="space-y-4">
                        <h3 className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest flex items-center gap-2">
                            Advanced Filters
                        </h3>

                        <FilterDropdown
                            title="Area (Team)"
                            options={data?.filterOptions?.areas || allAreas}
                            selected={selectedAreas}
                            onChange={setSelectedAreas}
                            placeholder="All Areas"
                        />

                        <FilterDropdown
                            title="Tags / Labels"
                            options={allLabels}
                            selected={selectedLabels}
                            onChange={setSelectedLabels}
                            placeholder="All Labels"
                        />

                        <FilterDropdown
                            title="Progress"
                            options={allProgress}
                            selected={selectedProgress}
                            onChange={setSelectedProgress}
                            placeholder="All Progress"
                        />

                        <FilterDropdown
                            title="Priority (Importance)"
                            options={allPriorities}
                            selected={selectedPriority}
                            onChange={setSelectedPriority}
                            placeholder="All Priorities"
                        />
                    </section>
                </div>
            </aside>

            {/* Main Content */}
            <div className="flex-1 overflow-y-auto min-w-0 bg-white dark:bg-slate-900 rounded-r-2xl border-l border-slate-100 dark:border-slate-800">
                <div className="p-8 space-y-8 min-h-full">
                    {error && (
                        <div className="max-w-[1600px] mx-auto w-full mb-4 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-xl flex items-center gap-3 text-red-600 dark:text-red-400 animate-in fade-in slide-in-from-top-2 shrink-0">
                            <AlertTriangle size={16} />
                            <div className="text-xs font-bold">
                                API Error: {error}. Data may be incomplete or missing.
                            </div>
                        </div>
                    )}

                    <div className="flex-1 flex flex-col gap-4 max-w-[1700px] mx-auto w-full min-h-0">
                        {/* Row 1: 4 KPIs */}
                        <div className="grid grid-cols-4 gap-4 shrink-0">
                            <KpiCard
                                title="Total Recordable Rate (TRIR)"
                                value="0.12"
                                icon={<Activity size={32} />}
                                color="text-emerald-500"
                                subtext="YTD Target"
                            />
                            <KpiCard
                                title="First Aid Safe Days"
                                value={data?.stats?.safeDays ?? '-'}
                                icon={<ShieldCheck size={32} />}
                                color="text-emerald-500"
                                subtext="Consecutive"
                            />
                            <KpiCard
                                title="YTD First Aid (FA)"
                                value={data?.incidents
                                    ? data.incidents.filter(inc => !hiddenIncidents.includes(inc.title)).length
                                    : '-'}
                                icon={<AlertTriangle size={32} />}
                                color="text-red-500"
                                subtext="Recorded"
                            />
                            <KpiCard
                                title="Open Safety Hazards"
                                value={data?.stats?.hazards ?? '-'}
                                icon={<AlertOctagon size={32} />}
                                color="text-amber-500"
                                subtext="Active Tasks"
                            />
                        </div>

                        {/* Row 2: Main Content Split */}
                        <div className="flex-1 grid grid-cols-12 gap-4 min-h-0">
                            {/* Left: Green Cross (4 cols) */}
                            <div className="col-span-4 h-full flex flex-col min-h-0">
                                <div className="flex-1 overflow-hidden">
                                    <GreenCross
                                        year={displayYear}
                                        month={displayMonth}
                                        data={greenCrossMap}
                                        onUpdate={handleGreenCrossUpdate}
                                    />
                                </div>
                            </div>

                            {/* Right: Detailed Analysis (8 cols) - Stacked Vertically */}
                            <div className="col-span-8 flex flex-col gap-5 h-full min-h-0">
                                {/* Incident List (Equal Height) */}
                                <div className="flex-1 bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-4 flex flex-col shadow-sm min-h-0 overflow-hidden relative">
                                    <div className="flex justify-between items-center mb-2 shrink-0">
                                        <h3 className="text-[9px] font-black text-slate-400 uppercase tracking-widest">Latest First Aid Incidents</h3>
                                        <button
                                            onClick={() => setIsHiddenListOpen(true)}
                                            className="p-1 px-2 rounded-lg bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-700 text-slate-400 hover:text-medtronic transition-all flex items-center gap-1.5 group"
                                            title="Manage Hidden Incidents"
                                        >
                                            <Filter size={10} />
                                            <span className="text-[8px] font-bold">RECOVERY {hiddenIncidents.length > 0 && `(${hiddenIncidents.length})`}</span>
                                        </button>
                                    </div>
                                    <div className="flex-1 overflow-y-auto pr-1 custom-scrollbar">
                                        <div className="flex flex-col gap-1.5">
                                            {data?.incidents && data.incidents.filter(inc => !hiddenIncidents.includes(inc.title)).length > 0 ? (
                                                data.incidents
                                                    .filter(inc => !hiddenIncidents.includes(inc.title))
                                                    .map((incident, i) => (
                                                        <button
                                                            key={i}
                                                            onClick={() => setSelectedIncident(incident)}
                                                            className="w-full text-left flex flex-col p-3 rounded-xl bg-slate-50 dark:bg-slate-800/50 border border-slate-100 dark:border-slate-800 hover:border-red-200/50 hover:bg-white dark:hover:bg-slate-800 transition-all group shrink-0"
                                                        >
                                                            <div className="flex justify-between items-start mb-1 gap-2">
                                                                <div className="text-[11px] font-black text-slate-800 dark:text-slate-200 group-hover:text-red-600 transition-colors line-clamp-2 leading-tight">{incident.title}</div>
                                                                <span className={`text-[8px] font-black uppercase tracking-tighter shrink-0 px-1.5 py-0.5 rounded-md ${incident.status === 'Completed'
                                                                    ? 'bg-emerald-100 text-emerald-600 dark:bg-emerald-900/30 dark:text-emerald-400'
                                                                    : 'bg-amber-100 text-amber-600 dark:bg-amber-900/30 dark:text-amber-400'
                                                                    }`}>
                                                                    {incident.status === 'Completed' ? 'Closed' : 'Open'}
                                                                </span>
                                                            </div>

                                                            <div className="flex justify-between items-center mt-auto pt-1 border-t border-slate-100 dark:border-slate-800/50 w-full">
                                                                <div className="text-[9px] font-bold text-slate-500 uppercase tracking-widest flex items-center gap-1">
                                                                    {incident.area}
                                                                </div>
                                                                <span className="text-[8px] font-bold text-slate-400">{new Date(incident.date).toLocaleDateString()}</span>
                                                            </div>
                                                        </button>
                                                    ))
                                            ) : (
                                                <div className="text-[10px] text-slate-400 italic text-center mt-6">
                                                    {hiddenIncidents.length > 0 && data?.incidents?.length === hiddenIncidents.length
                                                        ? "All incidents hidden. Use 'RECOVERY' to restore."
                                                        : "No recorded First Aid incidents"}
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                </div>

                                {/* Hazards Heatmap (Equal Height) */}
                                <div className="flex-1 bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-4 flex flex-col shadow-sm min-h-0 overflow-hidden">
                                    <h3 className="text-[9px] font-black text-slate-400 uppercase tracking-widest mb-1.5 shrink-0">Hazards Heatmap (Top 10 Areas)</h3>
                                    <div className="flex-1 overflow-hidden">
                                        <HazardHeatmap data={data?.hazardHeatmap || []} />
                                    </div>
                                </div>
                            </div>
                        </div>
                        {/* Extra bottom space for better visual comfort and to show container corners */}
                        <div className="h-4 shrink-0" />
                    </div>
                </div>

                {/* Hidden Incidents Management Modal */}
                {isHiddenListOpen && (
                    <div className="fixed inset-0 z-[110] flex items-center justify-center p-4 bg-slate-950/60 backdrop-blur-sm animate-in fade-in duration-300">
                        <div className="bg-white dark:bg-slate-900 w-full max-w-md rounded-3xl shadow-2xl overflow-hidden border border-slate-200 dark:border-slate-800 animate-in zoom-in-95 duration-300">
                            <div className="p-6">
                                <div className="flex justify-between items-center mb-6">
                                    <h3 className="text-sm font-black text-slate-800 dark:text-white uppercase tracking-widest">Hidden Incidents</h3>
                                    <button onClick={() => setIsHiddenListOpen(false)} className="p-2 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-full transition-colors text-slate-400">
                                        <X size={18} />
                                    </button>
                                </div>

                                <div className="space-y-2 max-h-[400px] overflow-y-auto pr-1 custom-scrollbar">
                                    {hiddenIncidents.length > 0 ? (
                                        hiddenIncidents.map((title, i) => (
                                            <div key={i} className="flex justify-between items-center p-3 bg-slate-50 dark:bg-slate-800/50 rounded-xl border border-slate-100 dark:border-slate-800">
                                                <span className="text-[11px] font-bold text-slate-700 dark:text-slate-300 truncate mr-4">{title}</span>
                                                <button
                                                    onClick={() => toggleHideIncident(title)}
                                                    className="px-3 py-1 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-[9px] font-black text-emerald-500 uppercase tracking-widest hover:bg-emerald-50 transition-all shadow-sm"
                                                >
                                                    Release
                                                </button>
                                            </div>
                                        ))
                                    ) : (
                                        <div className="text-center py-8 text-xs text-slate-400 italic font-medium">No hidden incidents found.</div>
                                    )}
                                </div>

                                <button
                                    onClick={() => setIsHiddenListOpen(false)}
                                    className="w-full mt-6 py-3 bg-slate-950 dark:bg-white dark:text-slate-950 text-white rounded-xl text-xs font-black uppercase tracking-widest hover:scale-[1.02] active:scale-[0.98] transition-all"
                                >
                                    Done
                                </button>
                            </div>
                        </div>
                    </div>
                )}

                {/* Incident Details Modal */}
                {selectedIncident && (
                    <div className="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-slate-950/60 backdrop-blur-sm animate-in fade-in duration-300">
                        <div className="bg-white dark:bg-slate-900 w-full max-w-lg rounded-3xl shadow-2xl overflow-hidden border border-slate-200 dark:border-slate-800 animate-in zoom-in-95 duration-300">
                            <div className="p-8">
                                <div className="flex justify-between items-start mb-6">
                                    <div>
                                        <span className="px-2 py-0.5 bg-red-100 dark:bg-red-900/30 text-red-600 dark:text-red-400 text-[9px] font-black uppercase tracking-widest rounded-full mb-2 inline-block">
                                            {selectedIncident.classification}
                                        </span>
                                        <h2 className="text-xl font-black text-slate-900 dark:text-white leading-tight">
                                            {selectedIncident.title}
                                        </h2>
                                    </div>
                                    <button
                                        onClick={() => setSelectedIncident(null)}
                                        className="p-2 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-full transition-colors text-slate-400"
                                    >
                                        <X size={20} />
                                    </button>
                                </div>

                                <div className="grid grid-cols-2 gap-6 mb-8">
                                    <div className="space-y-1">
                                        <div className="text-[9px] font-black text-slate-400 uppercase tracking-widest">Date Reported</div>
                                        <div className="text-sm font-bold text-slate-700 dark:text-slate-300 flex items-center gap-2">
                                            <Calendar size={14} className="text-slate-400" />
                                            {new Date(selectedIncident.date).toLocaleDateString(undefined, { dateStyle: 'long' })}
                                        </div>
                                    </div>
                                    <div className="space-y-1">
                                        <div className="text-[9px] font-black text-slate-400 uppercase tracking-widest">Location / Area</div>
                                        <div className="text-sm font-bold text-slate-700 dark:text-slate-300 flex items-center gap-2">
                                            <Filter size={14} className="text-slate-400" />
                                            {selectedIncident.area}
                                        </div>
                                    </div>
                                </div>

                                <div className="space-y-6">
                                    <div className="p-5 bg-slate-50 dark:bg-slate-800/50 rounded-2xl border border-slate-100 dark:border-slate-800">
                                        <h4 className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-3">Investigation Context & Description</h4>
                                        <p className="text-[14px] font-bold text-slate-800 dark:text-slate-200 leading-relaxed">
                                            {selectedIncident.description || "No detailed description provided in planner task."}
                                        </p>
                                    </div>

                                    <div className="flex items-center gap-6 py-4 border-t border-dashed border-slate-200 dark:border-slate-800">
                                        <div className="flex-1">
                                            <div className="text-[9px] font-black text-slate-400 uppercase tracking-widest mb-2">Planner Progress</div>
                                            <div className="flex items-center gap-3">
                                                <div className="flex-1 h-2 bg-slate-100 dark:bg-slate-800 rounded-full overflow-hidden">
                                                    <div
                                                        className="h-full bg-emerald-500 transition-all duration-500"
                                                        style={{ width: `${selectedIncident.progress}%` }}
                                                    />
                                                </div>
                                                <span className="text-xs font-black text-emerald-500 w-10 text-right">{selectedIncident.progress}%</span>
                                            </div>
                                            <div className="flex items-center gap-1.5 mt-2 text-[10px] font-bold text-slate-500">
                                                <CheckCircle2 size={12} className={selectedIncident.progress === 100 ? "text-emerald-500" : "text-slate-300"} />
                                                {selectedIncident.progress === 100 ? "Investigation Closed" : "Investigation in Progress"}
                                            </div>
                                        </div>
                                        <div className="flex items-center gap-3">
                                            <button
                                                onClick={() => {
                                                    toggleHideIncident(selectedIncident.title);
                                                    setSelectedIncident(null);
                                                }}
                                                className="px-4 py-3 bg-slate-100 dark:bg-slate-800 text-slate-500 hover:text-red-500 rounded-xl text-[10px] font-black uppercase tracking-widest transition-all flex items-center gap-2"
                                                title="Hide this incident from the main dashboard list"
                                            >
                                                Hide Card
                                            </button>
                                            <button
                                                onClick={() => setSelectedIncident(null)}
                                                className="px-6 py-3 bg-slate-950 dark:bg-white dark:text-slate-950 text-white rounded-xl text-xs font-black uppercase tracking-widest hover:scale-[1.02] active:scale-[0.98] transition-all"
                                            >
                                                Dismiss
                                            </button>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}
