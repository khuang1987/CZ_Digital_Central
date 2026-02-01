'use client';

import React, { useEffect, useState } from 'react';
import GreenCross, { DailyStatus } from '@/components/ehs/GreenCross';
import { ShieldCheck, AlertOctagon, Activity, HardHat, AlertTriangle, Calendar, Filter, ChevronLeft, ChevronRight, CheckCircle2 } from 'lucide-react';
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
    incidentsList: { title: string, date: string, area: string, classification: string }[];
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

    // Multi-select filters
    const [selectedAreas, setSelectedAreas] = useState<string[]>([]);
    const [selectedLabels, setSelectedLabels] = useState<string[]>([]);
    const [selectedProgress, setSelectedProgress] = useState<string[]>([]);
    const [selectedPriority, setSelectedPriority] = useState<string[]>([]);

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
        const months = new Set<string>();
        data.forEach(item => {
            areaTotals[item.area] = (areaTotals[item.area] || 0) + item.count;
            months.add(item.month);
        });

        const sortedAreas = Object.keys(areaTotals)
            .sort((a, b) => areaTotals[b] - areaTotals[a]);
        // Removed .slice(0, 10) to show all areas

        const sortedMonths = Array.from(months).sort((a, b) => {
            // Sort by FY order: May...Apr
            const mOrder: Record<string, number> = { 'MAY': 1, 'JUN': 2, 'JUL': 3, 'AUG': 4, 'SEP': 5, 'OCT': 6, 'NOV': 7, 'DEC': 8, 'JAN': 9, 'FEB': 10, 'MAR': 11, 'APR': 12 };
            const getOrder = (m: string) => mOrder[m.trim().slice(-3).toUpperCase()] || 0;
            return getOrder(a) - getOrder(b);
        });

        const getCount = (area: string, month: string) => {
            return data.find(item => item.area === area && item.month === month)?.count || 0;
        };

        const getColor = (count: number) => {
            if (count === 0) return 'bg-slate-50 dark:bg-slate-800/30 text-slate-200 dark:text-slate-700';
            if (count < 2) return 'bg-emerald-100 dark:bg-emerald-900/20 text-emerald-600';
            if (count < 5) return 'bg-emerald-300 dark:bg-emerald-700/40 text-white font-bold';
            return 'bg-emerald-500 dark:bg-emerald-600 text-white font-black';
        };

        if (sortedAreas.length === 0) return <div className="text-xs text-slate-400 italic text-center mt-10">No hazard data distribution</div>;

        return (
            <div className="w-full overflow-x-auto overflow-y-hidden">
                <table className="w-full text-left border-collapse table-fixed min-w-[500px]">
                    <thead>
                        <tr>
                            <th className="w-32 py-1 text-[9px] font-black text-slate-400 uppercase tracking-tighter">Area</th>
                            {sortedMonths.map(m => (
                                <th key={m} className="px-1 py-1 text-center text-[8px] font-black text-slate-400 uppercase tracking-tighter w-10">
                                    {m.trim().slice(-3)}
                                </th>
                            ))}
                        </tr>
                    </thead>
                    <tbody>
                        {sortedAreas.map(area => (
                            <tr key={area} className="group border-b border-slate-50 dark:border-slate-800/10 last:border-0 hover:bg-slate-50/50 dark:hover:bg-slate-800/20">
                                <td className="py-0.5 pr-1 text-[7.5px] leading-[1.1] font-bold text-slate-500 dark:text-slate-400 truncate max-w-[110px]" title={area}>{area}</td>
                                {sortedMonths.map(month => {
                                    const count = getCount(area, month);
                                    return (
                                        <td key={month} className="p-[0.5px]">
                                            <div className={`w-full aspect-square max-w-[14px] flex items-center justify-center text-[7px] leading-none rounded-[1px] transition-all group-hover:scale-110 shadow-sm ${getColor(count)}`}>
                                                {count > 0 ? count : ''}
                                            </div>
                                        </td>
                                    );
                                })}
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        );
    };

    return (
        <div className="flex w-full h-full overflow-hidden bg-slate-50 dark:bg-transparent">
            {/* Sidebar Filters */}
            <aside className={`${isFilterOpen ? 'w-72 opacity-100' : 'w-0 opacity-0 overflow-hidden'} border-r border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 flex flex-col transition-all duration-300 shrink-0 shadow-sm z-20`}>
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
            <div className="flex-1 flex flex-col h-screen overflow-hidden p-6">
                {error && (
                    <div className="max-w-[1600px] mx-auto w-full mb-4 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-xl flex items-center gap-3 text-red-600 dark:text-red-400 animate-in fade-in slide-in-from-top-2 shrink-0">
                        <AlertTriangle size={16} />
                        <div className="text-xs font-bold">
                            API Error: {error}. Data may be incomplete or missing.
                        </div>
                    </div>
                )}

                <div className="flex-1 flex flex-col gap-4 max-w-[1600px] mx-auto w-full min-h-0 pb-2">
                    {/* Row 1: 4 KPIs */}
                    <div className="grid grid-cols-4 gap-4 shrink-0">
                        <KpiCard
                            title="Total Recordable Incident Rate (TRIR)"
                            value="-"
                            icon={<Activity size={32} />}
                            color="text-slate-400"
                            subtext="YTD Placeholder"
                        />
                        <KpiCard
                            title="Safe Days"
                            value={data?.stats?.safeDays ?? '-'}
                            icon={<ShieldCheck size={32} />}
                            color="text-emerald-500"
                            subtext="Consecutive"
                        />
                        <KpiCard
                            title="YTD Incidents"
                            value={data?.stats?.incidents ?? '-'}
                            icon={<AlertTriangle size={32} />}
                            color="text-red-500"
                            subtext="Recordable"
                        />
                        <KpiCard
                            title="Open Internal Hazards"
                            value={data?.stats?.hazards ?? '-'}
                            icon={<AlertOctagon size={32} />}
                            color="text-amber-500"
                            subtext="Safety Tasks"
                        />
                    </div>

                    {/* Row 2: Main Content Split */}
                    <div className="flex-1 grid grid-cols-12 gap-5 min-h-0">
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
                        <div className="col-span-8 flex flex-col gap-4 h-full min-h-0">
                            {/* Incident List (Equal Height) */}
                            <div className="flex-1 bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-5 flex flex-col shadow-sm min-h-0">
                                <h3 className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-3 shrink-0">Latest Incidents (YTD)</h3>
                                <div className="flex-1 overflow-y-auto pr-1 custom-scrollbar">
                                    <div className="flex flex-col gap-2">
                                        {data?.incidentsList && data.incidentsList.length > 0 ? (
                                            data.incidentsList.map((incident, i) => (
                                                <div key={i} className="flex flex-col p-2.5 rounded-xl bg-slate-50 dark:bg-slate-800/50 border border-slate-100 dark:border-slate-800 hover:border-red-200/50 transition-all group shrink-0">
                                                    <div className="flex justify-between items-start mb-0.5">
                                                        <span className="text-[9px] font-black text-red-500 uppercase tracking-tighter">{incident.classification}</span>
                                                        <span className="text-[9px] font-bold text-slate-400">{new Date(incident.date).toLocaleDateString()}</span>
                                                    </div>
                                                    <div className="text-[11px] font-black text-slate-800 dark:text-slate-200 group-hover:text-red-600 transition-colors line-clamp-1">{incident.title}</div>
                                                    <div className="text-[9px] font-bold text-slate-400 uppercase tracking-widest mt-0.5 ml-auto">{incident.area}</div>
                                                </div>
                                            ))
                                        ) : (
                                            <div className="text-[10px] text-slate-400 italic text-center mt-6">
                                                No recorded incidents in current fiscal period
                                            </div>
                                        )}
                                    </div>
                                </div>
                            </div>

                            {/* Hazards Heatmap (Equal Height) */}
                            <div className="flex-1 bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-5 flex flex-col shadow-sm min-h-0 overflow-hidden">
                                <h3 className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-3 shrink-0">Hazards Heatmap (Top 10 Areas)</h3>
                                <div className="flex-1 overflow-auto custom-scrollbar">
                                    <HazardHeatmap data={data?.hazardHeatmap || []} />
                                </div>
                            </div>
                        </div>
                    </div>

                    {/* Placeholder for Screen 2 / 3 */}
                    <div className="border-t border-dashed border-slate-200 dark:border-slate-800 pt-8 opacity-50">
                        <div className="text-center text-slate-400 text-sm font-bold uppercase tracking-widest">
                            Additional Analysis Screens (Under Construction)
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
