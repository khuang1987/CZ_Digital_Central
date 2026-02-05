'use client';

import React, { useEffect, useState } from 'react';
import GreenCross, { DailyStatus } from '@/components/ehs/GreenCross';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, ComposedChart, Line, CartesianGrid, Legend } from 'recharts';
import { ShieldCheck, AlertOctagon, Activity, HardHat, AlertTriangle, Calendar, Filter, ChevronLeft, ChevronRight, CheckCircle2, X, Construction, Zap, FileSpreadsheet, History, ShieldAlert } from 'lucide-react';
import { useUI } from '@/context/UIContext';
import FilterDropdown from '@/components/common/FilterDropdown';
import StandardPageLayout, { PageTab } from '@/components/StandardPageLayout';

interface HazardTask {
    TaskId: string;
    title: string;
    date: string;
    status: string;
    classification: string;
    area: string;
    priority: string;
}

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
    hazardTasks?: HazardTask[];
    filterOptions?: {
        areas: string[];
    };
}

interface CalendarData {
    years: string[];
    months: Record<string, string[]>;
    currentFiscalInfo?: { fiscal_year: string, fiscal_month: string };
}

export default function EHSDashboard({ theme, toggleTheme }: { theme?: 'light' | 'dark', toggleTheme?: () => void }) {
    const [data, setData] = useState<EHSData | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    // Hardcoded fallback data
    const allAreas = ['147加工中心', '128柱+创伤', '104oard', '96-后处理', '65入物纵切', '56+线切割', '44器械单元', '36H后处理', '35器械纵切'];
    const allLabels = ['急救事件', '险兆事故', '不安全行为', '设备伤害', '滑倒/摔伤', '电气伤害', '化学品伤害', '人体工程学', 'PPE'];
    const allProgress = ['Not Started', 'In Progress', 'Completed'];
    const allPriorities = ['Urgent', 'Important', 'Medium', 'Low'];

    // Filter State
    const [calendar, setCalendar] = useState<CalendarData | null>(null);
    const [selectedYear, setSelectedYear] = useState<string>('');
    const [selectedMonth, setSelectedMonth] = useState<string>('');
    const [selectedIncident, setSelectedIncident] = useState<any | null>(null);

    const [selectedAreas, setSelectedAreas] = useState<string[]>([]);
    const [selectedLabels, setSelectedLabels] = useState<string[]>([]);
    const [selectedProgress, setSelectedProgress] = useState<string[]>([]);
    const [selectedPriority, setSelectedPriority] = useState<string[]>([]);

    const [hiddenIncidents, setHiddenIncidents] = useState<string[]>([]);
    const [isHiddenListOpen, setIsHiddenListOpen] = useState(false);

    const [selectedHazardMonth, setSelectedHazardMonth] = useState<string | null>(null);
    const [selectedHazardLabel, setSelectedHazardLabel] = useState<string | null>(null);

    const hazardMonthlyData = React.useMemo(() => {
        if (!data?.hazardTasks) return [];
        const monthMap: Record<string, number> = {};
        data.hazardTasks.forEach(task => {
            const date = new Date(task.date);
            const monthKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
            monthMap[monthKey] = (monthMap[monthKey] || 0) + 1;
        });
        return Object.entries(monthMap)
            .map(([month, count]) => ({ month, count }))
            .sort((a, b) => a.month.localeCompare(b.month));
    }, [data?.hazardTasks]);

    const hazardParetoData = React.useMemo(() => {
        if (!data?.hazardTasks) return [];
        const labelMap: Record<string, number> = {};
        const total = data.hazardTasks.length;

        data.hazardTasks.forEach(task => {
            const labels = task.classification ? task.classification.split(';') : ['Unlabeled'];
            labels.forEach(l => {
                const clean = l.trim();
                if (clean) labelMap[clean] = (labelMap[clean] || 0) + 1;
            });
        });

        const sorted = Object.entries(labelMap)
            .map(([label, count]) => ({ label, count }))
            .sort((a, b) => b.count - a.count);

        let cumulative = 0;
        return sorted.map(item => {
            cumulative += item.count;
            return {
                ...item,
                cumulativePercentage: Math.round((cumulative / total) * 100)
            };
        });
    }, [data?.hazardTasks]);

    const filteredHazardTasks = React.useMemo(() => {
        if (!data?.hazardTasks) return [];
        return data.hazardTasks.filter(task => {
            let match = true;
            if (selectedHazardMonth) {
                const date = new Date(task.date);
                const monthKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
                if (monthKey !== selectedHazardMonth) match = false;
            }
            if (selectedHazardLabel) {
                const labels = task.classification ? task.classification.split(';') : ['Unlabeled'];
                if (!labels.map(l => l.trim()).includes(selectedHazardLabel)) match = false;
            }
            return match;
        });
    }, [data?.hazardTasks, selectedHazardMonth, selectedHazardLabel]);

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
            fetchData();
        } catch (e) {
            console.error(e);
        }
    };

    const greenCrossMap: Record<string, any> = {};
    if (data?.greenCross) {
        data.greenCross.forEach(d => {
            greenCrossMap[d.date] = d;
        });
    }

    const getCalendarInfo = () => {
        if (!selectedMonth || !selectedYear) return { year: new Date().getFullYear(), month: new Date().getMonth() + 1 };
        const monthAbbr = selectedMonth.trim().slice(-3);
        const monthMap: Record<string, number> = {
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
            'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
        };
        const monthNum = monthMap[monthAbbr.toUpperCase()] || new Date().getMonth() + 1;
        const fyNum = parseInt(selectedYear.replace(/\D/g, ''));
        const yearNum = monthNum >= 5 ? (fyNum - 1) + 2000 : fyNum + 2000;
        return { year: yearNum, month: monthNum };
    }

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

    const handleReset = () => {
        setSelectedIncident(null);
        setSelectedAreas([]);
        setSelectedLabels([]);
        setSelectedProgress([]);
        setSelectedPriority([]);
        setSelectedHazardMonth(null);
        setSelectedHazardLabel(null);

        // Reset to current fiscal/calendar defaults if available
        // Reset to current fiscal/calendar defaults if available
        if (calendar && calendar.currentFiscalInfo) {
            setSelectedYear(calendar.currentFiscalInfo.fiscal_year);
            setSelectedMonth(calendar.currentFiscalInfo.fiscal_month);
        } else if (calendar && calendar.years && calendar.years.length > 0) {
            // Fallback to first available
            setSelectedYear(calendar.years[0]);
            if (calendar.months && calendar.months[calendar.years[0]] && calendar.months[calendar.years[0]].length > 0) {
                setSelectedMonth(calendar.months[calendar.years[0]][0]);
            }
        }
    };

    const { year: displayYear, month: displayMonth } = getCalendarInfo();

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

    const HazardHeatmap = ({ data }: { data: any[] }) => {
        const areaTotals: Record<string, number> = {};
        data.forEach(item => {
            areaTotals[item.area] = (areaTotals[item.area] || 0) + item.count;
        });

        const sortedAreas = Object.keys(areaTotals)
            .sort((a, b) => areaTotals[b] - areaTotals[a])
            .slice(0, 10);

        const sortedMonths = ['MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC', 'JAN', 'FEB', 'MAR', 'APR'];

        const getCount = (area: string, month: string) => {
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
                <div className="flex-1 flex flex-col min-h-0">
                    {sortedAreas.map((area, i) => (
                        <div key={area} className="flex-1 flex items-center border-b border-dashed border-slate-100 dark:border-slate-800/50 last:border-0 min-h-0 group hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors">
                            <div className="w-28 pr-2 shrink-0">
                                <div className="text-[11px] font-bold text-slate-600 dark:text-slate-400 truncate" title={area}>
                                    {area}
                                </div>
                            </div>
                            <div className="flex-1 h-full grid grid-cols-12 gap-0.5 py-[2px]">
                                {sortedMonths.map(month => {
                                    const count = getCount(area, month);
                                    return (
                                        <div key={month} className="relative group/cell h-full w-full">
                                            <div className={`w-full h-full flex items-center justify-center text-[11px] font-black rounded-sm transition-all duration-300 hover:scale-105 hover:z-10 shadow-sm cursor-default ${getColor(count)}`}>
                                                {count > 0 ? count : ''}
                                            </div>
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

    // --- EHS Tabs ---
    const ehsTabs: PageTab[] = [
        { label: 'Safety Dashboard', href: '/ehs', icon: <ShieldCheck size={14} />, active: true },
        { label: 'Incident Log', href: '/ehs/incidents', icon: <FileSpreadsheet size={14} /> },
        { label: 'Audit History', href: '/ehs/audits', icon: <History size={14} /> },
    ];

    const filters = (
        <div className="space-y-6">
            <section>
                <h3 className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-4 flex items-center gap-2">
                    <Calendar size={12} /> Time Selection
                </h3>

                <div className="space-y-4">
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
    );

    return (
        <StandardPageLayout
            theme={theme}
            toggleTheme={toggleTheme}
            title="EHS Dashboard"
            description="Environmental, Health, and Safety monitoring."
            icon={<ShieldAlert size={24} />}
            tabs={ehsTabs}
            filters={filters}
            onReset={handleReset}
        >
            <div className="space-y-8 min-h-full">
                {error && (
                    <div className="max-w-[1600px] mx-auto w-full mb-4 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-xl flex items-center gap-3 text-red-600 dark:text-red-400 animate-in fade-in slide-in-from-top-2 shrink-0">
                        <AlertTriangle size={16} />
                        <div className="text-xs font-bold">
                            API Error: {error}. Data may be incomplete or missing.
                        </div>
                    </div>
                )}

                <div className="flex-1 flex flex-col gap-4 max-w-[1700px] mx-auto w-full min-h-0">
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

                    <div className="flex-1 grid grid-cols-12 gap-4 min-h-[600px] lg:min-h-[calc(100vh-280px)]">
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

                        <div className="col-span-8 flex flex-col gap-5 h-full min-h-0">
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

                            <div className="flex-1 bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-4 flex flex-col shadow-sm min-h-0 overflow-hidden">
                                <h3 className="text-[9px] font-black text-slate-400 uppercase tracking-widest mb-1.5 shrink-0">Hazards Heatmap (Top 10 Areas)</h3>
                                <div className="flex-1 overflow-hidden">
                                    <HazardHeatmap data={data?.hazardHeatmap || []} />
                                </div>
                            </div>
                        </div>
                    </div>
                    <div className="h-4 shrink-0" />
                </div>

                <div className="mt-8 pt-8 border-t border-slate-200 dark:border-slate-800">
                    <h3 className="text-lg font-black text-slate-800 dark:text-white flex items-center gap-2 mb-6">
                        <Construction className="text-orange-500" size={24} />
                        Hazard Task Analysis (隐患排查详情)
                        {data?.hazardTasks && (
                            <span className="text-xs px-2 py-1 rounded bg-slate-100 dark:bg-slate-800 text-slate-500">
                                Total: {data.hazardTasks.length}
                            </span>
                        )}
                    </h3>

                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8 h-[350px]">
                        <div className="p-4 bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm flex flex-col">
                            <h4 className="text-xs font-bold text-slate-500 uppercase tracking-widest mb-4">Monthly Trend (Click to Filter)</h4>
                            <div className="flex-1 w-full min-h-0">
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart data={hazardMonthlyData} onClick={(data: any) => data?.activePayload && setSelectedHazardMonth(data.activePayload[0].payload.month === selectedHazardMonth ? null : data.activePayload[0].payload.month)}>
                                        <XAxis dataKey="month" tick={{ fontSize: 10 }} />
                                        <YAxis tick={{ fontSize: 10 }} />
                                        <Tooltip
                                            contentStyle={{ borderRadius: '12px', border: 'none', boxShadow: '0 10px 15px -3px rgb(0 0 0 / 0.1)' }}
                                            cursor={{ fill: 'transparent' }}
                                        />
                                        <Bar dataKey="count" fill="#f59e0b" radius={[4, 4, 0, 0]} className="cursor-pointer hover:opacity-80" />
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                            {selectedHazardMonth && (
                                <div className="mt-2 text-center text-xs font-bold text-orange-500">
                                    Filtered: {selectedHazardMonth} <button onClick={() => setSelectedHazardMonth(null)} className="underline ml-1">Clear</button>
                                </div>
                            )}
                        </div>

                        <div className="p-4 bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm flex flex-col">
                            <h4 className="text-xs font-bold text-slate-500 uppercase tracking-widest mb-4">Pareto by Issue Type</h4>
                            <div className="flex-1 w-full min-h-0">
                                <ResponsiveContainer width="100%" height="100%">
                                    <ComposedChart data={hazardParetoData} onClick={(data: any) => data?.activePayload && setSelectedHazardLabel(data.activePayload[0].payload.label === selectedHazardLabel ? null : data.activePayload[0].payload.label)}>
                                        <XAxis dataKey="label" tick={{ fontSize: 10 }} interval={0} angle={-30} textAnchor="end" height={60} />
                                        <YAxis yAxisId="left" tick={{ fontSize: 10 }} />
                                        <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 10 }} unit="%" />
                                        <Tooltip
                                            contentStyle={{ borderRadius: '12px', border: 'none', boxShadow: '0 10px 15px -3px rgb(0 0 0 / 0.1)' }}
                                        />
                                        <Legend verticalAlign="top" height={36} />
                                        <Bar yAxisId="left" dataKey="count" fill="#3b82f6" barSize={40} radius={[4, 4, 0, 0]} className="cursor-pointer hover:opacity-80" name="Count" />
                                        <Line yAxisId="right" type="monotone" dataKey="cumulativePercentage" stroke="#ef4444" strokeWidth={2} dot={{ r: 3 }} name="Cumulative %" />
                                    </ComposedChart>
                                </ResponsiveContainer>
                            </div>
                            {selectedHazardLabel && (
                                <div className="mt-2 text-center text-xs font-bold text-blue-500">
                                    Filtered: {selectedHazardLabel} <button onClick={() => setSelectedHazardLabel(null)} className="underline ml-1">Clear</button>
                                </div>
                            )}
                        </div>
                    </div>

                    <div className="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 overflow-hidden shadow-sm mb-8">
                        <div className="p-4 border-b border-slate-100 dark:border-slate-800 bg-slate-50/50 dark:bg-slate-800/20 flex justify-between items-center">
                            <h4 className="text-xs font-black text-slate-500 uppercase tracking-widest">
                                Task Detail List ({filteredHazardTasks.length})
                            </h4>
                            {(selectedHazardMonth || selectedHazardLabel) && (
                                <button
                                    onClick={() => { setSelectedHazardMonth(null); setSelectedHazardLabel(null); }}
                                    className="px-3 py-1 text-[10px] font-bold bg-white border border-slate-200 rounded-lg hover:bg-slate-50"
                                >
                                    Reset Filters
                                </button>
                            )}
                        </div>
                        <div className="max-h-[400px] overflow-y-auto custom-scrollbar">
                            <table className="w-full text-left text-xs">
                                <thead className="sticky top-0 bg-slate-50 dark:bg-slate-800 text-slate-500 font-bold uppercase tracking-wider z-10">
                                    <tr>
                                        <th className="p-3 w-24">Date</th>
                                        <th className="p-3 w-24">Area</th>
                                        <th className="p-3">Title / Description</th>
                                        <th className="p-3 w-32">Labels</th>
                                        <th className="p-3 w-24">Priority</th>
                                        <th className="p-3 w-24">Status</th>
                                    </tr>
                                </thead>
                                <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                                    {filteredHazardTasks.length > 0 ? filteredHazardTasks.map((task) => (
                                        <tr key={task.TaskId} className="hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors group">
                                            <td className="p-3 font-mono text-slate-500">
                                                {new Date(task.date).toLocaleDateString()}
                                            </td>
                                            <td className="p-3 font-bold text-slate-700 dark:text-slate-300">
                                                {task.area}
                                            </td>
                                            <td className="p-3 font-medium text-slate-800 dark:text-slate-200">
                                                {task.title}
                                            </td>
                                            <td className="p-3">
                                                <div className="flex flex-wrap gap-1">
                                                    {task.classification ? task.classification.split(';').map((l, i) => (
                                                        <span key={i} className="px-1.5 py-0.5 bg-slate-100 dark:bg-slate-800 text-slate-500 rounded text-[10px] whitespace-nowrap">
                                                            {l}
                                                        </span>
                                                    )) : <span className="text-slate-300">-</span>}
                                                </div>
                                            </td>
                                            <td className="p-3">
                                                <span className={`text-[10px] font-bold px-2 py-0.5 rounded-full ${task.priority === 'Urgent' ? 'bg-red-100 text-red-600' :
                                                    task.priority === 'Important' ? 'bg-amber-100 text-amber-600' :
                                                        'bg-slate-100 text-slate-500'
                                                    }`}>
                                                    {task.priority || 'Medium'}
                                                </span>
                                            </td>
                                            <td className="p-3">
                                                <span className={`text-[10px] font-bold ${task.status === 'Completed' ? 'text-emerald-500' : 'text-amber-500'
                                                    }`}>
                                                    {task.status}
                                                </span>
                                            </td>
                                        </tr>
                                    )) : (
                                        <tr>
                                            <td colSpan={6} className="p-8 text-center text-slate-400 italic">
                                                No tasks match the filters.
                                            </td>
                                        </tr>
                                    )}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>

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
        </StandardPageLayout>
    );
}
