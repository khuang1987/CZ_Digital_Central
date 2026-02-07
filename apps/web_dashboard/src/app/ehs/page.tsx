'use client';

import React, { useEffect, useState, useCallback, useMemo } from 'react';
import GreenCross, { DailyStatus } from '@/components/ehs/GreenCross';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, ComposedChart, Line, CartesianGrid, Legend, PieChart, Pie } from 'recharts';
import { ShieldCheck, AlertOctagon, Activity, HardHat, AlertTriangle, Calendar, Filter, ChevronLeft, ChevronRight, CheckCircle2, X, Construction, Zap, FileSpreadsheet, History, ShieldAlert, Download, Copy, MapPin, User, FileText, Tag } from 'lucide-react';
import * as XLSX from 'xlsx';
import { useUI } from '@/context/UIContext';
import FilterDropdown from '@/components/common/FilterDropdown';
import StandardPageLayout, { PageTab } from '@/components/StandardPageLayout';
import { t } from '@/lib/translations';

interface HazardTask {
    TaskId: string;
    title: string;
    description: string;
    date: string;
    status: string;
    classification: string;
    area: string;
    priority: string;
    creator: string;
    assignees: string;
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
    calendar?: CalendarData;
}

interface CalendarData {
    years: string[];
    months: Record<string, string[]>;
    currentFiscalInfo?: { fiscal_year: string, fiscal_month: string };
}

const DISPLAY_MODE_LABELS: Record<string, string> = { 'month': 'MONTHLY', 'week': 'WEEKLY', 'year': 'YEARLY' };

// Add global styles to remove chart focus rings
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

// Utility function to categorize task status (eliminates duplicate logic)
type StatusCategory = 'COMPLETED' | 'IN_PROGRESS' | 'NOT_STARTED';
const getStatusCategory = (status: string): StatusCategory => {
    const completedStatuses = ['Closed', 'Completed', '已完成', '已关闭'];
    const inProgressStatuses = ['In Progress', '进行中', '正在进行'];

    if (completedStatuses.includes(status)) return 'COMPLETED';
    if (inProgressStatuses.includes(status)) return 'IN_PROGRESS';
    return 'NOT_STARTED';
};

const isStatusCompleted = (status: string): boolean => {
    return ['Closed', 'Completed', '已完成', '已关闭'].includes(status);
};

export default function EHSDashboard({ theme, toggleTheme }: { theme?: 'light' | 'dark', toggleTheme?: () => void }) {
    const { language, toggleFilter, setResetHandler, setHasFilters } = useUI();
    const [data, setData] = useState<EHSData | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    // Constants for filter dropdowns
    const allProgress = ['Not Started', 'In Progress', 'Completed'];
    const allPriorities = ['Urgent', 'Important', 'Medium', 'Low'];

    // Filter State
    const [calendar, setCalendar] = useState<CalendarData | null>(null);
    const [timeMode, setTimeMode] = useState<'month' | 'year' | 'free'>('month');
    const [selectedYear, setSelectedYear] = useState<string>('');
    const [selectedMonth, setSelectedMonth] = useState<string>('');
    const [customStartDate, setCustomStartDate] = useState<string>('');
    const [customEndDate, setCustomEndDate] = useState<string>('');

    const [selectedIncident, setSelectedIncident] = useState<any>(null);

    const [selectedAreas, setSelectedAreas] = useState<string[]>([]);
    const [selectedLabels, setSelectedLabels] = useState<string[]>([]);
    const [selectedProgress, setSelectedProgress] = useState<string[]>([]);
    const [selectedPriority, setSelectedPriority] = useState<string[]>([]);

    const [hiddenIncidents, setHiddenIncidents] = useState<string[]>([]);
    const [isHiddenListOpen, setIsHiddenListOpen] = useState(false);

    const [selectedHazardMonth, setSelectedHazardMonth] = useState<string | null>(null);
    const [selectedHazardLabel, setSelectedHazardLabel] = useState<string | null>(null);
    const [selectedHazard, setSelectedHazard] = useState<HazardTask | null>(null);

    // Sorting State
    const [sortConfig, setSortConfig] = useState<{ key: keyof HazardTask; direction: 'asc' | 'desc' }>({
        key: 'date',
        direction: 'desc'
    });

    // Column Resizing State
    const [columnWidths, setColumnWidths] = useState<Record<string, number>>({
        index: 50,
        date: 100,
        area: 100,
        title: 250,
        classification: 150,
        creator: 120,
        assignees: 150,
        priority: 100,
        status: 100
    });

    const hazardMonthlyData = useMemo(() => {
        if (!data?.hazardTasks || !selectedYear) return [];

        const fiscalMonths = [
            { y: parseInt(selectedYear.replace(/\D/g, '')) - 1 + 2000, m: 5 },
            { y: parseInt(selectedYear.replace(/\D/g, '')) - 1 + 2000, m: 6 },
            { y: parseInt(selectedYear.replace(/\D/g, '')) - 1 + 2000, m: 7 },
            { y: parseInt(selectedYear.replace(/\D/g, '')) - 1 + 2000, m: 8 },
            { y: parseInt(selectedYear.replace(/\D/g, '')) - 1 + 2000, m: 9 },
            { y: parseInt(selectedYear.replace(/\D/g, '')) - 1 + 2000, m: 10 },
            { y: parseInt(selectedYear.replace(/\D/g, '')) - 1 + 2000, m: 11 },
            { y: parseInt(selectedYear.replace(/\D/g, '')) - 1 + 2000, m: 12 },
            { y: parseInt(selectedYear.replace(/\D/g, '')) + 2000, m: 1 },
            { y: parseInt(selectedYear.replace(/\D/g, '')) + 2000, m: 2 },
            { y: parseInt(selectedYear.replace(/\D/g, '')) + 2000, m: 3 },
            { y: parseInt(selectedYear.replace(/\D/g, '')) + 2000, m: 4 }
        ];

        const monthMap: Record<string, any> = {};
        fiscalMonths.forEach(({ y, m }) => {
            const key = `${y}-${String(m).padStart(2, '0')}`;
            const date = new Date(y, m - 1);
            monthMap[key] = {
                month: key,
                label: date.toLocaleDateString(language, { month: 'short' }).toUpperCase(),
                'NOT STARTED': 0,
                'IN PROGRESS': 0,
                'COMPLETED': 0
            };
        });

        data.hazardTasks.forEach(task => {
            const date = new Date(task.date);
            const monthKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;

            if (monthMap[monthKey]) {
                const statusCategory = getStatusCategory(task.status || 'Not Started');
                monthMap[monthKey][statusCategory]++;
            }
        });

        return Object.values(monthMap).sort((a: any, b: any) => a.month.localeCompare(b.month));
    }, [data?.hazardTasks, selectedYear, language]);

    const getMonthLabel = (monthKey: string) => {
        const item = hazardMonthlyData.find(d => d.month === monthKey);
        return item ? item.label : monthKey;
    };

    const hazardParetoData = useMemo(() => {
        if (!data?.hazardTasks) return [];
        let tasks = data.hazardTasks;

        if (selectedHazardMonth) {
            tasks = tasks.filter(task => {
                const dateParts = task.date.split('T')[0].split('-');
                const monthKey = `${dateParts[0]}-${dateParts[1]}`;
                return monthKey === selectedHazardMonth;
            });
        }

        const labelMap: Record<string, number> = {};
        const total = tasks.length;

        tasks.forEach(task => {
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
                cumulativePercentage: total > 0 ? (cumulative / total) * 100 : 0
            };
        });
    }, [data?.hazardTasks, selectedHazardMonth]);

    const filteredHazardTasks = useMemo(() => {
        if (!data?.hazardTasks) return [];
        const filtered = data.hazardTasks.filter(task => {
            if (selectedHazardMonth) {
                const dateParts = task.date.split('T')[0].split('-');
                const monthKey = `${dateParts[0]}-${dateParts[1]}`;
                if (monthKey !== selectedHazardMonth) return false;
            }
            if (selectedHazardLabel) {
                const labels = task.classification ? task.classification.split(';') : ['Unlabeled'];
                if (!labels.map(l => l.trim()).includes(selectedHazardLabel)) return false;
            }

            if (selectedAreas.length > 0 && !selectedAreas.includes(task.area)) return false;
            if (selectedPriority.length > 0 && !selectedPriority.includes(task.priority)) return false;
            if (selectedProgress.length > 0) {
                const statusCategory = getStatusCategory(task.status || 'Not Started');
                const matches = selectedProgress.some(p => {
                    if (p === 'Completed') return statusCategory === 'COMPLETED';
                    if (p === 'In Progress') return statusCategory === 'IN_PROGRESS';
                    if (p === 'Not Started') return statusCategory === 'NOT_STARTED';
                    return task.status === p;
                });
                if (!matches) return false;
            }

            if (selectedLabels.length > 0) {
                const taskLabels = task.classification ? task.classification.split(';').map(l => l.trim()) : ['Unlabeled'];
                const hasMatch = selectedLabels.some(l => taskLabels.includes(l));
                if (!hasMatch) return false;
            }

            return true;
        });

        return filtered.sort((a, b) => {
            const valA = a[sortConfig.key] || '';
            const valB = b[sortConfig.key] || '';
            if (sortConfig.key === 'date') {
                return sortConfig.direction === 'asc'
                    ? new Date(valA).getTime() - new Date(valB).getTime()
                    : new Date(valB).getTime() - new Date(valA).getTime();
            }
            if (sortConfig.direction === 'asc') {
                return String(valA).localeCompare(String(valB));
            } else {
                return String(valB).localeCompare(String(valA));
            }
        });
    }, [data?.hazardTasks, selectedHazardMonth, selectedHazardLabel, selectedAreas, selectedLabels, selectedProgress, selectedPriority, sortConfig]);

    const incidentStats = useMemo(() => {
        const visible = data?.incidents?.filter(inc => !hiddenIncidents.includes(inc.title)) || [];
        const closed = visible.filter(inc => isStatusCompleted(inc.status)).length;
        const open = visible.length - closed;
        return [
            { name: t('closed', language), value: closed, color: '#10b981' },
            { name: t('open', language), value: open, color: '#f59e0b' }
        ];
    }, [data?.incidents, hiddenIncidents, language]);

    const handleSort = useCallback((key: keyof HazardTask) => {
        setSortConfig(prev => ({
            key,
            direction: prev.key === key && prev.direction === 'desc' ? 'asc' : 'desc'
        }));
    }, []);

    const handleExport = useCallback(() => {
        if (!filteredHazardTasks || filteredHazardTasks.length === 0) return;
        const headers = [
            t('date', language), t('area', language), t('title_description', language), t('labels', language),
            t('created_by', language), t('assignees', language), t('priority', language), t('status', language)
        ];
        const rows = filteredHazardTasks.map(task => [
            new Date(task.date).toLocaleDateString(),
            task.area,
            task.title,
            task.classification || '',
            task.creator || '',
            task.assignees || '',
            task.priority || '',
            task.status || ''
        ]);
        let csvContent = "data:text/csv;charset=utf-8,\uFEFF" // Added BOM for Excel Chinese support
            + headers.join(",") + "\n"
            + rows.map(e => e.join(",")).join("\n");
        const encodedUri = encodeURI(csvContent);
        const link = document.createElement("a");
        link.setAttribute("href", encodedUri);
        link.setAttribute("download", `hazard_tasks_${new Date().toISOString().split('T')[0]}.csv`);
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }, [filteredHazardTasks, language]);

    const handleReset = useCallback(() => {
        setSelectedAreas([]);
        setSelectedLabels([]);
        setSelectedProgress([]);
        setSelectedPriority([]);
        setSelectedHazardMonth(null);
        setSelectedHazardLabel(null);
        setCustomStartDate('');
        setCustomEndDate('');
        // Restore to current fiscal month if available
        if (calendar?.currentFiscalInfo) {
            setSelectedYear(calendar.currentFiscalInfo.fiscal_year);
            setSelectedMonth(calendar.currentFiscalInfo.fiscal_month);
            setTimeMode('month');
        }
    }, [calendar]);

    const fetchData = useCallback(async () => {
        setLoading(true);
        setError(null);
        try {
            const params = new URLSearchParams();
            if (timeMode === 'month' && selectedYear && selectedMonth) {
                params.append('year', selectedYear);
                params.append('month', selectedMonth);
                params.append('mode', 'month');
            } else if (timeMode === 'year' && selectedYear) {
                params.append('year', selectedYear);
                params.append('mode', 'year');
            } else if (timeMode === 'free' && customStartDate && customEndDate) {
                params.append('start', customStartDate);
                params.append('end', customEndDate);
                params.append('mode', 'free');
            }

            const res = await fetch(`/api/ehs/stats?${params.toString()}`);
            if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
            const result = await res.json();
            setData(result);
            if (result.calendar) setCalendar(result.calendar);
        } catch (err: any) {
            setError(err.message || 'Failed to fetch data');
            console.error(err);
        } finally {
            setLoading(false);
        }
    }, [timeMode, selectedYear, selectedMonth, customStartDate, customEndDate]);

    const handleYearNavigate = useCallback((direction: number) => {
        if (!calendar?.years || !selectedYear) return;
        const idx = calendar.years.indexOf(selectedYear);
        if (idx === -1) return;
        const newIdx = idx + direction;
        if (newIdx >= 0 && newIdx < calendar.years.length) {
            setSelectedYear(calendar.years[newIdx]);
        }
    }, [calendar, selectedYear]);

    const handleMonthNavigate = useCallback((direction: number) => {
        if (!calendar?.months || !selectedYear || !selectedMonth) return;
        const months = calendar.months[selectedYear];
        if (!months) return;
        const idx = months.indexOf(selectedMonth);
        if (idx === -1) return;

        let newIdx = idx + direction;
        if (newIdx < 0) {
            // Go to prev year
            const yearIdx = calendar.years.indexOf(selectedYear);
            if (yearIdx > 0) {
                const prevYear = calendar.years[yearIdx - 1];
                const prevYearMonths = calendar.months[prevYear];
                setSelectedYear(prevYear);
                setSelectedMonth(prevYearMonths[prevYearMonths.length - 1]);
            }
        } else if (newIdx >= months.length) {
            // Go to next year
            const yearIdx = calendar.years.indexOf(selectedYear);
            if (yearIdx < calendar.years.length - 1) {
                const nextYear = calendar.years[yearIdx + 1];
                setSelectedYear(nextYear);
                setSelectedMonth(calendar.months[nextYear][0]);
            }
        } else {
            setSelectedMonth(months[newIdx]);
        }
    }, [calendar, selectedYear, selectedMonth]);

    const handleResize = useCallback((key: string, width: number) => {
        setColumnWidths(prev => ({ ...prev, [key]: Math.max(50, width) }));
    }, []);

    const toggleHideIncident = useCallback((title: string) => {
        setHiddenIncidents(prev =>
            prev.includes(title) ? prev.filter(t => t !== title) : [...prev, title]
        );
    }, []);

    const handleGreenCrossUpdate = async (date: string, status: DailyStatus, details?: string) => {
        if (!selectedYear || !selectedMonth) return;
        try {
            const res = await fetch('/api/ehs/green-cross', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ date, status, details })
            });
            if (res.ok) {
                // Refresh data to show updated status
                fetchData();
            }
        } catch (err) {
            console.error('Failed to update green cross:', err);
        }
    };

    // Derived values for GreenCross
    const displayYear = useMemo(() => {
        if (!selectedYear) return new Date().getFullYear();
        const y = parseInt(selectedYear.replace(/\D/g, ''));
        return y < 100 ? 2000 + y : y;
    }, [selectedYear]);

    const displayMonth = useMemo(() => {
        if (!selectedMonth) return new Date().getMonth() + 1;
        const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        const idx = months.indexOf(selectedMonth);
        return idx !== -1 ? idx + 1 : new Date().getMonth() + 1;
    }, [selectedMonth]);

    const greenCrossMap = useMemo(() => {
        const map: Record<number, { status: DailyStatus, details?: string }> = {};
        data?.greenCross?.forEach(item => {
            const d = new Date(item.date).getDate();
            map[d] = { status: item.status, details: item.details };
        });
        return map;
    }, [data?.greenCross]);

    useEffect(() => {
        fetchData();
    }, [fetchData]);

    useEffect(() => {
        setResetHandler(() => handleReset);
        setHasFilters(true);
    }, [setResetHandler, setHasFilters, handleReset]);

    // --- Helper Components ---
    const KpiCard = ({ title, value, icon, color, subtext }: any) => (
        <div className="bg-white dark:bg-slate-900 p-6 rounded-3xl border border-slate-200 dark:border-slate-800 shadow-sm flex items-center gap-6 group hover:border-medtronic transition-all">
            <div className={`p-4 rounded-2xl bg-slate-50 dark:bg-slate-800 ${color} shadow-inner group-hover:scale-110 transition-transform`}>
                {icon}
            </div>
            <div>
                <h4 className="text-[9px] font-black uppercase text-slate-400 tracking-widest mb-1">{t(title as any, language)}</h4>
                <div className="text-2xl font-black text-slate-900 dark:text-white leading-none mb-1.5">{value}</div>
                <div className="text-[10px] font-bold text-slate-500">{subtext}</div>
            </div>
        </div>
    );

    const HazardHeatmap = ({ data: heatmapData }: { data: any[] }) => {
        // Simple mock/placeholder for heatmap if component not found, but we want the actual one.
        // For now, let's just use a simplified version until we are sure.
        return (
            <div className="h-full w-full flex items-center justify-center text-slate-400 text-[10px] font-bold italic">
                {heatmapData.length > 0 ? (
                    <div className="grid grid-cols-12 gap-1 w-full h-full p-2">
                        {heatmapData.slice(0, 10).map((d, i) => (
                            <div key={i} className="flex flex-col gap-1 items-center justify-end">
                                <div
                                    className="w-full bg-medtronic rounded-t"
                                    style={{ height: `${Math.min(100, (d.count / 10) * 100)}%`, opacity: 0.3 + (d.count / 10) * 0.7 }}
                                />
                                <span className="text-[7px] truncate w-full text-center">{d.area}</span>
                            </div>
                        ))}
                    </div>
                ) : t('loading_data', language)}
            </div>
        );
    };

    // --- EHS Tabs ---
    const ehsTabs: PageTab[] = [
        { label: t('safety_dashboard', language), href: '/ehs', icon: <ShieldCheck size={14} />, active: true },
        { label: t('incident_log', language), href: '/ehs/incidents', icon: <FileSpreadsheet size={14} /> },
        { label: t('audit_history', language), href: '/ehs/audits', icon: <History size={14} /> },
    ];

    const areaOptions = useMemo(() => data?.filterOptions?.areas || [], [data?.filterOptions?.areas]);
    const classificationOptions = useMemo(() => Array.from(new Set(data?.incidents?.flatMap(inc => inc.classification ? inc.classification.split(';').map(l => l.trim()) : []) || [])), [data?.incidents]);

    const filters = (
        <div className="space-y-8 p-1">
            {/* 1. Time Selection */}
            <section tabIndex={0}>
                <h3 className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-4 flex items-center gap-2">
                    <Calendar size={12} /> {t('time_selection', language)}
                </h3>
                <div className="space-y-4">
                    <div className="flex gap-2">
                        {(['month', 'year', 'free'] as const).map(mode => (
                            <button
                                key={mode}
                                onClick={() => setTimeMode(mode)}
                                className={`flex-1 py-2 px-2 rounded-lg text-[10px] font-black uppercase tracking-tighter transition-all ${timeMode === mode
                                    ? 'bg-medtronic text-white shadow-lg'
                                    : 'bg-slate-50 dark:bg-slate-800 text-slate-500 hover:bg-slate-100 dark:hover:bg-slate-700'}`}
                            >
                                {mode === 'month' ? t('by_month', language) : mode === 'year' ? t('by_year', language) : t('custom_range', language)}
                            </button>
                        ))}
                    </div>

                    {/* Month Mode */}
                    {timeMode === 'month' && (
                        <>
                            <div className="flex items-center bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-700 rounded-xl group transition-all hover:border-medtronic/30">
                                <button onClick={() => handleYearNavigate(-1)} className="p-2.5 text-slate-400 hover:text-medtronic transition-colors border-r border-slate-200/50 dark:border-slate-700"><ChevronLeft size={14} /></button>
                                <div className="flex-1 relative">
                                    <select value={selectedYear || ''} onChange={(e) => setSelectedYear(e.target.value)} className="w-full bg-transparent text-slate-800 dark:text-slate-200 px-3 py-2 text-xs font-bold outline-none appearance-none cursor-pointer text-center">
                                        {(data?.calendar?.years || []).map(y => <option key={y} value={y}>{y}</option>)}
                                    </select>
                                </div>
                                <button onClick={() => handleYearNavigate(1)} className="p-2.5 text-slate-400 hover:text-medtronic transition-colors border-l border-slate-200/50 dark:border-slate-700"><ChevronRight size={14} /></button>
                            </div>

                            <div className="flex items-center bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-700 rounded-xl group transition-all hover:border-medtronic/30">
                                <button onClick={() => handleMonthNavigate(-1)} className="p-2.5 text-slate-400 hover:text-medtronic transition-colors border-r border-slate-200/50 dark:border-slate-700"><ChevronLeft size={14} /></button>
                                <div className="flex-1 relative">
                                    <select value={selectedMonth || ''} onChange={(e) => setSelectedMonth(e.target.value)} className="w-full bg-transparent text-slate-800 dark:text-slate-200 px-3 py-2 text-xs font-bold outline-none appearance-none cursor-pointer text-center">
                                        {(data?.calendar?.months?.[selectedYear || ''] || []).map(m => <option key={m} value={m}>{m}</option>)}
                                    </select>
                                </div>
                                <button onClick={() => handleMonthNavigate(1)} className="p-2.5 text-slate-400 hover:text-medtronic transition-colors border-l border-slate-200/50 dark:border-slate-700"><ChevronRight size={14} /></button>
                            </div>
                        </>
                    )}

                    {/* Year Mode */}
                    {timeMode === 'year' && (
                        <div className="flex items-center bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-700 rounded-xl group transition-all hover:border-medtronic/30">
                            <button onClick={() => handleYearNavigate(-1)} className="p-2.5 text-slate-400 hover:text-medtronic transition-colors border-r border-slate-200/50 dark:border-slate-700"><ChevronLeft size={14} /></button>
                            <div className="flex-1 relative">
                                <select value={selectedYear || ''} onChange={(e) => setSelectedYear(e.target.value)} className="w-full bg-transparent text-slate-800 dark:text-slate-200 px-3 py-2 text-xs font-bold outline-none appearance-none cursor-pointer text-center">
                                    {(data?.calendar?.years || []).map(y => <option key={y} value={y}>{y}</option>)}
                                </select>
                            </div>
                            <button onClick={() => handleYearNavigate(1)} className="p-2.5 text-slate-400 hover:text-medtronic transition-colors border-l border-slate-200/50 dark:border-slate-700"><ChevronRight size={14} /></button>
                        </div>
                    )}

                    {/* Free Mode */}
                    {timeMode === 'free' && (
                        <div className="space-y-2">
                            <div className="relative">
                                <span className="absolute left-3 top-1/2 -translate-y-1/2 text-[9px] font-black text-slate-400 uppercase tracking-tighter">{t('start', language)}</span>
                                <input
                                    type="date"
                                    value={customStartDate}
                                    onChange={(e) => setCustomStartDate(e.target.value)}
                                    className="w-full bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-700 rounded-xl pl-12 pr-4 py-2 text-xs font-bold outline-none focus:ring-1 focus:ring-medtronic/30"
                                />
                            </div>
                            <div className="relative">
                                <span className="absolute left-3 top-1/2 -translate-y-1/2 text-[9px] font-black text-slate-400 uppercase tracking-tighter">{t('end', language)}</span>
                                <input
                                    type="date"
                                    value={customEndDate}
                                    onChange={(e) => setCustomEndDate(e.target.value)}
                                    className="w-full bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-700 rounded-xl pl-12 pr-4 py-2 text-xs font-bold outline-none focus:ring-1 focus:ring-medtronic/30"
                                />
                            </div>
                        </div>
                    )}
                </div>
            </section>

            <div className="h-px bg-slate-100 dark:bg-slate-800" />

            {/* 2. Areas */}
            <section tabIndex={0}>
                <h3 className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-4">
                    {t('area', language)}
                </h3>
                <FilterDropdown
                    title={t('area', language)}
                    options={areaOptions}
                    selected={selectedAreas}
                    onChange={setSelectedAreas}
                    placeholder={t('search_area', language)}
                />
            </section>

            {/* 3. Classification / Priority / Progress */}
            <section tabIndex={0} className="space-y-6">
                <div>
                    <h3 className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-3">
                        {t('classification', language)}
                    </h3>
                    <FilterDropdown
                        title={t('classification', language)}
                        options={classificationOptions}
                        selected={selectedLabels}
                        onChange={setSelectedLabels}
                        placeholder={t('classification', language)}
                    />
                </div>

                <div>
                    <h3 className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-3">
                        {t('priority', language)}
                    </h3>
                    <FilterDropdown
                        title={t('priority', language)}
                        options={allPriorities}
                        selected={selectedPriority}
                        onChange={setSelectedPriority}
                        placeholder={t('priority', language)}
                    />
                </div>

                <div>
                    <h3 className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-3">
                        {t('status', language)}
                    </h3>
                    <FilterDropdown
                        title={t('status', language)}
                        options={allProgress}
                        selected={selectedProgress}
                        onChange={setSelectedProgress}
                        placeholder={t('status', language)}
                    />
                </div>
            </section>

            <button
                onClick={handleReset}
                className="w-full mt-2 py-2.5 bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-400 rounded-xl text-xs font-bold hover:bg-slate-200 dark:hover:bg-slate-700 transition-all flex items-center justify-center gap-2"
            >
                <X size={14} />
                {t('reset_filters', language)}
            </button>
        </div>
    );

    if (loading) return (
        <StandardPageLayout
            theme={theme}
            toggleTheme={toggleTheme}
            title={t('ehs_title', language)}
            description={t('ehs_desc', language)}
            icon={<ShieldCheck size={24} />}
            tabs={ehsTabs}
            filters={filters}
            onReset={handleReset}
            hideFilterTitle={true}
        >
            <div className="flex flex-col items-center justify-center h-[60vh] text-slate-400">
                <Activity size={48} className="mb-4 animate-spin" />
                <h3 className="text-xl font-bold">{t('loading_data', language)}</h3>
                <p className="text-sm opacity-80">{t('fetching_dashboard_data', language)}</p>
            </div>
        </StandardPageLayout>
    );

    if (error) return (
        <StandardPageLayout
            theme={theme}
            toggleTheme={toggleTheme}
            title={t('ehs_title', language)}
            description={t('ehs_desc', language)}
            icon={<ShieldCheck size={24} />}
            tabs={ehsTabs}
            filters={filters}
            onReset={handleReset}
            hideFilterTitle={true}
        >
            <div className="flex flex-col items-center justify-center h-[60vh] text-red-500">
                <AlertTriangle size={48} className="mb-4" />
                <h3 className="text-xl font-bold">{t('error_loading_data', language)}</h3>
                <p className="text-sm opacity-80">{error}</p>
                <button onClick={() => fetchData()} className="mt-4 px-6 py-2 bg-red-50 text-red-600 rounded-xl font-bold focus:outline-none">{t('retry', language)}</button>
            </div>
        </StandardPageLayout>
    );

    return (
        <StandardPageLayout
            theme={theme}
            toggleTheme={toggleTheme}
            title={t('ehs_title', language)}
            description={t('ehs_desc', language)}
            icon={<ShieldCheck size={20} />}
            tabs={ehsTabs}
            filters={filters}
            onReset={handleReset}
            hideFilterTitle={true}
        >
            <div className="space-y-8 min-h-full">
                {error && (
                    <div className="max-w-[1600px] mx-auto w-full mb-4 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-xl flex items-center gap-3 text-red-600 dark:text-red-400 animate-in fade-in slide-in-from-top-2 shrink-0">
                        <AlertTriangle size={16} />
                        <div className="text-xs font-bold">
                            API Error: {error}. {t('data_incomplete_missing', language)}
                        </div>
                    </div>
                )}

                <div className="flex-1 flex flex-col gap-4 max-w-[1700px] mx-auto w-full min-h-0">
                    <div className="grid grid-cols-4 gap-4 shrink-0">
                        <KpiCard
                            title={t('trir', language)}
                            value="0.12"
                            icon={<Activity size={32} />}
                            color="text-emerald-500"
                            subtext={t('ytd_current_year', language, { year: selectedYear || t('current_year', language) })}
                        />
                        <KpiCard
                            title={t('first_aid_safe_days', language)}
                            value={data?.stats?.safeDays ?? '-'}
                            icon={<ShieldCheck size={32} />}
                            color="text-emerald-500"
                            subtext={t('cumulative', language)}
                        />
                        <KpiCard
                            title={t('first_aid_ytd', language)}
                            value={data?.incidents
                                ? data.incidents.filter(inc => !hiddenIncidents.includes(inc.title)).length
                                : '-'}
                            icon={<AlertTriangle size={32} />}
                            color="text-red-500"
                            subtext={t('fiscal_total', language, { year: selectedYear || t('fiscal', language) })}
                        />
                        <KpiCard
                            title={t('open_safety_hazards', language)}
                            value={filteredHazardTasks.filter(t => {
                                const s = t.status || '';
                                return !(s === 'Completed' || s === 'Closed' || s === '已完成' || s === '已关闭');
                            }).length}
                            icon={<AlertOctagon size={32} />}
                            color="text-amber-500"
                            subtext={t('active_tasks_filtered', language)}
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
                                    <h3 className="text-[9px] font-black text-slate-400 uppercase tracking-widest flex items-center gap-2">
                                        <AlertTriangle size={10} className="text-red-500" />
                                        {t('incident_log_fiscal_ytd', language)}
                                    </h3>
                                    <button
                                        onClick={() => setIsHiddenListOpen(true)}
                                        className="p-1 px-2 rounded-lg bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-700 text-slate-400 hover:text-medtronic transition-all flex items-center gap-1.5 group"
                                        title={t('manage_hidden_incidents', language)}
                                    >
                                        <Filter size={10} />
                                        <span className="text-[8px] font-bold">{t('recovery', language)} {hiddenIncidents.length > 0 && `(${hiddenIncidents.length})`}</span>
                                    </button>
                                </div>
                                <div className="flex-1 flex gap-6 overflow-hidden min-h-0">
                                    <div className="flex-[1.2] overflow-y-auto pr-1 custom-scrollbar">
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
                                                                <span className={`text-[8px] font-black uppercase tracking-tighter shrink-0 px-1.5 py-0.5 rounded-md ${(incident.status === 'Completed' || incident.status === 'Closed' || incident.status === '已完成' || incident.status === '已关闭')
                                                                    ? 'bg-emerald-100 text-emerald-600 dark:bg-emerald-900/30 dark:text-emerald-400'
                                                                    : 'bg-amber-100 text-amber-600 dark:bg-amber-900/30 dark:text-amber-400'
                                                                    }`}>
                                                                    {(incident.status === 'Completed' || incident.status === 'Closed' || incident.status === '已完成' || incident.status === '已关闭') ? t('closed', language) : t('open', language)}
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
                                                        ? t('all_incidents_hidden', language)
                                                        : t('no_recorded_first_aid_incidents', language)}
                                                </div>
                                            )}
                                        </div>
                                    </div>

                                    {/* Donut Chart Portion */}
                                    <div className="flex-1 border-l border-slate-100 dark:border-slate-800/50 pl-6 flex flex-col justify-center relative">
                                        <div className="absolute inset-x-0 top-1/2 -translate-y-1/2 flex flex-col items-center justify-center pointer-events-none mt-2">
                                            <div className="text-[8px] font-black text-slate-400 uppercase tracking-widest leading-none mb-1">{t('incidents', language)}</div>
                                            <div className="text-xl font-black text-slate-900 dark:text-white leading-none">
                                                {incidentStats.reduce((acc, curr) => acc + curr.value, 0)}
                                            </div>
                                        </div>
                                        <div className="h-[180px] w-full">
                                            <ResponsiveContainer width="100%" height="100%">
                                                <PieChart>
                                                    <Pie
                                                        data={incidentStats}
                                                        cx="50%"
                                                        cy="50%"
                                                        innerRadius={52}
                                                        outerRadius={72}
                                                        paddingAngle={5}
                                                        dataKey="value"
                                                        stroke="none"
                                                    >
                                                        {incidentStats.map((entry, index) => (
                                                            <Cell key={`cell-${index}`} fill={entry.color} />
                                                        ))}
                                                    </Pie>
                                                    <Tooltip
                                                        contentStyle={{ backgroundColor: theme === 'dark' ? '#0f172a' : '#fff', borderRadius: '12px', border: 'none', boxShadow: '0 10px 15px -3px rgb(0 0 0 / 0.1)' }}
                                                        itemStyle={{ fontSize: '10px', fontWeight: 'bold' }}
                                                    />
                                                </PieChart>
                                            </ResponsiveContainer>
                                        </div>
                                        <div className="flex justify-center gap-4 mt-2">
                                            {incidentStats.map((stat, i) => (
                                                <div key={i} className="flex items-center gap-1.5 shrink-0">
                                                    <div className="w-2 h-2 rounded-full" style={{ backgroundColor: stat.color }} />
                                                    <span className="text-[8px] font-black text-slate-500 uppercase tracking-widest">{stat.name}</span>
                                                    <span className="text-[8px] font-black text-slate-400">{stat.value}</span>
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                </div>
                            </div>

                            <div className="flex-1 bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-4 flex flex-col shadow-sm min-h-0 overflow-hidden">
                                <h3 className="text-[9px] font-black text-slate-400 uppercase tracking-widest mb-1.5 shrink-0">{t('hazards_heatmap_top_10_areas', language)}</h3>
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
                        {t('hazard_task_analysis', language)}
                        {data?.hazardTasks && (
                            <span className="text-xs px-2 py-1 rounded bg-slate-100 dark:bg-slate-800 text-slate-500">
                                {t('total', language)}: {data.hazardTasks.length}
                            </span>
                        )}
                    </h3>

                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8 h-[350px]">
                        <div className="p-4 bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm flex flex-col">
                            <h4 className="text-xs font-bold text-slate-500 uppercase tracking-widest mb-4">{t('monthly_trend_click_to_filter', language)}</h4>
                            <div className="flex-1 w-full min-h-0">
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart
                                        data={hazardMonthlyData}
                                        style={{ outline: 'none', border: 'none' }}
                                        onClick={(state: any) => {
                                            if (state && state.activeLabel) {
                                                // Priority 1: Payload from explicit month key
                                                let clickedMonth = state.activePayload?.[0]?.payload?.month;

                                                // Priority 2: Match by label if payload is missing
                                                if (!clickedMonth) {
                                                    clickedMonth = hazardMonthlyData.find(d => d.label === state.activeLabel)?.month;
                                                }

                                                // Priority 3: If activeLabel ITSELF is the key
                                                if (!clickedMonth && /^\d{4}-\d{2}$/.test(state.activeLabel)) {
                                                    clickedMonth = state.activeLabel;
                                                }

                                                if (clickedMonth) {
                                                    setSelectedHazardMonth(prev => prev === clickedMonth ? null : clickedMonth);
                                                }
                                            }
                                        }}
                                    >
                                        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                                        <XAxis
                                            dataKey="month"
                                            tickFormatter={(val) => hazardMonthlyData.find(d => d.month === val)?.label || val}
                                            tick={{ fontSize: 10, fontWeight: 'bold' }}
                                            axisLine={false}
                                            tickLine={false}
                                        />
                                        <YAxis tick={{ fontSize: 10 }} axisLine={false} tickLine={false} />
                                        <Tooltip
                                            contentStyle={{ borderRadius: '12px', border: 'none', boxShadow: '0 10px 15px -3px rgb(0 0 0 / 0.1)', fontSize: '12px' }}
                                            cursor={false}
                                        />
                                        <Legend verticalAlign="top" height={36} iconType="circle" wrapperStyle={{ fontSize: '10px', fontWeight: 'bold', textTransform: 'uppercase' }} />
                                        {['NOT STARTED', 'IN PROGRESS', 'COMPLETED'].map((key) => (
                                            <Bar
                                                key={key}
                                                name={t(key.toLowerCase().replace(' ', '_') as any, language)}
                                                dataKey={key}
                                                stackId="a"
                                                fill={key === 'NOT STARTED' ? '#ef4444' : key === 'IN PROGRESS' ? '#f59e0b' : '#10b981'}
                                                radius={key === 'COMPLETED' ? [4, 4, 0, 0] : [0, 0, 0, 0]}
                                            >
                                                {hazardMonthlyData.map((entry, index) => (
                                                    <Cell
                                                        key={`cell-${index}`}
                                                        fillOpacity={!selectedHazardMonth || selectedHazardMonth === entry.month ? 1 : 0.25}
                                                        strokeWidth={0}
                                                    />
                                                ))}
                                            </Bar>
                                        ))}
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                        </div>

                        <div className="p-4 bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm flex flex-col">
                            <h4 className="text-xs font-bold text-slate-500 uppercase tracking-widest mb-4">{t('pareto_by_issue_type', language)}</h4>
                            <div className="flex-1 w-full min-h-0">
                                <ResponsiveContainer width="100%" height="100%">
                                    <ComposedChart
                                        data={hazardParetoData}
                                        style={{ outline: 'none', border: 'none' }}
                                        onClick={(data: any) => {
                                            if (data && data.activePayload && data.activePayload.length > 0) {
                                                const clickedLabel = data.activePayload[0].payload.label;
                                                setSelectedHazardLabel(prev => prev === clickedLabel ? null : clickedLabel);
                                            }
                                        }}
                                    >
                                        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                                        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                                        <XAxis dataKey="label" tick={{ fontSize: 10 }} interval={0} angle={-30} textAnchor="end" height={60} axisLine={false} tickLine={false} />
                                        <YAxis yAxisId="left" tick={{ fontSize: 10 }} axisLine={false} tickLine={false} />
                                        <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 10 }} unit="%" axisLine={false} tickLine={false} />
                                        <Tooltip
                                            contentStyle={{ borderRadius: '12px', border: 'none', boxShadow: '0 10px 15px -3px rgb(0 0 0 / 0.1)', fontSize: '12px' }}
                                            cursor={false}
                                        />
                                        <Legend verticalAlign="top" height={36} />
                                        <Bar yAxisId="left" dataKey="count" fill="#3b82f6" barSize={40} radius={[4, 4, 0, 0]}>
                                            {hazardParetoData.map((entry, index) => (
                                                <Cell
                                                    key={`cell-${index}`}
                                                    fillOpacity={!selectedHazardLabel || selectedHazardLabel === entry.label ? 1 : 0.25}
                                                    strokeWidth={0}
                                                />
                                            ))}
                                        </Bar>
                                        <Line yAxisId="right" type="monotone" dataKey="cumulativePercentage" stroke="#ef4444" strokeWidth={2} dot={{ r: 3 }} name={t('cumulative_percentage', language)} />
                                    </ComposedChart>
                                </ResponsiveContainer>
                            </div>
                        </div>
                    </div>
                </div>

                <div className="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 overflow-hidden shadow-sm mb-8">
                    <div className="p-4 border-b border-slate-100 dark:border-slate-800 bg-slate-50/50 dark:bg-slate-800/20 flex justify-between items-center">
                        <h4 className="text-xs font-black text-slate-500 uppercase tracking-widest">
                            {t('task_detail_list', language)} ({filteredHazardTasks.length})
                        </h4>
                        <div className="flex gap-2">
                            <button
                                onClick={handleExport}
                                className="px-3 py-1 text-[10px] font-bold bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg hover:bg-slate-50 dark:hover:bg-slate-700 transition-all flex items-center gap-1.5 text-slate-600 dark:text-slate-300 shadow-sm"
                            >
                                <Download size={12} />
                                {t('export_xlsx', language)}
                            </button>
                            {(selectedHazardMonth || selectedHazardLabel) && (
                                <button
                                    onClick={() => { setSelectedHazardMonth(null); setSelectedHazardLabel(null); }}
                                    className="px-3 py-1 text-[10px] font-bold bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg hover:bg-slate-50 dark:hover:bg-slate-700 transition-all"
                                >
                                    {t('reset_filters', language)}
                                </button>
                            )}
                        </div>
                    </div>
                    <div className="overflow-x-auto overflow-y-auto custom-scrollbar" style={{ maxHeight: '400px' }}>
                        <table className="w-full text-left text-xs table-fixed">
                            <thead className="sticky top-0 bg-slate-50 dark:bg-slate-800 text-slate-500 font-bold uppercase tracking-wider z-10 select-none">
                                <tr>
                                    {[
                                        { key: 'index', label: '#' },
                                        { key: 'date', label: t('date', language) },
                                        { key: 'area', label: t('area', language) },
                                        { key: 'title', label: t('title_description', language) },
                                        { key: 'classification', label: t('labels', language) },
                                        { key: 'creator', label: t('created_by', language) },
                                        { key: 'assignees', label: t('assignees', language) },
                                        { key: 'priority', label: t('priority', language) },
                                        { key: 'status', label: t('status', language) }
                                    ].map((col) => (
                                        <th
                                            key={col.key}
                                            className="p-3 relative group transition-colors hover:bg-slate-100 dark:hover:bg-slate-700 cursor-pointer"
                                            style={{ width: columnWidths[col.key] || 'auto' }}
                                            onClick={() => handleSort(col.key as any)}
                                        >
                                            <div className="flex items-center gap-1">
                                                {col.label}
                                                {sortConfig.key === col.key && (
                                                    <span className="text-medtronic">
                                                        {sortConfig.direction === 'asc' ? '↑' : '↓'}
                                                    </span>
                                                )}
                                            </div>
                                            {/* Resize Handle */}
                                            <div
                                                className="absolute right-0 top-0 bottom-0 w-1 cursor-col-resize hover:bg-medtronic transition-colors z-20"
                                                onMouseDown={(e) => {
                                                    e.stopPropagation();
                                                    const startX = e.pageX;
                                                    const startWidth = columnWidths[col.key] || 100;
                                                    const onMouseMove = (moveEvent: MouseEvent) => {
                                                        handleResize(col.key, startWidth + (moveEvent.pageX - startX));
                                                    };
                                                    const onMouseUp = () => {
                                                        document.removeEventListener('mousemove', onMouseMove);
                                                        document.removeEventListener('mouseup', onMouseUp);
                                                    };
                                                    document.addEventListener('mousemove', onMouseMove);
                                                    document.addEventListener('mouseup', onMouseUp);
                                                }}
                                            />
                                        </th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                                {filteredHazardTasks.length > 0 ? filteredHazardTasks.map((task, i) => {
                                    // Progress Mapping
                                    const s = task.status;
                                    const isCompleted = s === 'Completed' || s === 'Closed' || s === '已完成' || s === '已关闭';
                                    const isInProgress = s === 'In Progress' || s === '进行中' || s === '正在进行';

                                    let progress = 0;
                                    if (isInProgress) progress = 50;
                                    else if (isCompleted) progress = 100;

                                    return (
                                        <tr
                                            key={task.TaskId}
                                            className="hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors group cursor-pointer border-l-2 border-transparent hover:border-medtronic"
                                            onClick={() => setSelectedHazard(task)}
                                        >
                                            <td className="p-3 text-slate-400 font-mono text-[10px]" style={{ width: columnWidths.index }}>
                                                {i + 1}
                                            </td>
                                            <td className="p-3 font-mono text-slate-500 truncate" style={{ width: columnWidths.date }}>
                                                {new Date(task.date).toLocaleDateString()}
                                            </td>
                                            <td className="p-3 font-bold text-slate-700 dark:text-slate-300 truncate" style={{ width: columnWidths.area }}>
                                                {task.area}
                                            </td>
                                            <td className="p-3 font-medium text-slate-800 dark:text-slate-200" style={{ width: columnWidths.title }}>
                                                <div className="truncate shrink-0" title={task.title}>{task.title}</div>
                                            </td>
                                            <td className="p-3" style={{ width: columnWidths.classification }}>
                                                <div className="flex flex-wrap gap-1 overflow-hidden h-5">
                                                    {task.classification ? task.classification.split(';').map((l, i) => (
                                                        <span key={i} className="px-1.5 py-0.5 bg-slate-100 dark:bg-slate-800 text-slate-500 rounded text-[10px] whitespace-nowrap">
                                                            {l}
                                                        </span>
                                                    )) : <span className="text-slate-300">-</span>}
                                                </div>
                                            </td>
                                            <td className="p-3" style={{ width: columnWidths.creator }}>
                                                <div className="text-[10px] font-bold text-slate-600 dark:text-slate-400 truncate" title={task.creator}>
                                                    {task.creator || <span className="text-slate-300">-</span>}
                                                </div>
                                            </td>
                                            <td className="p-3" style={{ width: columnWidths.assignees }}>
                                                <div className="text-[10px] font-bold text-slate-600 dark:text-slate-400 truncate" title={task.assignees}>
                                                    {task.assignees || <span className="text-slate-300">-</span>}
                                                </div>
                                            </td>
                                            <td className="p-3" style={{ width: columnWidths.priority }}>
                                                <span className={`text-[10px] font-bold px-2 py-0.5 rounded-full ${task.priority === 'Urgent' ? 'bg-red-100 text-red-600' :
                                                    task.priority === 'Important' ? 'bg-amber-100 text-amber-600' :
                                                        'bg-slate-100 text-slate-500'
                                                    }`}>
                                                    {task.priority || t('medium', language)}
                                                </span>
                                            </td>
                                            <td className="p-3" style={{ width: columnWidths.status }}>
                                                <div className="flex flex-col gap-1">
                                                    <span
                                                        className={`text-[10px] font-black uppercase tracking-widest ${isCompleted ? 'text-emerald-500' : isInProgress ? 'text-amber-500' : 'text-slate-400'}`}
                                                        style={{
                                                            filter: isCompleted
                                                                ? 'drop-shadow(0 0 3px rgba(16, 185, 129, 0.4))'
                                                                : isInProgress
                                                                    ? 'drop-shadow(0 0 3px rgba(245, 158, 11, 0.4))'
                                                                    : 'none'
                                                        }}
                                                    >
                                                        {t((task.status?.toLowerCase().replace(' ', '_') || 'not_started') as any, language)}
                                                    </span>
                                                    <div className="w-full bg-slate-100 dark:bg-slate-800 rounded-full h-1 overflow-hidden">
                                                        <div
                                                            className={`h-full transition-all duration-500 ${isCompleted ? 'bg-emerald-500' : isInProgress ? 'bg-amber-500' : 'bg-slate-300'}`}
                                                            style={{ width: `${progress}%` }}
                                                        />
                                                    </div>
                                                </div>
                                            </td>
                                        </tr>
                                    );
                                }) : (
                                    <tr>
                                        <td colSpan={9} className="p-8 text-center text-slate-400 italic">
                                            {t('no_tasks_match_filters', language)}
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
                                <h3 className="text-sm font-black text-slate-800 dark:text-white uppercase tracking-widest">{t('hidden_incidents', language)}</h3>
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
                                                className="px-3 py-1 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 text-[9px] font-black text-emerald-500 uppercase tracking-widest hover:bg-emerald-50 transition-all shadow-sm"
                                            >
                                                {t('release', language)}
                                            </button>
                                        </div>
                                    ))
                                ) : (
                                    <div className="text-center py-8 text-xs text-slate-400 italic font-medium">{t('no_hidden_incidents_found', language)}</div>
                                )}
                            </div>

                            <button
                                onClick={() => setIsHiddenListOpen(false)}
                                className="w-full mt-6 py-3 bg-slate-950 dark:bg-white dark:text-slate-950 text-white rounded-xl text-xs font-black uppercase tracking-widest hover:scale-[1.02] active:scale-[0.98] transition-all"
                            >
                                {t('done', language)}
                            </button>
                        </div>
                    </div>
                </div>
            )}
            {selectedHazard && (
                <div
                    className="fixed inset-0 z-[120] flex items-center justify-center p-4 bg-slate-950/40 backdrop-blur-[6px] animate-in fade-in duration-300"
                    onClick={(e) => {
                        if (e.target === e.currentTarget) setSelectedHazard(null);
                    }}
                >
                    <div className="bg-white dark:bg-slate-900 w-full max-w-2xl rounded-[32px] shadow-2xl overflow-hidden border border-slate-200 dark:border-slate-800 animate-in zoom-in-95 duration-300 flex flex-col max-h-[95vh]">
                        {/* Priority Accent Header */}
                        <div className={`h-1.5 w-full ${selectedHazard.priority === 'Urgent' ? 'bg-red-500' : selectedHazard.priority === 'Important' ? 'bg-amber-500' : 'bg-blue-500'}`} />

                        <div className="p-10 overflow-y-auto custom-scrollbar">
                            <div className="flex justify-between items-start mb-6">
                                <div className="flex-1 min-w-0 pr-8">
                                    <div className="flex items-center gap-3 mb-3">
                                        <div className={`px-2.5 py-0.5 rounded-full text-[9px] font-black uppercase tracking-widest ${selectedHazard.priority === 'Urgent' ? 'bg-red-50 text-red-600 dark:bg-red-900/30 dark:text-red-400' :
                                            selectedHazard.priority === 'Important' ? 'bg-amber-50 text-amber-600 dark:bg-amber-900/30' :
                                                'bg-blue-50 text-blue-600 dark:bg-blue-900/30'
                                            }`}>
                                            {selectedHazard.priority || t('medium', language)} {t('priority', language)}
                                        </div>
                                        <button
                                            onClick={() => {
                                                navigator.clipboard.writeText(selectedHazard.TaskId);
                                                // Could add a toast here if available, using native alert for simplicity in this context
                                            }}
                                            className="group flex items-center gap-1.5 px-2 py-0.5 rounded-md hover:bg-slate-100 dark:hover:bg-slate-800 transition-colors"
                                            title={t('click_to_copy_id', language)}
                                        >
                                            <span className="text-[10px] font-bold text-slate-400 font-mono">#{selectedHazard.TaskId}</span>
                                            <Copy size={10} className="text-slate-300 group-hover:text-medtronic transition-colors" />
                                        </button>
                                    </div>
                                    <h2 className="text-2xl font-black text-slate-900 dark:text-white leading-tight tracking-tight mb-4">{selectedHazard.title}</h2>

                                    <div className="text-[14px] leading-relaxed text-slate-600 dark:text-slate-400 font-medium whitespace-pre-wrap">
                                        {selectedHazard.description || t('no_hazard_description_provided', language)}
                                    </div>
                                </div>
                                <button onClick={() => setSelectedHazard(null)} className="p-2.5 bg-slate-50 dark:bg-slate-800 hover:bg-slate-100 dark:hover:bg-slate-700 rounded-2xl transition-all text-slate-400 hover:text-red-500">
                                    <X size={20} />
                                </button>
                            </div>

                            {/* Minimal Metadata Row */}
                            <div className="pt-8 border-t border-slate-100 dark:border-slate-800 grid grid-cols-2 lg:grid-cols-4 gap-y-6 gap-x-8">
                                <div>
                                    <div className="text-[9px] font-black text-slate-400 uppercase tracking-widest mb-1.5 flex items-center gap-1.5">
                                        <User size={10} /> {t('created_by', language)}
                                    </div>
                                    <div className="text-[12px] font-bold text-slate-700 dark:text-slate-200 truncate" title={selectedHazard.creator}>
                                        {selectedHazard.creator || t('system', language)}
                                    </div>
                                </div>
                                <div>
                                    <div className="text-[9px] font-black text-slate-400 uppercase tracking-widest mb-1.5 flex items-center gap-1.5">
                                        <HardHat size={10} /> {t('assignees', language)}
                                    </div>
                                    <div className="text-[12px] font-bold text-slate-700 dark:text-slate-200 truncate" title={selectedHazard.assignees}>
                                        {selectedHazard.assignees || t('unassigned', language)}
                                    </div>
                                </div>
                                <div>
                                    <div className="text-[9px] font-black text-slate-400 uppercase tracking-widest mb-1.5 flex items-center gap-1.5">
                                        <MapPin size={10} /> {t('incident_area', language)}
                                    </div>
                                    <div className="text-[12px] font-bold text-slate-700 dark:text-slate-200 truncate">
                                        {selectedHazard.area}
                                    </div>
                                </div>
                                <div>
                                    <div className="text-[9px] font-black text-slate-400 uppercase tracking-widest mb-1.5 flex items-center gap-1.5">
                                        <Calendar size={10} /> {t('created_date', language)}
                                    </div>
                                    <div className="text-[12px] font-bold text-slate-700 dark:text-slate-200">
                                        {new Date(selectedHazard.date).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })}
                                        <span className="ml-2 text-[9px] font-bold text-slate-400">
                                            ({(() => {
                                                const days = Math.floor((new Date().getTime() - new Date(selectedHazard.date).getTime()) / (1000 * 3600 * 24));
                                                return days === 0 ? t('today', language) : `${days} ${t('days_ago' as any, language)}`;
                                            })()})
                                        </span>
                                    </div>
                                </div>
                            </div>

                            <div className="mt-8 pt-8 border-t border-slate-100 dark:border-slate-800">
                                <div className="text-[9px] font-black text-slate-400 uppercase tracking-widest mb-3 flex items-center gap-2">
                                    <Tag size={10} /> {t('classifications', language)}
                                </div>
                                <div className="flex flex-wrap gap-1.5">
                                    {selectedHazard.classification ? selectedHazard.classification.split(';').map((label, i) => (
                                        <span key={i} className="px-2.5 py-1 bg-slate-50 dark:bg-slate-800 border border-slate-100 dark:border-slate-800 rounded-lg text-[10px] font-bold text-slate-500 dark:text-slate-400">
                                            #{label.trim()}
                                        </span>
                                    )) : <span className="text-slate-400 italic text-xs">{t('no_labels_applied', language)}</span>}
                                </div>
                            </div>
                        </div>

                        {/* Premium Integrated Footer */}
                        <div className="px-8 py-6 bg-slate-50 dark:bg-slate-800/50 border-t border-slate-100 dark:border-slate-800 flex items-center justify-between">
                            <div className="flex items-center gap-6 flex-1">
                                <div className="flex items-center gap-4">
                                    <div className={`p-3 rounded-2xl shadow-sm ${(selectedHazard.status === 'Completed' || selectedHazard.status === 'Closed' || selectedHazard.status === '已完成' || selectedHazard.status === '已关闭') ? 'bg-emerald-500 text-white' : (selectedHazard.status === 'In Progress' || selectedHazard.status === '进行中' || selectedHazard.status === '正在进行') ? 'bg-amber-500 text-white' : 'bg-slate-400 text-white'}`}>
                                        {(selectedHazard.status === 'Completed' || selectedHazard.status === 'Closed' || selectedHazard.status === '已完成' || selectedHazard.status === '已关闭') ? <CheckCircle2 size={24} /> : <Activity size={24} />}
                                    </div>
                                    <div>
                                        <div className="text-[10px] font-black text-slate-400 uppercase tracking-widest leading-none mb-1">{t('current_status', language)}</div>
                                        <div className="text-lg font-black text-slate-900 dark:text-white leading-none">{t((selectedHazard.status?.toLowerCase().replace(' ', '_') || 'not_started') as any, language)}</div>
                                    </div>
                                </div>
                                <div className="h-10 w-px bg-slate-200 dark:bg-slate-700 mx-2" />
                                <div className="flex-1 max-w-[200px]">
                                    <div className="text-[9px] font-black text-slate-400 uppercase tracking-widest mb-1.5 flex justify-between">
                                        <span>{t('task_progress', language)}</span>
                                        <span className="text-medtronic">
                                            {(selectedHazard.status === 'Completed' || selectedHazard.status === 'Closed' || selectedHazard.status === '已完成' || selectedHazard.status === '已关闭') ? '100%' :
                                                (selectedHazard.status === 'In Progress' || selectedHazard.status === '进行中' || selectedHazard.status === '正在进行') ? '50%' : '0%'}
                                        </span>
                                    </div>
                                    <div className="w-full bg-slate-200 dark:bg-slate-700 rounded-full h-2 overflow-hidden">
                                        <div
                                            className={`h-full transition-all duration-1000 ease-out ${(selectedHazard.status === 'Completed' || selectedHazard.status === 'Closed' || selectedHazard.status === '已完成' || selectedHazard.status === '已关闭') ? 'bg-emerald-500' :
                                                (selectedHazard.status === 'In Progress' || selectedHazard.status === '进行中' || selectedHazard.status === '正在进行') ? 'bg-amber-400' : 'bg-slate-400'
                                                }`}
                                            style={{
                                                width: (selectedHazard.status === 'Completed' || selectedHazard.status === 'Closed' || selectedHazard.status === '已完成' || selectedHazard.status === '已关闭') ? '100%' :
                                                    (selectedHazard.status === 'In Progress' || selectedHazard.status === '进行中' || selectedHazard.status === '正在进行') ? '50%' : '0%'
                                            }}
                                        />
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            )}

            {selectedIncident && (
                <div className="fixed inset-0 z-[120] flex items-center justify-center p-4 bg-slate-950/40 backdrop-blur-[6px] animate-in fade-in duration-300">
                    <div className="bg-white dark:bg-slate-900 w-full max-w-2xl rounded-[32px] shadow-2xl overflow-hidden border border-slate-200 dark:border-slate-800 animate-in zoom-in-95 duration-300 flex flex-col max-h-[95vh]">
                        {/* Red Accent Header for Incidents */}
                        <div className="h-1.5 w-full bg-red-600" />

                        <div className="p-8 overflow-y-auto custom-scrollbar">
                            <div className="flex justify-between items-start mb-8">
                                <div className="flex-1 min-w-0 pr-8">
                                    <div className="flex items-center gap-3 mb-3">
                                        <div className="px-2.5 py-0.5 bg-red-50 text-red-600 dark:bg-red-900/30 dark:text-red-400 text-[9px] font-black uppercase tracking-widest rounded-full">
                                            {selectedIncident.classification}
                                        </div>
                                        <div className="text-[10px] font-bold text-slate-400 font-mono">{t('incident_report', language)}</div>
                                    </div>
                                    <h2 className="text-2xl font-black text-slate-900 dark:text-white leading-tight tracking-tight">
                                        {selectedIncident.title}
                                    </h2>
                                </div>
                                <button
                                    onClick={() => setSelectedIncident(null)}
                                    className="p-2.5 bg-slate-50 dark:bg-slate-800 hover:bg-slate-100 dark:hover:bg-slate-700 rounded-2xl transition-all text-slate-400 hover:text-red-500"
                                >
                                    <X size={20} />
                                </button>
                            </div>

                            <div className="grid grid-cols-2 gap-4 mb-8">
                                <div className="p-4 rounded-2xl bg-slate-50 dark:bg-slate-800/50 border border-slate-100 dark:border-slate-800/50">
                                    <div className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-2 flex items-center gap-1.5">
                                        <Calendar size={10} /> {t('date_reported', language)}
                                    </div>
                                    <div className="text-[13px] font-bold text-slate-700 dark:text-slate-200">
                                        {new Date(selectedIncident.date).toLocaleDateString(undefined, { dateStyle: 'long' })}
                                    </div>
                                </div>
                                <div className="p-4 rounded-2xl bg-slate-50 dark:bg-slate-800/50 border border-slate-100 dark:border-slate-800/50">
                                    <div className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-2 flex items-center gap-1.5">
                                        <MapPin size={10} /> {t('location_area', language)}
                                    </div>
                                    <div className="text-[13px] font-bold text-slate-700 dark:text-slate-200 truncate">
                                        {selectedIncident.area}
                                    </div>
                                </div>
                            </div>

                            <div className="space-y-6">
                                <div className="space-y-3">
                                    <h4 className="text-[10px] font-black text-slate-400 uppercase tracking-widest flex items-center gap-2">
                                        <FileText size={12} /> {t('investigation_context', language)}
                                    </h4>
                                    <div className="p-6 bg-slate-50/50 dark:bg-slate-800/30 rounded-3xl border border-dashed border-slate-200 dark:border-slate-700 text-[14px] font-bold text-slate-800 dark:text-slate-200 leading-relaxed whitespace-pre-wrap">
                                        {selectedIncident.description || t('no_incident_description_provided', language)}
                                    </div>
                                </div>
                            </div>
                        </div>

                        <div className="px-8 py-6 bg-slate-50 dark:bg-slate-800/50 border-t border-slate-100 dark:border-slate-800">
                            <div className="flex items-center gap-8 mb-4">
                                <div className="flex-1">
                                    <div className="flex justify-between items-end mb-2">
                                        <div className="text-[10px] font-black text-slate-400 uppercase tracking-widest">{t('planner_progress', language)}</div>
                                        <div className="text-sm font-black text-emerald-500">{selectedIncident.progress}%</div>
                                    </div>
                                    <div className="h-2.5 bg-slate-200 dark:bg-slate-700 rounded-full overflow-hidden">
                                        <div
                                            className="h-full bg-emerald-500 transition-all duration-1000 ease-out"
                                            style={{ width: `${selectedIncident.progress}%` }}
                                        />
                                    </div>
                                </div>
                                <div className="flex gap-2">
                                    <button
                                        onClick={() => {
                                            toggleHideIncident(selectedIncident.title);
                                            setSelectedIncident(null);
                                        }}
                                        className="px-4 py-3 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 text-slate-500 hover:text-red-500 rounded-xl text-[10px] font-black uppercase tracking-widest transition-all shadow-sm"
                                        title="Hide this incident from the main dashboard list"
                                    >
                                        Hide Card
                                    </button>
                                    <button
                                        onClick={() => setSelectedIncident(null)}
                                        className="px-8 py-3 bg-slate-950 dark:bg-white dark:text-slate-950 text-white rounded-xl text-xs font-black uppercase tracking-widest hover:scale-105 active:scale-95 transition-all shadow-xl"
                                    >
                                        Dismiss
                                    </button>
                                </div>
                            </div>
                            <div className="flex items-center gap-1.5 text-[10px] font-bold text-slate-500 bg-slate-100 dark:bg-slate-800/80 px-3 py-1.5 rounded-lg w-fit">
                                <CheckCircle2 size={12} className={selectedIncident.progress === 100 ? "text-emerald-500" : "text-slate-300"} />
                                {selectedIncident.progress === 100 ? "Investigation Closed" : "Investigation in Progress"}
                            </div>
                        </div>
                    </div>
                </div>
            )}
        </StandardPageLayout>
    );
}
