'use client';

import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import {
    Calendar, RefreshCw, BarChart3, Clock, Table as TableIcon, Loader2,
    AlertCircle, ArrowRight, TrendingUp, TrendingDown, Factory, Activity, Download, ChevronLeft, ChevronRight,
    ChevronDown, ChevronUp, ChevronsDown, ChevronsUp, Zap, Package, ShieldCheck
} from 'lucide-react';
import { useUI } from '@/context/UIContext';
import StandardPageLayout, { PageTab } from '@/components/StandardPageLayout';
import { UnifiedDatePicker } from '@/components/ui/UnifiedDatePicker';

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
        variance?: number;
        variancePercent?: number;
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
            actualEH?: number;
            weeklyData: Array<{
                fiscalWeek: number;
                hours: number;
            }>;
        }>;
    }>;
    areaDistribution?: Array<{
        area: string;
        hours: number;
        percentage: number;
    }>;
    filterOptions?: {
        areas: string[];
        operations: string[];
        schedulers: string[];
    };
    heatmap?: Array<{
        op: string;
        date: string;
        val: number;
        area?: string;
    }>;
};

// --- Scheduler Display Mapping for Product Line Merge ---
// Maps display names to underlying raw ProductionScheduler values
const SCHEDULER_GROUPS: Record<string, { displayName: string; rawValues: string[] }> = {
    // 1303 Plant
    '1303_instruments': { displayName: '器械', rawValues: ['1303/CIN', '1303/OEM', '1303/#'] },
    '1303_implants': { displayName: '植入物', rawValues: ['1303/CIM'] },
    // 9997 Plant (康辉)
    '9997_active': { displayName: '有源', rawValues: ['9997/AP', '9997/AP1'] },
    '9997_instruments': { displayName: '器械', rawValues: ['9997/INS', '9997/#', '9997/M01', '9997/M02'] },
    '9997_sterile': { displayName: '无菌', rawValues: ['9997/STR'] },
    '9997_implants': { displayName: '植入物', rawValues: [] }, // Fallback for unmatched
};

// Fixed display order for product lines
const SCHEDULER_DISPLAY_ORDER = ['器械', '植入物', '无菌', '有源'];

// Helper to get display options for a plant
function getSchedulerDisplayOptions(plant: string, rawSchedulers: string[]): { key: string; displayName: string }[] {
    const result: { key: string; displayName: string }[] = [];
    const matchedRaw = new Set<string>();

    // Check each group for matching raw values (except fallback groups with empty rawValues)
    Object.entries(SCHEDULER_GROUPS).forEach(([key, group]) => {
        if (key.startsWith(plant + '_') && group.rawValues.length > 0) {
            const matching = rawSchedulers.filter(s => group.rawValues.includes(s));
            if (matching.length > 0) {
                result.push({ key, displayName: group.displayName });
                matching.forEach(s => matchedRaw.add(s));
            }
        }
    });

    // For 9997: Add unmatched to 植入物 (implants fallback)
    if (plant === '9997') {
        const unmatched = rawSchedulers.filter(s => !matchedRaw.has(s));
        if (unmatched.length > 0) {
            // Update the implants group rawValues dynamically
            SCHEDULER_GROUPS['9997_implants'].rawValues = unmatched;
            result.push({ key: '9997_implants', displayName: '植入物' });
        }
    } else {
        // For other plants: show unmatched as-is
        rawSchedulers.forEach(s => {
            if (!matchedRaw.has(s)) {
                result.push({ key: s, displayName: s });
            }
        });
    }

    // Sort by fixed display order: 器械 → 植入物 → 无菌 → 有源
    result.sort((a, b) => {
        const orderA = SCHEDULER_DISPLAY_ORDER.indexOf(a.displayName);
        const orderB = SCHEDULER_DISPLAY_ORDER.indexOf(b.displayName);
        // Items not in order list go to the end
        return (orderA === -1 ? 999 : orderA) - (orderB === -1 ? 999 : orderB);
    });

    return result;
}

// Helper to expand display key to raw values for API
function expandSchedulerSelection(selectedKeys: string[]): string[] {
    const rawValues: string[] = [];
    selectedKeys.forEach(key => {
        if (SCHEDULER_GROUPS[key]) {
            rawValues.push(...SCHEDULER_GROUPS[key].rawValues);
        } else {
            rawValues.push(key); // Fallback for unmatched
        }
    });
    return rawValues;
}


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
        opt && opt.toLowerCase().includes(searchTerm.toLowerCase())
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
export default function LaborEhPage({ theme, toggleTheme }: { theme?: 'light' | 'dark', toggleTheme?: () => void }) {
    const productionTabs: PageTab[] = [
        { label: '工时分析 (Labor EH)', href: '/production/labor-eh', icon: <Activity size={14} />, active: true },
        { label: '调试换型 (Changeover)', href: '/production/changeover', icon: <Zap size={14} /> },
        { label: '排程 (Schedule)', href: '/production/schedule', icon: <Calendar size={14} /> },
        { label: 'OEE 看板', href: '/production/oee', icon: <BarChart3 size={14} />, disabled: true },
        { label: '物料查询', href: '/production/material', icon: <Package size={14} />, disabled: true },
    ];

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
    const [heatmapExpandedAreas, setHeatmapExpandedAreas] = useState<Set<string>>(new Set());
    const [customRange, setCustomRange] = useState({ start: '', end: '' });
    const [selectedAreas, setSelectedAreas] = useState<string[]>([]);
    const [selectedProcesses, setSelectedProcesses] = useState<string[]>([]);
    const [selectedSchedulers, setSelectedSchedulers] = useState<string[]>([]);

    // Cache scheduler options per plant to prevent flashing unmapped values
    const [schedulerCache, setSchedulerCache] = useState<Record<string, string[]>>({});

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
        setSelectedProcesses([]);
        setSelectedSchedulers([]);
    };

    // --- Data State ---
    const [data, setData] = useState<LaborData | null>(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const { setResetHandler } = useUI();

    const handleResetFilters = useCallback(() => {
        setSelectedAreas([]);
        setSelectedProcesses([]);
        setSelectedSchedulers([]);
        setSelectedDate(null);
    }, []);

    useEffect(() => {
        setResetHandler(handleResetFilters);
    }, [setResetHandler, handleResetFilters]);

    // --- Records Pagination State ---
    const [records, setRecords] = useState<any[]>([]);
    const [page, setPage] = useState(1);
    const [hasMore, setHasMore] = useState(true);
    const [isRecordsLoading, setIsRecordsLoading] = useState(false);

    // --- Drill-down State (Dashboard Only) ---
    const [selectedDate, setSelectedDate] = useState<string | null>(null);
    const [dailyData, setDailyData] = useState<LaborData | null>(null);
    const [isDailyLoading, setIsDailyLoading] = useState(false);

    // --- Heatmap Dimension Toggle ---
    const [heatmapDimension, setHeatmapDimension] = useState<'operation' | 'area'>('operation');

    // --- Client-Side Caching for Instant Filter Response ---
    const [rawDataCache, setRawDataCache] = useState<LaborData | null>(null);

    // Apply area/process filters client-side for instant response
    const filteredData = useMemo(() => {
        if (!rawDataCache) return null;

        // No filters applied - return raw data as-is
        if (selectedAreas.length === 0 && selectedProcesses.length === 0 && selectedSchedulers.length === 0) {
            return rawDataCache;
        }

        // Build lookup for valid operations based on selected areas
        const validOps = new Set<string>();
        if (selectedAreas.length > 0) {
            (rawDataCache.areaOperationDetail || []).forEach(area => {
                if (selectedAreas.includes(area.area)) {
                    (area.operations || []).forEach(op => validOps.add(op.operationName));
                }
            });
        }

        const processSet = new Set(selectedProcesses);
        const schedulerSet = new Set(selectedSchedulers);

        // Filter areaOperationDetail
        const filteredAreaOps = (rawDataCache.areaOperationDetail || [])
            .filter(a => selectedAreas.length === 0 || selectedAreas.includes(a.area))
            .map(a => ({
                ...a,
                operations: (a.operations || []).filter(op =>
                    (selectedProcesses.length === 0 || processSet.has(op.operationName))
                )
            }))
            .filter(a => a.operations.length > 0);

        // Filter heatmap
        const filteredHeatmap = (rawDataCache.heatmap || []).filter(h => {
            const opMatch = selectedProcesses.length === 0 || processSet.has(h.op);
            const areaMatch = selectedAreas.length === 0 || validOps.has(h.op);
            return opMatch && areaMatch;
        });

        // Filter areaDistribution
        const filteredAreaDist = (rawDataCache.areaDistribution || [])
            .filter(a => selectedAreas.length === 0 || selectedAreas.includes(a.area));

        // Recalculate summary from filtered area data
        let filteredActualEH = 0;
        filteredAreaOps.forEach(a => {
            a.operations.forEach(op => { filteredActualEH += op.actualEH || 0; });
        });

        return {
            ...rawDataCache,
            summary: {
                ...rawDataCache.summary,
                actualEH: filteredActualEH,
                variance: filteredActualEH - (rawDataCache.summary?.targetEH || 0),
                variancePercent: rawDataCache.summary?.targetEH ? ((filteredActualEH - rawDataCache.summary.targetEH) / rawDataCache.summary.targetEH) * 100 : 0
            },
            areaOperationDetail: filteredAreaOps,
            areaDistribution: filteredAreaDist,
            heatmap: filteredHeatmap
        } as LaborData;
    }, [rawDataCache, selectedAreas, selectedProcesses, selectedSchedulers]);

    // --- Drill-down Logic ---
    const handleBarClick = (dateStr: string) => {
        if (selectedDate === dateStr) {
            setSelectedDate(null); // Deselect
        } else {
            setSelectedDate(dateStr); // Select
        }
    };

    // Fetch Daily Data (Dashboard Mode Only)
    useEffect(() => {
        if (!selectedDate) {
            setDailyData(null);
            return;
        }

        async function fetchDailyData() {
            setIsDailyLoading(true);
            try {
                let url = `/api/production/labor-eh?mode=dashboard&granularity=custom&startDate=${selectedDate}&endDate=${selectedDate}&plant=${selectedPlant}`;
                // Pass current filters to ensure daily view respects them
                if (selectedAreas.length > 0) url += `&areas=${selectedAreas.join(',')}`;
                if (selectedProcesses.length > 0) url += `&processes=${selectedProcesses.join(',')}`;
                if (selectedSchedulers.length > 0) url += `&productSchedulers=${selectedSchedulers.join(',')}`;

                const res = await fetch(url);
                const json = await res.json();
                if (!res.ok) throw new Error(json.error);
                setDailyData(json);
            } catch (err) {
                console.error("Daily fetch error", err);
            } finally {
                setIsDailyLoading(false);
            }
        }
        fetchDailyData();
    }, [selectedDate, selectedPlant, selectedAreas, selectedProcesses, selectedSchedulers]);

    // Derived Data for Dashboard (Toggle between Daily and Period)
    // Note: details are handled separately by 'records' state now
    const activeData = selectedDate ? (dailyData || { ...data, details: [], anomalies: [], areaOperationDetail: [] }) : data;


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
                    handleMonthChange(json.currentFiscalInfo.fiscal_month);
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

    // 2. Fetch Full Data (Dashboard Mode Only) - NO area/process filters sent to server
    // Using ref to skip debounce on initial load for faster first render
    const isFirstLoad = useRef(true);
    // Cache for API responses to support instant switching back
    const apiResponseCache = useRef<Record<string, any>>({});

    useEffect(() => {
        if (granularity === 'week' && selectedWeeks.length === 0) return;
        if (granularity === 'month' && !selectedMonth) return;
        if (granularity === 'custom' && (!customRange.start || !customRange.end)) return;

        async function fetchLaborEHData() {
            // Generate cache key
            const sortedWeeks = [...selectedWeeks].sort().join(',');
            const sortedSchedulers = [...selectedSchedulers].sort().join(',');
            const cacheKey = `${granularity}_${selectedYear}_${selectedPlant}_${selectedMonth}_${sortedWeeks}_${customRange.start}_${customRange.end}_${sortedSchedulers}`;

            if (apiResponseCache.current[cacheKey]) {
                const cached = apiResponseCache.current[cacheKey];
                setRawDataCache(cached);
                setData(cached);
                setIsLoading(false);
                return;
            }

            setIsLoading(true);
            setError(null);
            try {
                let url = `/api/production/labor-eh?mode=dashboard&granularity=${granularity}&year=${selectedYear}&plant=${selectedPlant}`;
                if (granularity === 'week') url += `&week=${selectedWeeks.join(',')}`;
                if (granularity === 'month') url += `&month=${selectedMonth}`;
                if (granularity === 'custom') url += `&startDate=${customRange.start}&endDate=${customRange.end}`;

                // Re-enable scheduler filter on server side because we can't filter it client-side (grouping issues)
                if (selectedSchedulers.length > 0) url += `&productSchedulers=${selectedSchedulers.join(',')}`;

                const res = await fetch(url);
                const json = await res.json();

                if (!res.ok) throw new Error(json.error || 'Failed to fetch SQL data');

                // Save to cache
                apiResponseCache.current[cacheKey] = json;

                setRawDataCache(json);
                setData(json);
                // Cache scheduler options for this plant
                if (json.filterOptions?.schedulers) {
                    setSchedulerCache(prev => ({ ...prev, [selectedPlant]: json.filterOptions.schedulers }));
                }
            } catch (err: any) {
                setError(err.message);
            } finally {
                setIsLoading(false);
            }
        }

        // Skip debounce on first load for immediate response
        if (isFirstLoad.current) {
            isFirstLoad.current = false;
            fetchLaborEHData();
        } else {
            // Debounce subsequent loads (filter changes)
            const debounceTimer = setTimeout(fetchLaborEHData, 300);
            return () => clearTimeout(debounceTimer);
        }
    }, [selectedYear, selectedMonth, selectedWeeks, selectedPlant, granularity, customRange, selectedSchedulers]);

    // 2b. Apply client-side filters instantly when area/process/scheduler changes
    useEffect(() => {
        if (filteredData) {
            setData(filteredData);
            // Default to Collapsed View
            setExpandedAreas(new Set());
            setHeatmapExpandedAreas(new Set());
        }
    }, [filteredData]);

    // 3. Fetch Records (Paginated)
    const fetchRecords = async (pageNum: number, reset: boolean = false) => {
        // Guard: Logic to determine effective Date Range
        // Must mirror the logic of the two effects above to ensure consistency
        if (granularity === 'week' && selectedWeeks.length === 0) return;
        if (granularity === 'month' && !selectedMonth) return;
        if (granularity === 'custom' && (!customRange.start || !customRange.end)) return;

        setIsRecordsLoading(true);
        try {
            const params = new URLSearchParams();
            params.set('mode', 'records');
            params.set('page', pageNum.toString());
            params.set('pageSize', '50');
            params.set('plant', selectedPlant);
            params.set('year', selectedYear);

            // Filters
            if (selectedAreas.length) params.set('areas', selectedAreas.join(','));
            if (selectedProcesses.length) params.set('processes', selectedProcesses.join(','));
            if (selectedSchedulers.length) params.set('productSchedulers', selectedSchedulers.join(','));

            if (selectedDate) {
                // Drill-down context
                params.set('granularity', 'custom');
                params.set('startDate', selectedDate);
                params.set('endDate', selectedDate);
            } else {
                // Period context
                params.set('granularity', granularity);
                if (granularity === 'week') params.set('week', selectedWeeks.join(','));
                if (granularity === 'month') params.set('month', selectedMonth);
                if (granularity === 'custom') {
                    params.set('startDate', customRange.start);
                    params.set('endDate', customRange.end);
                }
            }

            const res = await fetch(`/api/production/labor-eh?${params.toString()}`);
            const json = await res.json();

            if (reset) {
                setRecords(json.details || []);
            } else {
                setRecords(prev => [...prev, ...(json.details || [])]);
            }
            setHasMore(json.hasMore);
            setPage(pageNum);
        } catch (e) {
            console.error("Records fetch error", e);
        } finally {
            setIsRecordsLoading(false);
        }
    };

    // Effect: Trigger initial records fetch when ANY filter changes
    const isFirstRecordsLoad = useRef(true);

    useEffect(() => {
        if (granularity === 'week' && selectedWeeks.length === 0) return;
        if (granularity === 'month' && !selectedMonth) return;
        if (granularity === 'custom' && (!customRange.start || !customRange.end)) return;

        function doFetch() {
            setPage(1);
            setHasMore(true);
            setRecords([]);
            fetchRecords(1, true);
        }

        // Skip debounce on first load
        if (isFirstRecordsLoad.current) {
            isFirstRecordsLoad.current = false;
            doFetch();
        } else {
            const timer = setTimeout(doFetch, 350);
            return () => clearTimeout(timer);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [selectedYear, selectedMonth, selectedWeeks, selectedPlant, granularity, customRange, selectedAreas, selectedProcesses, selectedSchedulers, selectedDate]);


    const handleLoadMore = () => {
        if (!isRecordsLoading && hasMore) {
            fetchRecords(page + 1, false);
        }
    };

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
        if (!records || records.length === 0) return;
        const headers = ["PostingDate", "OrderNumber", "Material", "actualEH", "WorkCenter", "Plant"];
        const csvContent = [
            headers.join(","),
            ...records.map((row: any) => [
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

    const filters = (
        <div className="space-y-6">
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

            {/* 2. Time Selection */}
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
            {(() => {
                const rawSchedulers = schedulerCache[selectedPlant] || data?.filterOptions?.schedulers || [];
                const displayOptions = getSchedulerDisplayOptions(selectedPlant, rawSchedulers);
                return (
                    <section className="mb-3">
                        <h4 className="text-[10px] font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider mb-1">产品线 (Product Line)</h4>
                        <div className="flex flex-wrap gap-1.5">
                            {displayOptions.length === 0 ? (
                                <span className="text-xs text-slate-400 italic">无可用产品线</span>
                            ) : displayOptions.map(opt => (
                                <button
                                    key={opt.key}
                                    onClick={(e) => {
                                        const isMulti = e.ctrlKey || e.metaKey;
                                        const groupRawValues = SCHEDULER_GROUPS[opt.key]?.rawValues || [opt.key];
                                        const wasSelected = selectedSchedulers.some(s => groupRawValues.includes(s));
                                        if (isMulti) {
                                            if (wasSelected) {
                                                const toRemove = new Set(groupRawValues);
                                                setSelectedSchedulers(prev => prev.filter(s => !toRemove.has(s)));
                                            } else {
                                                setSelectedSchedulers(prev => [...prev, ...groupRawValues.filter(v => !prev.includes(v))]);
                                            }
                                        } else {
                                            if (wasSelected) {
                                                const isOnlySelected = selectedSchedulers.length === groupRawValues.length && groupRawValues.every(v => selectedSchedulers.includes(v));
                                                if (isOnlySelected) setSelectedSchedulers([]);
                                                else setSelectedSchedulers(groupRawValues);
                                            } else {
                                                setSelectedSchedulers(groupRawValues);
                                            }
                                        }
                                    }}
                                    className={`px-2 py-1 text-[10px] rounded-md border transition-all font-medium ${selectedSchedulers.some(s => SCHEDULER_GROUPS[opt.key]?.rawValues.includes(s) || s === opt.key)
                                        ? 'bg-medtronic text-white border-medtronic shadow-sm'
                                        : 'bg-white dark:bg-slate-800 border-slate-200 dark:border-slate-700 text-slate-600 dark:text-slate-300 hover:border-medtronic hover:text-medtronic'
                                        }`}
                                >
                                    {opt.displayName}
                                </button>
                            ))}
                        </div>
                    </section>
                );
            })()}

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
        </div>
    );

    return (
        <StandardPageLayout
            theme={theme}
            toggleTheme={toggleTheme}
            title="Labor Efficiency"
            description="Track workforce productivity and labor utilization rates."
            icon={<Activity size={24} />}
            tabs={productionTabs}
            filters={filters}
        >
            {/* Match EHS structure: Outer wrapper + separated KPI section + responsive main block */}
            <div className="flex-1 flex flex-col gap-4 max-w-[1700px] mx-auto w-full min-h-0">
                {isLoading && (
                    <div className="absolute inset-0 flex items-center justify-center bg-white/50 dark:bg-slate-900/50 backdrop-blur-sm z-50 rounded-2xl transition-all duration-500">
                        <div className="flex flex-col items-center gap-4">
                            <Loader2 className="animate-spin text-amber-500" size={40} />
                            <p className="text-sm font-bold text-slate-500 animate-pulse">正在从 MES/v_metrics 计算工时效率...</p>
                        </div>
                    </div>
                )}

                {/* KPI Tiles - Separated as shrink-0 section */}
                {(() => {
                    // Targets are only available at Plant Level. If filtering, hide targets/variance to avoid confusion.
                    const hasFilters = selectedAreas.length > 0 || selectedProcesses.length > 0 || selectedSchedulers.length > 0;
                    const showTargets = !hasFilters;

                    return (
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 shrink-0">
                            <KpiTile
                                title="Average Hours"
                                value={data?.summary?.actualAvgEH || 0}
                                target={showTargets ? data?.summary?.targetAvgEH : undefined}
                                ratio={showTargets && data?.summary && data.summary.targetAvgEH > 0 ? (data.summary.actualAvgEH / data.summary.targetAvgEH) * 100 : undefined}
                                diff={showTargets && data ? data.summary.actualAvgEH - data.summary.targetAvgEH : undefined}
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
                                target={showTargets ? data?.summary?.targetEH : undefined}
                                ratio={showTargets && data?.summary && data.summary.targetEH > 0 ? (data.summary.actualEH / data.summary.targetEH) * 100 : undefined}
                                // Total Diff = Actual - Target
                                diff={showTargets && data?.summary ? data.summary.actualEH - data.summary.targetEH : undefined}
                                unit="H"
                                color="amber"
                                icon={<Activity size={24} />}
                            />
                            <KpiTile
                                title="YTD Actual EH"
                                value={data?.summary?.ytdActualEH || 0}
                                target={showTargets ? data?.summary?.ytdTargetEH : undefined}
                                ratio={showTargets && data?.summary && data.summary.ytdTargetEH > 0 ? (data.summary.ytdActualEH / data.summary.ytdTargetEH) * 100 : undefined}
                                // YTD Diff = Actual - (AnnualTarget / 300 * ActualDays) APPROXIMATION
                                diff={showTargets && data?.summary ? data.summary.ytdActualEH - ((data.summary.ytdTargetEH / 300) * data.summary.actualDays) : undefined}
                                unit="H"
                                color="violet"
                                icon={<Factory size={24} />}
                            />
                        </div>
                    );
                })()}

                {/* Main Dashboard Block - Trend Chart + Diagnostics with responsive height */}
                <div className="flex-1 flex flex-col gap-4 min-h-[600px] lg:min-h-[calc(100vh-280px)]">
                    {/* Main Trend Chart - Compressed to Half Height */}
                    <div className="ios-widget bg-white dark:bg-slate-900 p-6 flex flex-col shadow-xl shadow-slate-200/50 dark:shadow-none border border-slate-200/50 dark:border-slate-800 shrink-0">
                        <div className="flex justify-between items-center mb-6">
                            <div>
                                <h3 className="text-sm font-black text-slate-800 dark:text-slate-100 uppercase tracking-widest">Daily & Trend EH Analysis</h3>
                                <p className="text-[10px] text-slate-400 font-bold uppercase tracking-[0.2em] mt-1">工时产出对标分布图 ({granularity})</p>
                            </div>
                            <div className="flex gap-6 text-[10px] font-black uppercase tracking-tighter">
                                {selectedDate && (
                                    <span className="flex items-center gap-2 text-medtronic animate-pulse">
                                        <div className="w-3 h-3 rounded-full bg-medtronic" />
                                        FILTERED: {new Date(selectedDate).toLocaleDateString()}
                                    </span>
                                )}
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

                                        const isActive = selectedDate === d.SDate;
                                        const isDimmed = selectedDate && !isActive;

                                        return (
                                            <div
                                                key={i}
                                                onClick={() => d.SDate && handleBarClick(d.SDate)}
                                                className={`flex-1 flex flex-col items-center gap-1 group relative h-full cursor-pointer transition-all duration-300 ${isDimmed ? 'opacity-30 grayscale' : 'opacity-100 scale-100'}`}
                                            >
                                                <div className="w-full bg-transparent overflow-visible flex-1 relative">
                                                    {/* Target Bar (Ghost) */}
                                                    <div
                                                        className="absolute bottom-0 w-full bg-slate-300/20 dark:bg-slate-700/10 border-2 border-slate-400/30 border-dashed rounded-t-lg transition-all z-0"
                                                        style={{ height: `${targetPct}%` }}
                                                    />
                                                    {/* Actual Bar */}
                                                    <div
                                                        className={`absolute bottom-0 w-full rounded-t-lg transition-all duration-500 group-hover:brightness-105 z-10 shadow-sm ${isAchievement
                                                            ? 'bg-gradient-to-t from-blue-600 to-blue-400'
                                                            : 'bg-gradient-to-t from-slate-500 to-slate-400'
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
                        <div className="flex-1 overflow-hidden min-h-0">
                            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 h-full">
                                {/* Area/Operation Distribution Table */}
                                <div className="bg-slate-50 dark:bg-slate-900/50 p-5 rounded-2xl flex flex-col h-[500px] lg:h-full overflow-hidden">
                                    <div className="flex justify-between items-center mb-4 shrink-0">
                                        <h3 className="text-xs font-black text-slate-400 uppercase tracking-widest">区域工序分布 (Area Distribution)</h3>
                                        <button
                                            onClick={() => {
                                                const allAreas = activeData?.areaOperationDetail?.map((a: any) => a.area) || [];
                                                if (expandedAreas.size > 0) {
                                                    setExpandedAreas(new Set());
                                                } else {
                                                    setExpandedAreas(new Set(allAreas));
                                                }
                                            }}
                                            className="flex items-center gap-1 px-2 py-1 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded text-slate-500 hover:text-medtronic hover:border-medtronic transition-colors"
                                            title={expandedAreas.size > 0 ? "Collapse All" : "Expand All"}
                                        >
                                            {expandedAreas.size > 0 ? <ChevronsUp size={14} /> : <ChevronsDown size={14} />}
                                        </button>
                                    </div>

                                    {(activeData?.areaOperationDetail?.length ?? 0) > 0 ? (
                                        (() => {
                                            const weeks = new Set<number>();
                                            activeData?.areaOperationDetail?.forEach((area: any) => {
                                                area?.operations?.forEach((op: any) => {
                                                    op?.weeklyData?.forEach((w: any) => weeks.add(w.fiscalWeek));
                                                });
                                            });
                                            const sortedWeeks = Array.from(weeks).sort((a, b) => (a || 0) - (b || 0));
                                            const displayWeeks = [...sortedWeeks, ...Array(Math.max(0, 6 - sortedWeeks.length)).fill(null)];

                                            return (
                                                <div className="overflow-x-auto overflow-y-auto relative rounded-xl border border-slate-200 dark:border-slate-700 flex-1">
                                                    <table className="w-full text-[10px] border-separate border-spacing-0">
                                                        <thead className="sticky top-0 z-20 shadow-sm">
                                                            <tr className="bg-slate-50 dark:bg-slate-900/95">
                                                                <th className="sticky left-0 z-30 bg-slate-50 dark:bg-slate-900/95 text-left py-3 px-3 font-black text-slate-400 uppercase border-b border-slate-200 dark:border-slate-700 w-[230px] min-w-[230px] shadow-r-lg">区域 / 工序 (Area / Op)</th>
                                                                <th className="text-right py-3 px-3 font-black text-slate-400 uppercase border-b border-slate-200 dark:border-slate-700 w-[80px] min-w-[80px]">昨天(h)</th>
                                                                {displayWeeks.map((week, idx) => (
                                                                    <th key={idx} className="text-right py-3 px-3 font-black text-slate-400 uppercase whitespace-nowrap border-b border-slate-200 dark:border-slate-700 w-[80px] min-w-[80px]">
                                                                        {week ? `W${week}` : ''}
                                                                    </th>
                                                                ))}
                                                            </tr>
                                                        </thead>
                                                        <tbody>
                                                            {activeData?.areaOperationDetail?.map((areaData: any) => {
                                                                const isExpanded = expandedAreas.has(areaData.area);
                                                                const yesterdaySum = areaData?.operations?.reduce((acc: number, item: any) => acc + (item.yesterday || 0), 0) || 0;
                                                                const weeklySums = displayWeeks.map(week => ({
                                                                    week,
                                                                    hours: week ? areaData?.operations?.reduce((acc: number, item: any) => {
                                                                        const w = item?.weeklyData?.find((wd: any) => wd.fiscalWeek === week);
                                                                        return acc + (w ? w.hours : 0);
                                                                    }, 0) : 0
                                                                }));

                                                                return (
                                                                    <React.Fragment key={areaData.area}>
                                                                        <tr
                                                                            onClick={() => {
                                                                                const newSet = new Set(expandedAreas);
                                                                                if (newSet.has(areaData.area)) newSet.delete(areaData.area);
                                                                                else newSet.add(areaData.area);
                                                                                setExpandedAreas(newSet);
                                                                            }}
                                                                            className="group hover:bg-slate-100/80 dark:hover:bg-slate-800/50 cursor-pointer transition-colors"
                                                                        >
                                                                            <td className="sticky left-0 z-10 bg-white/95 dark:bg-slate-900/95 group-hover:bg-slate-100/95 dark:group-hover:bg-slate-800/95 py-2 px-3 border-b border-slate-100 dark:border-slate-800 align-middle shadow-r-lg">
                                                                                <div className="flex items-center gap-2">
                                                                                    <div className="flex justify-center w-4 text-medtronic shrink-0">
                                                                                        {isExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                                                                                    </div>
                                                                                    <div className="font-black text-slate-700 dark:text-slate-200 text-xs truncate max-w-[90px]" title={areaData.area}>{areaData.area}</div>
                                                                                    <div className="flex items-center gap-1 text-[10px] text-slate-500 bg-slate-100 dark:bg-slate-800 px-1.5 py-0.5 rounded-full ml-auto shrink-0">
                                                                                        <span className="font-bold">{Math.round(areaData.totalHours)}h</span>
                                                                                        <span className="text-slate-300">|</span>
                                                                                        <span className="font-bold text-medtronic">{areaData.percentage}%</span>
                                                                                    </div>
                                                                                </div>
                                                                            </td>
                                                                            <td className="text-right py-2 px-3 border-b border-slate-100 dark:border-slate-800 font-black text-slate-700 dark:text-slate-300 align-middle bg-slate-50/30 dark:bg-slate-900/30">
                                                                                {yesterdaySum > 0 ? yesterdaySum.toFixed(1) : '—'}
                                                                            </td>
                                                                            {weeklySums.map((w, idx) => (
                                                                                <td key={idx} className="text-right py-2 px-3 border-b border-slate-100 dark:border-slate-800 font-bold text-slate-600 dark:text-slate-400 align-middle bg-slate-50/30 dark:bg-slate-900/30 whitespace-nowrap">
                                                                                    {w.week && w.hours > 0 ? w.hours.toFixed(1) : ''}
                                                                                </td>
                                                                            ))}
                                                                        </tr>
                                                                        {isExpanded && areaData?.operations?.map((op: any) => (
                                                                            <tr key={`${areaData.area}-${op.operationName}`} className="hover:bg-slate-50/50 dark:hover:bg-slate-800/30 transition-colors">
                                                                                <td className="sticky left-0 bg-slate-50/80 dark:bg-slate-900/80 py-2 px-3 border-b border-slate-100 dark:border-slate-800 font-semibold text-slate-600 dark:text-slate-400 text-[10px] pl-10 shadow-r-lg truncate max-w-[230px]" title={op.operationName}>
                                                                                    {op.operationName}
                                                                                </td>
                                                                                <td className="text-right py-2 px-3 border-b border-slate-100 dark:border-slate-800 font-medium text-slate-600 dark:text-slate-400">
                                                                                    {op.yesterday > 0 ? op.yesterday.toFixed(1) : '—'}
                                                                                </td>
                                                                                {displayWeeks.map((week: any, idx: number) => {
                                                                                    const weekData = op?.weeklyData?.find((wd: any) => wd.fiscalWeek === week);
                                                                                    return (
                                                                                        <td key={idx} className="text-right py-2 px-3 border-b border-slate-100 dark:border-slate-800 font-medium text-slate-500 dark:text-slate-500 whitespace-nowrap">
                                                                                            {weekData && weekData.hours > 0 ? weekData.hours.toFixed(1) : '—'}
                                                                                        </td>
                                                                                    );
                                                                                })}
                                                                            </tr>
                                                                        ))}
                                                                    </React.Fragment>
                                                                );
                                                            })}
                                                        </tbody>
                                                    </table>
                                                </div>
                                            );
                                        })()
                                    ) : (
                                        <div className="flex-1 flex items-center justify-center text-xs text-slate-400 italic">No area distribution data available</div>
                                    )}
                                </div>


                                {/* Heatmap: Last 15 Days Operation Output */}
                                <div className="bg-slate-50 dark:bg-slate-900/50 p-5 rounded-2xl lg:col-span-1 flex flex-col h-[500px] lg:h-full overflow-hidden">
                                    {/* Header */}
                                    <div className="flex items-center justify-between mb-4 shrink-0">
                                        <h3 className="text-xs font-black text-slate-400 uppercase tracking-widest">
                                            区域/工序产出热力图 (Last 15 Days)
                                        </h3>
                                        <div className="flex items-center gap-2">
                                            <button
                                                onClick={() => {
                                                    const allHeatmapAreas = Array.from(new Set((data?.heatmap || []).map((d: any) => d.area || 'Unknown')));
                                                    if (heatmapExpandedAreas.size > 0) {
                                                        setHeatmapExpandedAreas(new Set());
                                                    } else {
                                                        setHeatmapExpandedAreas(new Set(allHeatmapAreas));
                                                    }
                                                }}
                                                className="flex items-center gap-1 px-2 py-1 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded text-slate-500 hover:text-medtronic hover:border-medtronic transition-colors"
                                                title={heatmapExpandedAreas.size > 0 ? "Collapse All" : "Expand All"}
                                            >
                                                {heatmapExpandedAreas.size > 0 ? <ChevronsUp size={14} /> : <ChevronsDown size={14} />}
                                            </button>
                                            <div className="text-[10px] text-slate-400">
                                                <span className="bg-slate-200 dark:bg-slate-700 px-2 py-0.5 rounded">Grouped by Area</span>
                                            </div>
                                        </div>
                                    </div>

                                    {data?.heatmap && data.heatmap.length > 0 ? (
                                        <div className="overflow-x-auto overflow-y-auto relative rounded-xl border border-slate-200 dark:border-slate-700 flex-1 bg-white dark:bg-slate-900/50 p-3">
                                            {(() => {
                                                // Prepare Heatmap Data
                                                const rawData = data.heatmap || [];
                                                const uniqueDates = Array.from(new Set(rawData.map((d: any) => new Date(d.date).toISOString().split('T')[0]))).sort();
                                                const displayDates = uniqueDates.slice(-15);

                                                // Month abbreviations for header
                                                const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

                                                // Build month spans for merged header
                                                const monthSpans: { month: string; count: number }[] = [];
                                                let currentMonth = '';
                                                displayDates.forEach(date => {
                                                    const m = monthNames[new Date(date).getMonth()];
                                                    if (m === currentMonth) {
                                                        monthSpans[monthSpans.length - 1].count++;
                                                    } else {
                                                        monthSpans.push({ month: m, count: 1 });
                                                        currentMonth = m;
                                                    }
                                                });

                                                // Grouping Logic: Area -> Ops
                                                const groupedData: Record<string, { total: number, dates: Record<string, number>, ops: Record<string, { total: number, dates: Record<string, number> }> }> = {};

                                                rawData.forEach((d: any) => {
                                                    const area = d.area || 'Unknown';
                                                    const dateKey = new Date(d.date).toISOString().split('T')[0];
                                                    if (!displayDates.includes(dateKey)) return;

                                                    if (!groupedData[area]) groupedData[area] = { total: 0, dates: {}, ops: {} };

                                                    // Area Totals
                                                    groupedData[area].total += d.val;
                                                    groupedData[area].dates[dateKey] = (groupedData[area].dates[dateKey] || 0) + d.val;

                                                    // Op Totals
                                                    if (!groupedData[area].ops[d.op]) groupedData[area].ops[d.op] = { total: 0, dates: {} };
                                                    groupedData[area].ops[d.op].total += d.val;
                                                    groupedData[area].ops[d.op].dates[dateKey] = (groupedData[area].ops[d.op].dates[dateKey] || 0) + d.val;
                                                });

                                                const sortedAreas = Object.keys(groupedData).sort((a, b) => groupedData[b].total - groupedData[a].total);

                                                // Find max value for color scale (global max or local?) Global max of daily values to keep scale consistent
                                                // We use Op level max for ops, and Area level max for areas?
                                                // Actually, if Area row is summary, its values are larger. Color scale should probably adapt?
                                                // But usually heatmap uses ONE scale.
                                                // If Area values are sums, they will be very dark.
                                                // Let's use separate scales or log scale?
                                                // For simplicity, let's use the OP level max for rendering ops, and Area level max for areas (or just relative).

                                                const allOpValues = Object.values(groupedData).flatMap(g => Object.values(g.ops).flatMap(o => Object.values(o.dates)));
                                                const maxOpVal = Math.max(...allOpValues, 10);

                                                const allAreaValues = Object.values(groupedData).flatMap(g => Object.values(g.dates));
                                                const maxAreaVal = Math.max(...allAreaValues, 10);

                                                const renderCell = (val: number, max: number) => {
                                                    const intensity = Math.min(1, val / max);
                                                    // Blue scale
                                                    return `rgba(59, 130, 246, ${Math.max(0.1, intensity)})`; // blue-500 base
                                                };

                                                return (
                                                    <div className="min-w-[400px]">
                                                        {/* Month Row (Merged) */}
                                                        <div className="flex mb-0">
                                                            <div className="w-[180px] shrink-0" /> {/* Label Placeholder */}
                                                            {monthSpans.map((span, idx) => (
                                                                <div
                                                                    key={idx}
                                                                    className="text-[9px] font-bold text-slate-500 dark:text-slate-400 text-center border-b border-slate-200 dark:border-slate-700"
                                                                    style={{ flex: span.count, minWidth: span.count * 20 }}
                                                                >
                                                                    {span.month}
                                                                </div>
                                                            ))}
                                                        </div>

                                                        {/* Day Row (dd only) */}
                                                        <div className="flex mb-1">
                                                            <div className="w-[180px] shrink-0 text-[10px] font-black text-slate-400 uppercase flex items-end pb-1 pl-3">区域 / 工序 (Area / Op)</div>
                                                            {displayDates.map(date => (
                                                                <div key={date} className="flex-1 min-w-[20px] text-[9px] font-semibold text-slate-400 text-center">
                                                                    {new Date(date).getDate()}
                                                                </div>
                                                            ))}
                                                        </div>

                                                        {/* Rows */}
                                                        <div className="space-y-0.5">
                                                            {sortedAreas.map(areaKey => {
                                                                const areaData = groupedData[areaKey];
                                                                const isExpanded = heatmapExpandedAreas.has(areaKey);

                                                                const sortedOps = Object.keys(areaData.ops).sort((a, b) => areaData.ops[b].total - areaData.ops[a].total);

                                                                return (
                                                                    <div key={areaKey} className="group">
                                                                        {/* Area Summary Row */}
                                                                        <div
                                                                            onClick={() => {
                                                                                const newSet = new Set(heatmapExpandedAreas);
                                                                                if (newSet.has(areaKey)) newSet.delete(areaKey);
                                                                                else newSet.add(areaKey);
                                                                                setHeatmapExpandedAreas(newSet);
                                                                            }}
                                                                            className="flex items-center hover:bg-slate-100 dark:hover:bg-slate-800 cursor-pointer transition-colors py-1 border-b border-slate-100 dark:border-slate-800"
                                                                        >
                                                                            <div className="w-[180px] shrink-0 text-xs font-black text-slate-700 dark:text-slate-200 truncate pr-2 flex items-center gap-2 pl-3">
                                                                                <div className="flex justify-center w-4 text-medtronic shrink-0">
                                                                                    {isExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                                                                                </div>
                                                                                {areaKey}
                                                                            </div>
                                                                            {displayDates.map(date => {
                                                                                const val = areaData.dates[date] || 0;
                                                                                return (
                                                                                    <div
                                                                                        key={date}
                                                                                        className="flex-1 min-w-[20px] h-6 mx-px rounded-sm flex items-center justify-center text-[9px] font-bold text-slate-700 dark:text-white transition-all relative group/cell"
                                                                                        style={{ backgroundColor: val > 0 ? renderCell(val, maxAreaVal) : 'transparent' }}
                                                                                    >
                                                                                        {val > 0 && <span className="drop-shadow-sm">{Math.round(val)}</span>}
                                                                                        {/* Tooltip */}
                                                                                        {val > 0 && (
                                                                                            <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1 opacity-0 group-hover/cell:opacity-100 transition-opacity bg-black text-white text-[9px] px-1.5 py-0.5 rounded pointer-events-none whitespace-nowrap z-50">
                                                                                                {areaKey}: {Math.round(val)}h
                                                                                            </div>
                                                                                        )}
                                                                                    </div>
                                                                                );
                                                                            })}
                                                                        </div>

                                                                        {/* Op Rows (Expanded) */}
                                                                        {isExpanded && sortedOps.map(opKey => {
                                                                            const opData = areaData.ops[opKey];
                                                                            return (
                                                                                <div key={opKey} className="flex items-center hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors py-1 border-b border-slate-50 dark:border-slate-800/50">
                                                                                    <div className="w-[180px] shrink-0 text-[10px] font-semibold text-slate-600 dark:text-slate-400 truncate pl-9 pr-2">
                                                                                        {opKey}
                                                                                    </div>
                                                                                    {displayDates.map(date => {
                                                                                        const val = opData.dates[date] || 0;
                                                                                        return (
                                                                                            <div
                                                                                                key={date}
                                                                                                className="flex-1 min-w-[20px] h-6 mx-px rounded-sm flex items-center justify-center text-[8px] text-white/90 transition-all relative group/cell"
                                                                                                style={{ backgroundColor: val > 0 ? renderCell(val, maxOpVal) : 'rgba(241, 245, 249, 0.4)' }}
                                                                                            >
                                                                                                {/* Tooltip */}
                                                                                                {val > 0 && (
                                                                                                    <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1 opacity-0 group-hover/cell:opacity-100 transition-opacity bg-black text-white text-[9px] px-1.5 py-0.5 rounded pointer-events-none whitespace-nowrap z-50">
                                                                                                        {opKey}: {Math.round(val)}h
                                                                                                    </div>
                                                                                                )}
                                                                                            </div>
                                                                                        );
                                                                                    })}
                                                                                </div>
                                                                            );
                                                                        })}
                                                                    </div>
                                                                );
                                                            })}
                                                        </div>
                                                    </div>
                                                );
                                            })()}
                                        </div>
                                    ) : (
                                        <div className="flex-1 flex items-center justify-center text-xs text-slate-400 italic">No heatmap data available</div>
                                    )}
                                </div>
                            </div>

                        </div>
                    </div>
                </div>
                {/* End Main Dashboard Block */}
            </div>

            {/* Spacer to push content down and separate from the table (Matches EHS layout) */}
            <div className="h-4 shrink-0" />

            <div className="min-h-full p-8 bg-slate-50 dark:bg-transparent border-t border-slate-200 dark:border-slate-800">
                <section className="ios-widget bg-white dark:bg-slate-900 p-8 shadow-2xl shadow-slate-200/50 dark:shadow-none border-none h-[calc(100vh-200px)] flex flex-col">
                    <div className="flex justify-between items-center mb-6 shrink-0">
                        <div>
                            <h2 className="text-xl font-black text-slate-900 dark:text-white flex items-center gap-3">
                                <TableIcon className="text-medtronic" /> Operational Record Explorer
                            </h2>
                            <p className="text-[10px] text-slate-400 font-bold uppercase tracking-widest mt-1">流水明细追踪 ({activeData?.details?.length || 0} Records)</p>
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

                    <div className="overflow-auto rounded-2xl border border-slate-100 dark:border-slate-800 flex-1 relative">
                        <table className="w-full text-left border-separate border-spacing-0">
                            <thead className="bg-slate-50 dark:bg-slate-800/50 sticky top-0 z-10 shadow-sm">
                                <tr>
                                    {['#', 'Batch No', 'Product', 'Op No', 'Op Name', 'Qty', 'EH', 'Plant', 'Area', 'Prod Line'].map((th, i) => (
                                        <th key={th} className={`px-4 py-3 text-[10px] font-black text-slate-400 uppercase tracking-widest border-b border-slate-100 dark:border-slate-800 bg-slate-50 dark:bg-slate-900 ${i === 0 ? 'rounded-tl-2xl' : ''}`}>{th}</th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody className="text-xs font-bold text-slate-700 dark:text-slate-300 divide-y divide-slate-100 dark:divide-slate-800">
                                {records.map((row: any, i: number) => (
                                    <tr key={i} className="hover:bg-blue-50/50 dark:hover:bg-blue-900/10 transition-colors group">
                                        <td className="px-4 py-3 text-slate-400 text-[10px] font-mono">{(i + 1) + ((page - 1) * 50)}</td>
                                        <td className="px-4 py-3 border-r border-slate-50 dark:border-slate-800/50 font-mono text-xs text-medtronic font-bold" title={`Order: ${row.OrderNumber}\nDate: ${new Date(row.PostingDate).toLocaleDateString()}`}>
                                            {row.BatchNumber || row.OrderNumber}
                                        </td>
                                        <td className="px-4 py-3 font-bold">{row.Material}</td>
                                        <td className="px-4 py-3 font-mono text-xs text-slate-500">{row.Operation || '-'}</td>
                                        <td className="px-4 py-3 text-slate-500 truncate max-w-[150px]" title={row.rawOpDesc || row.operationDesc}>{row.operationDesc || row.rawOpDesc || '-'}</td>
                                        <td className="px-4 py-3 text-right font-mono text-slate-600">{row.ActualQuantity ? Math.round(row.ActualQuantity) : '-'}</td>
                                        <td className="px-4 py-3 text-right font-black text-medtronic">{Math.round(Number(row.actualEH))}h</td>
                                        <td className="px-4 py-3 text-slate-400 text-[10px]">{row.Plant}</td>
                                        <td className="px-4 py-3 text-slate-400 text-[10px] truncate max-w-[80px]" title={row.area}>{row.area || '-'}</td>
                                        <td className="px-4 py-3 text-slate-400 text-[10px] truncate max-w-[80px]" title={row.productLine}>{row.productLine || '-'}</td>
                                    </tr>
                                ))}
                                {(!records || records.length === 0) && !isRecordsLoading && (
                                    <tr>
                                        <td colSpan={10} className="px-6 py-20 text-center text-slate-400 italic">No detailed records found for this period.</td>
                                    </tr>
                                )}
                                {/* Load More Trigger */}
                                {hasMore && (
                                    <tr>
                                        <td colSpan={10} className="p-4 text-center">
                                            <button
                                                onClick={handleLoadMore}
                                                disabled={isRecordsLoading}
                                                className="px-6 py-2 bg-slate-100 dark:bg-slate-800 text-slate-500 dark:text-slate-400 hover:bg-medtronic hover:text-white rounded-full text-xs font-bold transition-all disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2 mx-auto"
                                            >
                                                {isRecordsLoading ? <Loader2 size={14} className="animate-spin" /> : <ChevronDown size={14} />}
                                                {isRecordsLoading ? 'Loading more...' : 'Load More Records'}
                                            </button>
                                        </td>
                                    </tr>
                                )}
                            </tbody>

                        </table>
                    </div>
                </section>
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
            {/* Loading Overlay (Bottom Right) */}
            {(isLoading || isDailyLoading || isRecordsLoading) && (
                <div className="fixed bottom-6 right-6 bg-blue-600/90 backdrop-blur-md text-white px-6 py-4 rounded-2xl shadow-2xl flex items-center gap-4 z-50 animate-in slide-in-from-bottom-10 border border-blue-500/50">
                    <Loader2 className="animate-spin" size={20} />
                    <div>
                        <div className="text-xs font-black">正在刷新数据...</div>
                        <div className="text-[10px] opacity-80">
                            {isDailyLoading ? 'Fetching Daily Details' :
                                isRecordsLoading ? 'Loading Records...' : 'Syncing with server'}
                        </div>
                    </div>
                </div>
            )}
        </StandardPageLayout>
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
