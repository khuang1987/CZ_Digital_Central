'use client';

import React, { useState, useEffect, useMemo, useRef } from 'react';
import { ShieldCheck, Search, Activity, Database, FileText, AlertTriangle, CheckCircle2, XCircle, Calendar, ChevronLeft, ChevronRight, ChevronDown, Clock, Info } from 'lucide-react';
import { format } from 'date-fns';
import { useUI } from '@/context/UIContext';
import FilterDropdown from '@/components/common/FilterDropdown';

// --- Types ---
interface ValidationData {
    view_metrics: any[];
    raw_mes: any[];
    raw_sfc: any[];
    raw_routing: any[];
    calendar_context: any[];
}

interface BatchInfo {
    BatchNumber: string;
    OrderNo: string;
    ProductNo_MES: string;
    ProductNo: string;
    Machine: string;
    OpCode: string;
    OpName: string;
    RawOpDesc?: string;
    Qty_In: number;
    Qty_Out: number;
    Operator: string;
}

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
const FISCAL_MONTHS = [
    { value: '05', label: 'May', yearOffset: -1 },
    { value: '06', label: 'Jun', yearOffset: -1 },
    { value: '07', label: 'Jul', yearOffset: -1 },
    { value: '08', label: 'Aug', yearOffset: -1 },
    { value: '09', label: 'Sep', yearOffset: -1 },
    { value: '10', label: 'Oct', yearOffset: -1 },
    { value: '11', label: 'Nov', yearOffset: -1 },
    { value: '12', label: 'Dec', yearOffset: -1 },
    { value: '01', label: 'Jan', yearOffset: 0 },
    { value: '02', label: 'Feb', yearOffset: 0 },
    { value: '03', label: 'Mar', yearOffset: 0 },
    { value: '04', label: 'Apr', yearOffset: 0 },
];

export default function BatchValidationPage() {
    const { isFilterOpen } = useUI();

    // --- Filter State ---
    const [granularity, setGranularity] = useState<'month' | 'year' | 'custom'>('month');

    // Fiscal State
    const [calendarData, setCalendarData] = useState<CalendarData | null>(null);
    const [selectedFiscalYear, setSelectedFiscalYear] = useState('FY25');
    const [selectedFiscalMonth, setSelectedFiscalMonth] = useState('May');
    const [selectedFiscalWeeks, setSelectedFiscalWeeks] = useState<number[]>([]);

    const [customRange, setCustomRange] = useState({ start: '', end: '' });
    const [selectedAreas, setSelectedAreas] = useState<string[]>(['1303']); // Default 1303
    const [selectedOps, setSelectedOps] = useState<string[]>([]);

    // Handlers
    const handleYearChange = (delta: number) => {
        if (!calendarData) return;
        const idx = calendarData.years.indexOf(selectedFiscalYear);
        const nextIdx = idx + delta;
        if (nextIdx >= 0 && nextIdx < calendarData.years.length) {
            setSelectedFiscalYear(calendarData.years[nextIdx]);
        }
    };

    const handleMonthChange = (delta: number) => {
        if (!calendarData || !calendarData.months[selectedFiscalYear]) return;
        const availableMonths = calendarData.months[selectedFiscalYear];
        const idx = availableMonths.indexOf(selectedFiscalMonth);
        let nextIdx = idx + delta;

        if (nextIdx < 0) {
            handleYearChange(1); // Usually years are DESC, so +1 is older? No, calendar list is ORDER BY fiscal_year DESC.
            // But handleYearChange logic above depends on array order. 
            // Let's stick to the labor-eh logic if possible or just use a simple delta.
        } else if (nextIdx >= availableMonths.length) {
            handleYearChange(-1);
        } else {
            setSelectedFiscalMonth(availableMonths[nextIdx]);
        }
        setSelectedFiscalWeeks([]); // Reset weeks when month changes
    };

    const toggleWeek = (week: number) => {
        if (selectedFiscalWeeks.includes(week)) {
            setSelectedFiscalWeeks(selectedFiscalWeeks.filter(w => w !== week));
        } else {
            setSelectedFiscalWeeks([...selectedFiscalWeeks, week]);
        }
    };

    // Core state
    const [batchList, setBatchList] = useState<string[]>([]);
    const [selectedBatch, setSelectedBatch] = useState('');
    const [batchSearch, setBatchSearch] = useState('');
    const [isBatchSelectOpen, setIsBatchSelectOpen] = useState(false);

    const [opList, setOpList] = useState<any[]>([]);
    const [selectedOp, setSelectedOp] = useState('');
    const [allOps, setAllOps] = useState<{ label: string, value: string }[]>([]);

    const [batchInfo, setBatchInfo] = useState<BatchInfo | null>(null);
    const [loading, setLoading] = useState(false);
    const [data, setData] = useState<ValidationData | null>(null);
    const [error, setError] = useState<string | null>(null);

    // --- Hardcoded Options ---
    // In real app, might fetch from API
    const areaOptions = [
        { label: '1303 (Changzhou)', value: '1303' },
        { label: '9997 (Kanghui)', value: '9997' }
    ];

    const opOptions = ['10', '20', '30', '40', '50', '60']; // Sample common ops

    // Helper: Calculate Date Range from Fiscal Selection
    const getFiscalDateRange = (fy: string, monthVal?: string) => {
        // fy format: 'FY25'
        const fyYear = 2000 + parseInt(fy.replace('FY', ''), 10);
        // FY25 ends in 2025. Start is May 2024.

        if (!monthVal) {
            // Whole Year: May (Year-1) to Apr (Year)
            const start = new Date(fyYear - 1, 4, 1); // May 1st
            const end = new Date(fyYear, 3, 30); // Apr 30th
            return { start: format(start, 'yyyy-MM-dd'), end: format(end, 'yyyy-MM-dd') };
        } else {
            // Specific Month
            const mConfig = FISCAL_MONTHS.find((m: any) => m.value === monthVal);
            if (!mConfig) return { start: '', end: '' };

            const realYear = fyYear + mConfig.yearOffset;
            const mIndex = parseInt(monthVal, 10) - 1; // 0-based

            const start = new Date(realYear, mIndex, 1);
            const end = new Date(realYear, mIndex + 1, 0); // Last day of month
            return { start: format(start, 'yyyy-MM-dd'), end: format(end, 'yyyy-MM-dd') };
        }
    };


    // --- Initialize Defaults from Calendar API & Sidebar Ops ---
    useEffect(() => {
        // Calendar
        fetch('/api/production/calendar')
            .then(res => res.json())
            .then((data: CalendarData) => {
                setCalendarData(data);
                if (data.currentFiscalInfo) {
                    setSelectedFiscalYear(data.currentFiscalInfo.fiscal_year);
                    setSelectedFiscalMonth(data.currentFiscalInfo.fiscal_month);
                }
            })
            .catch(err => console.error('Failed to load calendar:', err));

        // Sidebar Ops
        fetch('/api/server/batch-validation?mode=all_ops')
            .then(res => res.json())
            .then(data => setAllOps(data))
            .catch(err => console.error('Failed to load ops:', err));
    }, []);

    // --- Data Fetching (Batch List with Debounced Search) ---
    useEffect(() => {
        const fetchBatches = async () => {
            let url = `/api/server/batch-validation?mode=batch_list`;

            // Append filters
            if (granularity === 'custom' && customRange.start) {
                url += `&startDate=${customRange.start}`;
                if (customRange.end) url += `&endDate=${customRange.end}`;
            } else if (granularity === 'month' || granularity === 'year') {
                if (selectedFiscalYear) url += `&year=${selectedFiscalYear}`;
                if (granularity === 'month' && selectedFiscalMonth) url += `&month=${selectedFiscalMonth}`;
                if (selectedFiscalWeeks.length > 0) url += `&weeks=${selectedFiscalWeeks.join(',')}`;
            }

            if (selectedAreas.length > 0) url += `&areas=${selectedAreas.join(',')}`;
            if (selectedOps.length > 0) url += `&ops=${selectedOps.join(',')}`;

            // Add server-side search if typing
            if (batchSearch && !selectedBatch) {
                url += `&search=${encodeURIComponent(batchSearch)}`;
            }

            try {
                const res = await fetch(url);
                const data = await res.json();
                setBatchList(data);
                if (selectedBatch && !data.includes(selectedBatch)) {
                    // Keep the current selection even if not in the new filtered top 200
                    setBatchList(prev => prev.includes(selectedBatch) ? prev : [selectedBatch, ...prev]);
                }
            } catch (err) {
                console.error('Failed to fetch batches:', err);
            }
        };

        const timer = setTimeout(fetchBatches, 300);
        return () => clearTimeout(timer);
    }, [granularity, customRange, selectedAreas, selectedOps, selectedFiscalYear, selectedFiscalMonth, selectedFiscalWeeks, batchSearch, selectedBatch]);

    // --- Data Fetching (Op List) ---
    useEffect(() => {
        if (!selectedBatch) {
            setOpList([]);
            return;
        }
        fetch(`/api/server/batch-validation?mode=op_list&batch=${selectedBatch}`)
            .then(res => res.json())
            .then(data => {
                setOpList(data);
                // Default to first operation if not already selected
                if (data.length > 0 && !selectedOp) {
                    setSelectedOp(data[0].code);
                }
            })
            .catch(err => console.error(err));
    }, [selectedBatch]);

    // --- Data Fetching (Batch Info) ---
    useEffect(() => {
        if (!selectedBatch || !selectedOp) {
            setBatchInfo(null);
            return;
        }
        fetch(`/api/server/batch-validation?mode=batch_info&batch=${selectedBatch}&operation=${selectedOp}`)
            .then(res => res.json())
            .then(data => setBatchInfo(data))
            .catch(err => console.error(err));
    }, [selectedBatch, selectedOp]);

    const handleCalculate = async () => {
        if (!selectedBatch || !selectedOp) return;
        setLoading(true);
        setError(null);
        setData(null);
        try {
            const res = await fetch(`/api/server/batch-validation?mode=validate&batch=${selectedBatch}&operation=${selectedOp}`);
            const json = await res.json();
            if (json.error) throw new Error(json.error);
            setData(json);
        } catch (err: any) {
            setError(err.message || 'Failed to fetch validation data');
        } finally {
            setLoading(false);
        }
    };

    // --- Calculation Logic (Ported from Python) ---

    // Helper: Find calendar info for a date
    const getCalendarInfo = (date: Date, context: any[]) => {
        const dateStr = date.toISOString().split('T')[0];
        // Timestamps in JSON might be strings, need robust comparing
        const entry = context.find(c => c.CalendarDate.toString().startsWith(dateStr));
        return entry || { IsWorkday: true, CumulativeNonWorkDays: 0 }; // Default to workday if missing
    };

    const getNonWorkDaysDeduction = (start: Date | null, end: Date | null, context: any[]) => {
        if (!start || !end || !context.length) return 0;

        const startDate = new Date(start);
        const endDate = new Date(end);

        // Reset times for date-only comparison
        const sDateOnly = new Date(startDate.getFullYear(), startDate.getMonth(), startDate.getDate());
        const eDateOnly = new Date(endDate.getFullYear(), endDate.getMonth(), endDate.getDate());

        const startInfo = getCalendarInfo(startDate, context);
        const endInfo = getCalendarInfo(endDate, context);

        const startIsWork = startInfo.IsWorkday ? 1 : 0;
        const endIsWork = endInfo.IsWorkday ? 1 : 0;

        if (sDateOnly.getTime() === eDateOnly.getTime()) {
            // Same day
            if (startIsWork === 1) return 0;
            return (endDate.getTime() - startDate.getTime()) / 1000 / 86400;
        }

        let deductionSeconds = 0;

        // Part 1: Start Day Deduction
        if (startIsWork === 0) {
            const midnightNext = new Date(sDateOnly);
            midnightNext.setDate(midnightNext.getDate() + 1);
            deductionSeconds += (midnightNext.getTime() - startDate.getTime()) / 1000;
        }

        // Part 2: End Day Deduction
        if (endIsWork === 0) {
            const midnightEnd = new Date(eDateOnly);
            deductionSeconds += (endDate.getTime() - midnightEnd.getTime()) / 1000;
        }

        // Part 3: Middle Days
        // Logic: (EndCum - StartCum) - (1 if End is NW)
        const startCum = startInfo.CumulativeNonWorkDays || 0;
        const endCum = endInfo.CumulativeNonWorkDays || 0;

        const correction = endIsWork === 0 ? 1 : 0;
        const middleDays = (endCum - correction - startCum);

        if (middleDays > 0) {
            deductionSeconds += middleDays * 86400;
        }

        return deductionSeconds / 86400;
    };

    const manualCalcST = (qty: number, scrap: number, eh_machine: number, eh_labor: number, setup: number, oee: number, is_setup: string) => {
        if (eh_machine == null && eh_labor == null) return null;
        const eh = (eh_machine && eh_machine > 0) ? eh_machine : eh_labor;
        if (eh == null) return null;

        const setupVal = (is_setup === 'Yes' && setup) ? setup : 0;
        const oeeVal = (oee && oee > 0) ? oee : 0.77;
        const totalQty = (qty || 0) + (scrap || 0);

        const stHours = setupVal + (totalQty * eh / 3600 / oeeVal) + 0.5;
        return Number((stHours / 24).toFixed(4));
    };

    const manualCalcLT = (trackOut: Date, start: Date, deductionDays: number) => {
        if (!trackOut || !start) return null;
        const grossDays = (trackOut.getTime() - start.getTime()) / 1000 / 86400;
        const lt = grossDays - (deductionDays || 0);
        return Number(Math.max(lt, 0).toFixed(4));
    };


    // --- Render Logic ---
    const renderContent = () => {
        if (!data) return null;

        const viewRow = data.view_metrics[0] || {};
        const mesRow = data.raw_mes[0] || {};

        // --- Recalculation ---
        const sfcTrackIn = data.raw_sfc[0]?.TrackInTime ? new Date(data.raw_sfc[0].TrackInTime) : null;
        const mesEnterStep = mesRow.EnterStepTime ? new Date(mesRow.EnterStepTime) : null;
        const mesTrackIn = mesRow.TrackInTime ? new Date(mesRow.TrackInTime) : null;
        const trackOut = mesRow.TrackOutTime ? new Date(mesRow.TrackOutTime) : null;
        const prevBatchEnd = viewRow.PreviousBatchEndTime ? new Date(viewRow.PreviousBatchEndTime) : null;

        // LT Logic
        const isFirstOp = ['10', '0010'].includes(String(viewRow.Operation).trim());
        let actualStart = mesEnterStep;
        if (isFirstOp) {
            actualStart = sfcTrackIn || mesEnterStep || mesTrackIn;
        } else {
            actualStart = mesEnterStep;
        }

        // PT Logic
        let ptStart = prevBatchEnd || mesTrackIn;
        if (mesEnterStep && prevBatchEnd) {
            const gap = (mesEnterStep.getTime() - prevBatchEnd.getTime());
            if (gap > 0) ptStart = mesTrackIn || prevBatchEnd; // Gap exists
        }

        // Run Calcs
        const ltNw = getNonWorkDaysDeduction(actualStart, trackOut, data.calendar_context);
        const ptNw = getNonWorkDaysDeduction(ptStart, trackOut, data.calendar_context);

        const pyLT = manualCalcLT(trackOut!, actualStart!, ltNw);
        const pyPT = manualCalcLT(trackOut!, ptStart!, ptNw);
        const pyST = manualCalcST(
            viewRow.TrackOutQuantity,
            viewRow.ScrapQty,
            viewRow.EH_machine,
            viewRow.EH_labor,
            viewRow.SetupTime,
            viewRow.OEE,
            viewRow.IsSetup
        );

        // Validation Display Helpers
        const renderCheck = (sqlVal: number, pyVal: number | null, label: string) => {
            if (sqlVal == null || pyVal == null) return <span className="text-slate-400">-</span>;
            const diff = Math.abs(sqlVal - pyVal);
            const isMatch = diff < 0.001;
            return (
                <div className={`flex items-center gap-2 font-mono text-sm ${isMatch ? 'text-emerald-600' : 'text-red-500'}`}>
                    {isMatch ? <CheckCircle2 size={16} /> : <XCircle size={16} />}
                    <span>Diff: {diff.toFixed(4)}</span>
                </div>
            );
        };

        return (
            <div className="space-y-8 animate-in fade-in slide-in-from-bottom-4 duration-500">
                {/* 1. Comparison Card */}
                <div className="ios-widget p-6 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl shadow-sm">
                    <h3 className="text-sm font-black uppercase tracking-widest text-slate-500 mb-6 flex items-center gap-2">
                        <Activity size={16} /> Validation Results
                    </h3>
                    {/* Formula Explanation Section */}
                    <div className="mb-8 p-4 bg-blue-50/50 dark:bg-blue-900/20 border border-blue-100 dark:border-blue-800 rounded-xl space-y-4">
                        <h4 className="text-xs font-bold text-blue-700 dark:text-blue-400 uppercase tracking-widest flex items-center gap-2">
                            <Clock size={14} /> Calculation logic & step tracing
                        </h4>
                        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                            <div className="space-y-2">
                                <p className="text-[10px] font-bold text-slate-400 uppercase">Lead Time (LT)</p>
                                <div className="p-2 bg-white dark:bg-slate-800 rounded-lg border border-slate-100 dark:border-slate-700 font-mono text-[10px] leading-relaxed">
                                    <div className="text-blue-600 font-bold mb-1">LT = (Out - Start) - NonWork</div>
                                    <div className="text-slate-500 italic">Values applied:</div>
                                    <div>Out: {trackOut?.toLocaleString()}</div>
                                    <div>Start: {actualStart?.toLocaleString()}</div>
                                    <div>NW Ded: -{ltNw.toFixed(4)}d</div>
                                    <div className="border-t border-slate-100 dark:border-slate-700 mt-1 pt-1 font-bold text-slate-700 dark:text-slate-300">= {pyLT?.toFixed(4)} days</div>
                                </div>
                            </div>
                            <div className="space-y-2">
                                <p className="text-[10px] font-bold text-slate-400 uppercase">Process Time (PT)</p>
                                <div className="p-2 bg-white dark:bg-slate-800 rounded-lg border border-slate-100 dark:border-slate-700 font-mono text-[10px] leading-relaxed">
                                    <div className="text-blue-600 font-bold mb-1">PT = (Out - PrevEnd) - NonWork</div>
                                    <div className="text-slate-500 italic">Values applied:</div>
                                    <div>Out: {trackOut?.toLocaleString()}</div>
                                    <div>PrevEnd: {ptStart?.toLocaleString()}</div>
                                    <div>NW Ded: -{ptNw.toFixed(4)}d</div>
                                    <div className="border-t border-slate-100 dark:border-slate-700 mt-1 pt-1 font-bold text-slate-700 dark:text-slate-300">= {pyPT?.toFixed(4)} days</div>
                                </div>
                            </div>
                            <div className="space-y-2">
                                <p className="text-[10px] font-bold text-slate-400 uppercase">Standard Time (ST)</p>
                                <div className="p-2 bg-white dark:bg-slate-800 rounded-lg border border-slate-100 dark:border-slate-700 font-mono text-[10px] leading-relaxed">
                                    <div className="text-blue-600 font-bold mb-1">ST = Setup + (Qty * EH / 3600 / OEE) + 0.5hr</div>
                                    <div className="text-slate-500 italic">Values applied:</div>
                                    <div>Qty: {viewRow.TrackOutQuantity + (viewRow.ScrapQty || 0)}</div>
                                    <div>EH: {viewRow.EH_machine || viewRow.EH_labor || 0}s</div>
                                    <div>OEE: {viewRow.OEE || 0.77}</div>
                                    <div className="border-t border-slate-100 dark:border-slate-700 mt-1 pt-1 font-bold text-slate-700 dark:text-slate-300">= {pyST?.toFixed(4)} days</div>
                                </div>
                            </div>
                        </div>
                    </div>

                    <div className="overflow-x-auto">
                        <table className="w-full text-sm text-left">
                            <thead className="bg-slate-50 dark:bg-slate-800/50 text-xs uppercase text-slate-500 font-bold">
                                <tr>
                                    <th className="px-6 py-4 rounded-tl-xl">Metric</th>
                                    <th className="px-6 py-4">SQL View Result</th>
                                    <th className="px-6 py-4">Client Recalculation</th>
                                    <th className="px-6 py-4 rounded-tr-xl">Status</th>
                                </tr>
                            </thead>
                            <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                                <tr>
                                    <td className="px-6 py-4 font-bold">LT (Lead Time)</td>
                                    <td className="px-6 py-4 font-mono">{viewRow['LT(d)']?.toFixed(4) || '-'}</td>
                                    <td className="px-6 py-4 font-mono">{pyLT?.toFixed(4) || '-'}</td>
                                    <td className="px-6 py-4">{renderCheck(viewRow['LT(d)'], pyLT, 'LT')}</td>
                                </tr>
                                <tr>
                                    <td className="px-6 py-4 font-bold">PT (Process Time)</td>
                                    <td className="px-6 py-4 font-mono">{viewRow['PT(d)']?.toFixed(4) || '-'}</td>
                                    <td className="px-6 py-4 font-mono">{pyPT?.toFixed(4) || '-'}</td>
                                    <td className="px-6 py-4">{renderCheck(viewRow['PT(d)'], pyPT, 'PT')}</td>
                                </tr>
                                <tr>
                                    <td className="px-6 py-4 font-bold">ST (Standard Time)</td>
                                    <td className="px-6 py-4 font-mono">{viewRow['ST(d)']?.toFixed(4) || '-'}</td>
                                    <td className="px-6 py-4 font-mono">{pyST?.toFixed(4) || '-'}</td>
                                    <td className="px-6 py-4">{renderCheck(viewRow['ST(d)'], pyST, 'ST')}</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                    <div className="mt-4 p-4 bg-slate-50 dark:bg-slate-800/50 rounded-xl text-xs text-slate-500 font-mono">
                        <p>Logic Context:</p>
                        <p>LT Start: {actualStart?.toLocaleString() || 'N/A'} (Ded: {ltNw.toFixed(4)}d)</p>
                        <p>PT Start: {ptStart?.toLocaleString() || 'N/A'} (Ded: {ptNw.toFixed(4)}d)</p>
                        <p>Status: {viewRow.CompletionStatus || 'Unknown'}</p>
                    </div>
                </div>

                {/* 2. Raw Data Tabs */}
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    <RawDataCard title="Raw MES Data" icon={<Database size={16} />} data={data.raw_mes} />
                    <RawDataCard title="Raw SFC Data" icon={<Database size={16} />} data={data.raw_sfc} />
                    <div className="lg:col-span-2">
                        <RawDataCard title="SAP Routing Standard" icon={<FileText size={16} />} data={data.raw_routing} />
                    </div>
                </div>
            </div>
        );
    };

    return (
        <div className="flex flex-1 overflow-hidden min-h-[calc(100vh-140px)] rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm bg-white dark:bg-slate-900">
            {/* --- Left Sidebar Filters --- */}
            <aside className={`${isFilterOpen ? 'w-72 opacity-100' : 'w-0 opacity-0 overflow-hidden'} border-r border-slate-200 dark:border-slate-800 bg-slate-50/30 dark:bg-slate-800/20 flex flex-col transition-all duration-300 shrink-0 z-20`}>
                <div className="p-6 space-y-8 w-72 flex flex-col h-full overflow-y-auto">
                    {/* 1. Time Granularity */}
                    <section>
                        <h3 className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-4 flex items-center gap-2">
                            时间粒度 (Granularity)
                        </h3>
                        <div className="flex flex-wrap gap-2">
                            {['month', 'year', 'custom'].map(g => (
                                <button
                                    key={g}
                                    onClick={() => setGranularity(g as any)}
                                    className={`flex-1 py-1.5 px-2 rounded-lg text-[10px] font-black uppercase tracking-tighter transition-all ${granularity === g
                                        ? 'bg-medtronic text-white shadow-lg'
                                        : 'bg-slate-50 dark:bg-slate-800 text-slate-500'}`}
                                >
                                    {g === 'month' ? '按月' : g === 'year' ? '按年' : '自由'}
                                </button>
                            ))}
                        </div>
                    </section>

                    {/* 2. Date Selection (Fiscal Calendar) */}
                    <section>
                        <h3 className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-4 flex items-center gap-2">
                            <Calendar size={12} /> 时间选择
                        </h3>

                        <div className="space-y-4">
                            {/* Year Navigator */}
                            <div className="flex items-center bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-700 rounded-xl group transition-all hover:border-medtronic/30">
                                <button
                                    onClick={() => handleYearChange(-1)}
                                    className="p-3 text-slate-700 dark:text-slate-300 hover:text-medtronic transition-colors border-r border-slate-200/50 dark:border-slate-700"
                                >
                                    <ChevronLeft size={14} />
                                </button>
                                <div className="flex-1 relative">
                                    <select
                                        value={selectedFiscalYear}
                                        onChange={(e) => setSelectedFiscalYear(e.target.value)}
                                        className="w-full bg-transparent text-slate-800 dark:text-slate-200 px-4 py-2 text-xs font-bold outline-none appearance-none cursor-pointer text-center"
                                    >
                                        {calendarData ? calendarData.years.map(y => <option key={y} value={y}>{y}</option>) : <option>Loading...</option>}
                                    </select>
                                    <div className="absolute right-3 top-1/2 -translate-y-1/2 pointer-events-none text-slate-300 dark:text-slate-600">
                                        <ChevronRight size={10} className="rotate-90" />
                                    </div>
                                </div>
                                <button
                                    onClick={() => handleYearChange(1)}
                                    className="p-3 text-slate-700 dark:text-slate-300 hover:text-medtronic transition-colors border-l border-slate-200/50 dark:border-slate-700"
                                >
                                    <ChevronRight size={14} />
                                </button>
                            </div>

                            {/* Month Navigator (Only if Month) */}
                            {granularity === 'month' && (
                                <div className="flex items-center bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-700 rounded-xl group transition-all hover:border-medtronic/30">
                                    <button
                                        onClick={() => handleMonthChange(-1)}
                                        className="p-3 text-slate-700 dark:text-slate-300 hover:text-medtronic transition-colors border-r border-slate-200/50 dark:border-slate-700"
                                    >
                                        <ChevronLeft size={14} />
                                    </button>
                                    <div className="flex-1 relative">
                                        <select
                                            value={selectedFiscalMonth}
                                            onChange={(e) => setSelectedFiscalMonth(e.target.value)}
                                            className="w-full bg-transparent text-slate-800 dark:text-slate-200 px-4 py-2 text-xs font-bold outline-none appearance-none cursor-pointer text-center"
                                        >
                                            {calendarData && calendarData.months[selectedFiscalYear] ? calendarData.months[selectedFiscalYear].map(m => (
                                                <option key={m} value={m}>
                                                    {selectedFiscalYear} {m}
                                                </option>
                                            )) : <option>Loading...</option>}
                                        </select>
                                        <div className="absolute right-3 top-1/2 -translate-y-1/2 pointer-events-none text-slate-300 dark:text-slate-600">
                                            <ChevronRight size={10} className="rotate-90" />
                                        </div>
                                    </div>
                                    <button
                                        onClick={() => handleMonthChange(1)}
                                        className="p-3 text-slate-700 dark:text-slate-300 hover:text-medtronic transition-colors border-l border-slate-200/50 dark:border-slate-700"
                                    >
                                        <ChevronRight size={14} />
                                    </button>
                                </div>
                            )}

                            {/* Fiscal Weeks */}
                            {granularity === 'month' && calendarData && calendarData.weeks[selectedFiscalMonth] && (
                                <div className="space-y-3">
                                    <h4 className="text-[9px] font-bold text-slate-400 uppercase tracking-widest pl-1">
                                        Fiscal Weeks
                                    </h4>
                                    <div className="flex flex-wrap gap-2">
                                        {calendarData.weeks[selectedFiscalMonth].map(w => (
                                            <button
                                                key={w}
                                                onClick={() => toggleWeek(w)}
                                                className={`flex-1 min-w-[50px] py-1.5 rounded-lg text-[10px] font-bold border transition-all ${selectedFiscalWeeks.includes(w)
                                                    ? 'bg-medtronic/10 border-medtronic text-medtronic shadow-sm'
                                                    : 'bg-white dark:bg-slate-800 border-slate-200 dark:border-slate-700 text-slate-500 hover:border-slate-400'}`}
                                            >
                                                W{w}
                                            </button>
                                        ))}
                                    </div>
                                    {selectedFiscalWeeks.length > 0 && (
                                        <button
                                            onClick={() => setSelectedFiscalWeeks([])}
                                            className="text-[9px] font-bold text-slate-400 hover:text-red-400 transition-colors flex items-center gap-1 pl-1"
                                        >
                                            <XCircle size={10} /> Clear Selection
                                        </button>
                                    )}
                                </div>
                            )}

                            {/* Custom Date Range */}
                            {granularity === 'custom' && (
                                <div className="space-y-2 p-2 bg-slate-50 rounded-xl border border-dashed border-slate-300">
                                    <input
                                        type="date"
                                        value={customRange.start}
                                        onChange={e => setCustomRange(p => ({ ...p, start: e.target.value }))}
                                        className="w-full bg-white dark:bg-slate-800 border-slate-200 text-xs p-2 rounded-lg"
                                    />
                                    <input
                                        type="date"
                                        value={customRange.end}
                                        onChange={e => setCustomRange(p => ({ ...p, end: e.target.value }))}
                                        className="w-full bg-white dark:bg-slate-800 border-slate-200 text-xs p-2 rounded-lg"
                                    />
                                </div>
                            )}
                        </div>
                    </section>

                    {/* 3. Plant Selection (Buttons) */}
                    <section>
                        <h3 className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-4 flex items-center gap-2">
                            工厂选择 (Plant)
                        </h3>
                        <div className="flex items-center gap-3">
                            <button
                                onClick={() => setSelectedAreas(['1303'])}
                                className={`flex-1 py-3 px-2 rounded-xl text-xs font-bold transition-all border-2 ${selectedAreas.includes('1303')
                                    ? 'border-medtronic bg-medtronic text-white shadow-xl shadow-blue-500/20'
                                    : 'border-slate-200 bg-white text-slate-600 hover:border-medtronic/50'}`}
                            >
                                常州 1303
                            </button>
                            <button
                                onClick={() => setSelectedAreas(['9997'])}
                                className={`flex-1 py-3 px-2 rounded-xl text-xs font-bold transition-all border-2 ${selectedAreas.includes('9997')
                                    ? 'border-medtronic bg-medtronic text-white shadow-xl shadow-blue-500/20'
                                    : 'border-slate-200 bg-white text-slate-600 hover:border-medtronic/50'}`}
                            >
                                康辉 9997
                            </button>
                        </div>
                    </section>

                    {/* 4. Operation */}
                    <section>
                        <FilterDropdown
                            title="工序选择 (Operation)"
                            options={allOps}
                            selected={selectedOps}
                            onChange={setSelectedOps}
                            placeholder="所有工序"
                        />
                    </section>
                </div>
            </aside>

            <main className="flex-1 overflow-y-auto min-w-0 bg-white dark:bg-slate-900 rounded-r-2xl border-l border-slate-100 dark:border-slate-800">
                <div className="p-8 space-y-8 min-h-full">

                    {/* Controls */}
                    <div className="bg-white dark:bg-slate-900 p-6 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm grid grid-cols-1 md:grid-cols-[320px,260px,auto] gap-6 items-start">
                        <div className="flex-1 w-full relative">
                            <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider mb-2">
                                Batch Number ({batchList.length}{batchList.length >= 200 ? '+' : ''})
                            </label>
                            <div className="relative group">
                                <div className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400 group-focus-within:text-medtronic transition-colors">
                                    <Search size={16} />
                                </div>
                                <input
                                    type="text"
                                    className={`w-full h-10 pl-10 pr-10 rounded-xl border bg-slate-50 text-sm font-mono focus:ring-2 outline-none transition-all ${batchList.length >= 200 ? 'border-amber-300 focus:ring-amber-500' : 'border-slate-200 focus:ring-blue-500'}`}
                                    placeholder="Search or Select Batch..."
                                    value={batchSearch}
                                    onFocus={() => setIsBatchSelectOpen(true)}
                                    onChange={(e) => {
                                        setBatchSearch(e.target.value);
                                        setIsBatchSelectOpen(true);
                                    }}
                                />
                                {batchSearch && (
                                    <button
                                        onClick={(e) => {
                                            e.stopPropagation();
                                            setBatchSearch('');
                                            setSelectedBatch('');
                                            setIsBatchSelectOpen(false);
                                        }}
                                        className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-300 hover:text-slate-500 transition-colors p-1"
                                    >
                                        <XCircle size={16} />
                                    </button>
                                )}
                                {isBatchSelectOpen && (
                                    <div className="absolute top-full left-0 right-0 mt-2 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-xl shadow-2xl z-50 max-h-60 overflow-y-auto overflow-x-hidden animate-in fade-in slide-in-from-top-2 duration-200">
                                        <div className="p-2 border-b border-slate-100 dark:border-slate-800 sticky top-0 bg-white/95 dark:bg-slate-900/95 backdrop-blur-sm z-10 flex items-center justify-between">
                                            <span className="text-[10px] font-black text-slate-400 uppercase tracking-widest pl-1">
                                                {batchList.length >= 200 ? 'First 200 Matched' : 'Available Batches'}
                                            </span>
                                            <button onClick={() => setIsBatchSelectOpen(false)} className="text-slate-400 hover:text-slate-600 p-1">
                                                <XCircle size={14} />
                                            </button>
                                        </div>
                                        {batchList.length === 0 ? (
                                            <div className="p-4 text-center text-xs text-slate-400 italic">No batches found</div>
                                        ) : (
                                            <div className="p-1 grid grid-cols-1 gap-0.5">
                                                {batchList
                                                    .filter(b => !batchSearch || b.toLowerCase().includes(batchSearch.toLowerCase()))
                                                    .map(b => (
                                                        <button
                                                            key={b}
                                                            onClick={() => {
                                                                setSelectedBatch(b);
                                                                setBatchSearch(b);
                                                                setIsBatchSelectOpen(false);
                                                            }}
                                                            className={`text-left px-3 py-2 rounded-lg text-xs font-mono transition-all ${selectedBatch === b
                                                                ? 'bg-medtronic text-white font-bold shadow-md'
                                                                : 'hover:bg-slate-50 dark:hover:bg-slate-800 text-slate-600 dark:text-slate-400'}`}
                                                        >
                                                            {b}
                                                        </button>
                                                    ))
                                                }
                                            </div>
                                        )}
                                    </div>
                                )}
                            </div>
                            {batchList.length >= 200 && (
                                <div className="mt-2 flex items-center gap-2 p-2 bg-amber-50 dark:bg-amber-900/10 border border-amber-100 dark:border-amber-900/30 rounded-lg">
                                    <Info size={12} className="text-amber-600" />
                                    <p className="text-[10px] text-amber-700 dark:text-amber-400 font-bold leading-none">
                                        Filtered Top 200. Use sidebar for older data.
                                    </p>
                                </div>
                            )}
                        </div>
                        <div className="flex-1 w-full">
                            <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider mb-2">Operation</label>
                            <select
                                className="w-full h-10 px-3 rounded-xl border border-slate-200 bg-slate-50 text-sm font-mono focus:ring-2 focus:ring-blue-500 outline-none transition-all"
                                value={selectedOp}
                                onChange={(e) => setSelectedOp(e.target.value)}
                                disabled={!selectedBatch}
                            >
                                <option value="">-- Select Op --</option>
                                {opList.map(op => (
                                    <option key={op.code} value={op.code}>
                                        {op.code} - {op.desc || op.name}
                                    </option>
                                ))}
                            </select>
                        </div>
                        <div className="w-full">
                            <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider mb-2 opacity-0 select-none">Action</label>
                            <button
                                onClick={handleCalculate}
                                disabled={!selectedBatch || !selectedOp || loading}
                                className="w-full h-10 px-8 bg-medtronic hover:bg-blue-700 text-white font-bold rounded-xl shadow-lg shadow-blue-500/30 transition-all disabled:opacity-50 disabled:shadow-none min-w-[120px]"
                            >
                                {loading ? <Activity className="animate-spin mx-auto" size={20} /> : "Calculate"}
                            </button>
                        </div>
                    </div>

                    {/* Batch Information Card */}
                    {batchInfo && (
                        <div className="bg-gradient-to-br from-white to-slate-50 dark:from-slate-900 dark:to-slate-800 p-6 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm animate-in fade-in slide-in-from-top-4 duration-500">
                            <div className="flex items-center gap-3 mb-6">
                                <div className="p-2 bg-blue-100 dark:bg-blue-900/30 text-blue-600 rounded-lg">
                                    <FileText size={18} />
                                </div>
                                <h3 className="text-sm font-black text-slate-800 dark:text-slate-100 uppercase tracking-widest">
                                    Batch Summary Information
                                </h3>
                            </div>
                            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-6">
                                <InfoItem label="Batch Number" value={batchInfo.BatchNumber} color="blue" />
                                <InfoItem label="Order Number" value={batchInfo.OrderNo} />
                                <InfoItem label="Product (MES)" value={batchInfo.ProductNo_MES} />
                                <InfoItem label="Material" value={batchInfo.ProductNo} />
                                <InfoItem label="Operation" value={`${batchInfo.OpCode} - ${batchInfo.RawOpDesc || batchInfo.OpName}`} color="indigo" />
                                <InfoItem label="Machine" value={batchInfo.Machine} />
                                <InfoItem label="Qty In" value={batchInfo.Qty_In} />
                                <InfoItem label="Qty Out" value={batchInfo.Qty_Out} />
                                <InfoItem label="Operator" value={batchInfo.Operator} />
                                <InfoItem label="Last Updated" value={new Date().toLocaleDateString()} />
                            </div>
                        </div>
                    )}

                    {/* Error */}
                    {error && (
                        <div className="p-4 bg-red-50 text-red-600 rounded-xl flex items-center gap-3 text-sm font-bold border border-red-100">
                            <AlertTriangle size={20} /> {error}
                        </div>
                    )}

                    {/* Content */}
                    {renderContent()}
                </div>
            </main>
        </div>
    );
}

function InfoItem({ label, value, color = 'slate' }: { label: string, value: any, color?: string }) {
    const colorClasses: Record<string, string> = {
        blue: 'text-blue-600 bg-blue-50 dark:bg-blue-900/20',
        indigo: 'text-indigo-600 bg-indigo-50 dark:bg-indigo-900/20',
        slate: 'text-slate-600 bg-slate-50 dark:bg-slate-800/50'
    };

    return (
        <div className="space-y-1.5">
            <span className="text-[10px] font-black text-slate-400 uppercase tracking-widest block">{label}</span>
            <div className={`px-2 py-1 rounded text-xs font-mono font-bold inline-block ${colorClasses[color] || colorClasses.slate}`}>
                {value !== null && value !== undefined ? String(value) : 'N/A'}
            </div>
        </div>
    );
}

function RawDataCard({ title, icon, data }: { title: string, icon: any, data: any[] }) {
    if (!data || data.length === 0) return null;
    const keys = Object.keys(data[0]);

    return (
        <div className="ios-widget p-6 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl shadow-sm overflow-hidden">
            <h3 className="text-xs font-black uppercase tracking-widest text-slate-500 mb-4 flex items-center gap-2">
                {icon} {title}
            </h3>
            <div className="overflow-x-auto">
                <table className="w-full text-xs text-left whitespace-nowrap">
                    <thead className="bg-slate-50 dark:bg-slate-800 border-b border-slate-100 dark:border-slate-800">
                        <tr>
                            {keys.map(k => <th key={k} className="px-4 py-2 font-bold text-slate-500">{k}</th>)}
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                        {data.map((row, i) => (
                            <tr key={i} className="hover:bg-slate-50 transition-colors">
                                {keys.map(k => (
                                    <td key={k} className="px-4 py-2 font-mono text-slate-600 dark:text-slate-400">
                                        {row[k] !== null ? String(row[k]) : <span className="text-slate-300">null</span>}
                                    </td>
                                ))}
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
}
