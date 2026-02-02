'use client';

import React, { useState } from 'react';
import { Check, X, AlertTriangle, MessageSquare, ShieldCheck } from 'lucide-react';

export type DailyStatus = 'Safe' | 'Incident' | 'Holiday' | null;

interface GreenCrossProps {
    year: number;
    month: number; // 1-12
    data: Record<string, { status: DailyStatus; details?: string }>; // key: "YYYY-MM-DD"
    onUpdate: (date: string, status: DailyStatus, details?: string) => void;
}

export default function GreenCross({ year, month, data, onUpdate }: GreenCrossProps) {
    const daysInMonth = new Date(year, month, 0).getDate();
    const today = new Date();
    // Normalize today to avoid time comparison issues
    today.setHours(0, 0, 0, 0);

    const isCurrentMonth = today.getFullYear() === year && (today.getMonth() + 1) === month;
    const currentDay = today.getDate();

    const [selectedDay, setSelectedDay] = useState<number | null>(null);
    const [selectedStatus, setSelectedStatus] = useState<DailyStatus>(null);
    const [note, setNote] = useState('');
    const [password, setPassword] = useState('');
    const [isSaving, setIsSaving] = useState(false);

    // Generate days 1..31
    const days = Array.from({ length: daysInMonth }, (_, i) => i + 1);

    // Helper to get status
    const getDateStr = (d: number) => `${year}-${String(month).padStart(2, '0')}-${String(d).padStart(2, '0')}`;

    // Updated Logic: Default to 'Safe' for past dates if no record exists
    const getStatus = (d: number): DailyStatus => {
        const dateStr = getDateStr(d);
        if (data[dateStr]?.status) return data[dateStr].status;

        const checkDate = new Date(year, month - 1, d);
        checkDate.setHours(0, 0, 0, 0);

        if (checkDate <= today) {
            return 'Safe';
        }

        return null;
    };

    const handleDayClick = (day: number) => {
        // Future check
        const checkDate = new Date(year, month - 1, day);
        checkDate.setHours(0, 0, 0, 0);

        if (checkDate > today) return;

        setSelectedDay(day);
        const currentData = data[getDateStr(day)];
        setSelectedStatus(currentData?.status || getStatus(day));
        setNote(currentData?.details || '');
    };

    const handleSave = async () => {
        if (!selectedDay || !selectedStatus) return;

        // Validate password
        if (password !== '0000') {
            alert('Password incorrect! Please enter: 0000');
            return;
        }

        setIsSaving(true);
        try {
            await onUpdate(getDateStr(selectedDay), selectedStatus, note);
            setSelectedDay(null);
            setNote('');
            setPassword('');
            setSelectedStatus(null);
        } catch (e) {
            console.error('Save failed:', e);
            alert('Failed to save safety status. Please try again.');
        } finally {
            setIsSaving(false);
        }
    };

    return (
        <div className="flex flex-col h-full bg-white dark:bg-slate-900 rounded-2xl shadow-sm border border-slate-200 dark:border-slate-800 p-4 relative overflow-hidden">
            <div className="mb-4 flex justify-between items-center shrink-0">
                <h3 className="text-sm font-black text-slate-800 dark:text-white uppercase tracking-widest flex items-center gap-2">
                    <div className="w-6 h-6 bg-emerald-500 rounded flex items-center justify-center text-white font-bold text-lg">+</div>
                    Safety Green Cross
                </h3>
                <div className="text-[10px] font-bold text-slate-400 uppercase">
                    {year} / {String(month).padStart(2, '0')}
                </div>
            </div>

            {/* Grid Container */}
            <div className="grid grid-cols-7 gap-2 flex-1 auto-rows-fr min-h-0">
                {/* Weekday Headers */}
                {['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'].map(d => (
                    <div key={d} className="text-center text-[9px] font-black text-slate-300 uppercase tracking-wider py-0.5">
                        {d}
                    </div>
                ))}

                {/* Padding for start of month */}
                {Array.from({ length: new Date(year, month - 1, 1).getDay() }).map((_, i) => (
                    <div key={`empty-${i}`} />
                ))}

                {/* Days */}
                {days.map(day => {
                    const status = getStatus(day);

                    const checkDate = new Date(year, month - 1, day);
                    checkDate.setHours(0, 0, 0, 0);
                    const isFuture = checkDate > today;
                    const isToday = checkDate.getTime() === today.getTime();

                    let bgClass = 'bg-slate-50 dark:bg-slate-800 border-slate-200 dark:border-slate-700 text-slate-400';

                    if (status === 'Safe') {
                        if (isToday) {
                            // Today - Outline only, no fill per user request
                            bgClass = 'bg-white dark:bg-slate-900 border-2 border-emerald-500 text-emerald-600 shadow-sm ring-2 ring-emerald-500/10';
                        } else {
                            bgClass = 'bg-emerald-500 border-emerald-600 text-white shadow shadow-emerald-500/30';
                        }
                    }

                    if (status === 'Incident') {
                        bgClass = 'bg-red-500 border-red-600 text-white shadow shadow-red-500/30 animate-pulse';
                    }

                    return (
                        <button
                            key={day}
                            onClick={() => handleDayClick(day)}
                            disabled={isFuture}
                            className={`
                                relative rounded-xl border flex flex-col items-center justify-center p-1.5 transition-all
                                ${bgClass}
                                ${isFuture ? 'opacity-30 cursor-not-allowed' : 'hover:scale-105 active:scale-95 cursor-pointer'}
                                min-h-[45px]
                            `}
                        >
                            <span className="text-base font-black leading-none">{day}</span>
                            {status === 'Incident' && <AlertTriangle size={12} className="mt-1" />}
                            {status === 'Safe' && !isToday && <Check size={12} className="mt-1" />}
                            {isToday && <span className="text-[8px] font-black uppercase mt-1 leading-none">Today</span>}
                        </button>
                    );
                })}
            </div>

            {/* Legend */}
            <div className="mt-4 flex gap-4 text-[9px] font-bold uppercase text-slate-500 justify-center text-center shrink-0">
                <div className="flex items-center gap-1.5"><div className="w-2.5 h-2.5 rounded bg-emerald-500" /> Safe</div>
                <div className="flex items-center gap-1.5"><div className="w-2.5 h-2.5 rounded bg-red-500" /> Incident</div>
            </div>

            {/* Edit Dialog (Simple Overlay) */}
            {selectedDay && (
                <div className="absolute inset-0 z-50 bg-white/95 dark:bg-slate-900/95 backdrop-blur-sm rounded-2xl flex flex-col items-center justify-top p-8 overflow-y-auto">
                    <h4 className="text-xl font-black text-slate-800 dark:text-white mb-2">
                        Day {selectedDay} Status
                    </h4>
                    <p className="text-[10px] text-slate-500 mb-6 font-bold uppercase tracking-widest">Mark safety status</p>

                    <div className="flex gap-4 w-full mb-6">
                        <button
                            onClick={() => setSelectedStatus('Safe')}
                            className={`flex-1 py-4 rounded-2xl transition-all flex flex-col items-center gap-2 border-2 ${selectedStatus === 'Safe'
                                ? 'bg-emerald-500 text-white border-emerald-500 shadow-lg shadow-emerald-500/20'
                                : 'bg-emerald-50 dark:bg-emerald-900/10 text-emerald-600 border-emerald-100 dark:border-emerald-800 hover:border-emerald-200'
                                }`}
                        >
                            <Check size={24} />
                            <span className="font-black uppercase text-xs">Safe</span>
                        </button>
                        <button
                            onClick={() => setSelectedStatus('Incident')}
                            className={`flex-1 py-4 rounded-2xl transition-all flex flex-col items-center gap-2 border-2 ${selectedStatus === 'Incident'
                                ? 'bg-red-500 text-white border-red-500 shadow-lg shadow-red-500/20'
                                : 'bg-red-50 dark:bg-red-900/10 text-red-600 border-red-100 dark:border-red-800 hover:border-red-200'
                                }`}
                        >
                            <AlertTriangle size={24} />
                            <span className="font-black uppercase text-xs">Incident</span>
                        </button>
                    </div>

                    <div className="w-full relative mb-4">
                        <MessageSquare size={16} className="absolute top-3 left-3 text-slate-400" />
                        <textarea
                            value={note}
                            onChange={e => setNote(e.target.value)}
                            placeholder="Add incident details (optional)..."
                            className="w-full bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl py-2 pl-9 pr-4 text-xs font-medium outline-none focus:ring-2 focus:ring-medtronic resize-none h-20"
                        />
                    </div>

                    <div className="w-full mb-6">
                        <div className="flex items-center gap-2 mb-2 px-1">
                            <ShieldCheck size={12} className="text-medtronic" />
                            <span className="text-[10px] font-black uppercase text-slate-400 tracking-widest">Verify Password</span>
                        </div>
                        <input
                            type="password"
                            value={password}
                            onChange={e => setPassword(e.target.value)}
                            placeholder="Enter password (0000)"
                            className="w-full bg-slate-100 dark:bg-slate-800 border-none rounded-xl py-3 px-4 text-sm font-black outline-none focus:ring-2 focus:ring-medtronic text-center tracking-widest"
                        />
                    </div>

                    <div className="flex flex-col w-full gap-4">
                        <button
                            onClick={handleSave}
                            disabled={isSaving || !selectedStatus}
                            className="w-full py-4 bg-medtronic hover:bg-medtronic-dark text-white rounded-2xl font-black uppercase tracking-widest shadow-lg shadow-medtronic/20 transition-all active:scale-[0.98] disabled:opacity-50 disabled:grayscale"
                        >
                            {isSaving ? 'Saving Changes...' : 'Save & Close'}
                        </button>

                        <button
                            onClick={() => {
                                setSelectedDay(null);
                                setPassword('');
                                setSelectedStatus(null);
                            }}
                            className="text-[10px] font-black text-slate-400 hover:text-slate-600 uppercase tracking-widest py-2"
                        >
                            Cancel
                        </button>
                    </div>
                </div>
            )}
        </div>
    );
}
