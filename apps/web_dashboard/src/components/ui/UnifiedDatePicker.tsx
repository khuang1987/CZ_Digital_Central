'use client';

import React from 'react';
import { Calendar } from 'lucide-react';

interface UnifiedDatePickerProps extends React.InputHTMLAttributes<HTMLInputElement> {
    label?: string;
}

export function UnifiedDatePicker({ label, className = '', ...props }: UnifiedDatePickerProps) {
    return (
        <div className="w-full">
            {label && (
                <label className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-1.5 block ml-1">
                    {label}
                </label>
            )}
            <div className="relative group">
                <div className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400 group-focus-within:text-medtronic transition-colors pointer-events-none">
                    <Calendar size={14} />
                </div>
                <input
                    type="date"
                    className={`
                        w-full bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-xl
                        py-2.5 px-3 pl-9 pr-3
                        text-xs font-bold text-slate-700 dark:text-slate-200
                        outline-none focus:ring-1 focus:ring-medtronic focus:border-medtronic
                        cursor-pointer transition-all hover:border-slate-300 dark:hover:border-slate-700
                        ${className}
                    `}
                    {...props}
                />
            </div>
        </div>
    );
}
