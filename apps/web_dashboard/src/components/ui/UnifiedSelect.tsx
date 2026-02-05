'use client';

import React from 'react';

interface UnifiedSelectProps extends React.SelectHTMLAttributes<HTMLSelectElement> {
    label?: string;
    icon?: React.ReactNode;
}

export function UnifiedSelect({ label, icon, className = '', children, ...props }: UnifiedSelectProps) {
    return (
        <div className="w-full">
            {label && (
                <label className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-1.5 block ml-1">
                    {label}
                </label>
            )}
            <div className="relative group">
                {icon && (
                    <div className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400 group-focus-within:text-medtronic transition-colors pointer-events-none">
                        {icon}
                    </div>
                )}
                <select
                    className={`
                        w-full appearance-none bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-xl
                        py-2.5 px-3 pr-8 ${icon ? 'pl-9' : ''}
                        text-xs font-bold text-slate-700 dark:text-slate-200
                        outline-none focus:ring-1 focus:ring-medtronic focus:border-medtronic
                        cursor-pointer transition-all hover:border-slate-300 dark:hover:border-slate-700
                        ${className}
                    `}
                    {...props}
                >
                    {children}
                </select>
                <div className="absolute right-3 top-1/2 -translate-y-1/2 pointer-events-none text-slate-400">
                    <svg width="10" height="6" viewBox="0 0 10 6" fill="none" xmlns="http://www.w3.org/2000/svg">
                        <path d="M1 1L5 5L9 1" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                    </svg>
                </div>
            </div>
        </div>
    );
}
