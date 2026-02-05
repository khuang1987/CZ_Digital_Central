'use client';

import React, { useState } from 'react';
import { Filter } from 'lucide-react';
import { useUI } from '@/context/UIContext';

interface FilterPanelProps {
    children: React.ReactNode;
    title?: string;
}

export default function FilterPanel({ children, title = 'Filters' }: FilterPanelProps) {
    const { isFilterOpen } = useUI();
    // isFilterOpen comes from Context, toggled by Sidebar

    return (
        <div className="flex h-full border-r border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-950 shrink-0 z-20 shadow-[4px_0_24px_-12px_rgba(0,0,0,0.1)] transition-all">

            {/* Sliding Content Drawer */}
            <div
                className={`
                    flex flex-col overflow-hidden transition-all duration-300 ease-[cubic-bezier(0.4,0,0.2,1)] bg-white dark:bg-slate-950
                    ${!isFilterOpen ? 'w-0 opacity-0' : 'w-[280px] opacity-100'}
                `}
            >
                <div className="flex-1 overflow-y-auto custom-scrollbar p-5 min-w-[280px]">
                    {/* Title */}
                    {title && (
                        <div className="mb-6 pb-4 border-b border-slate-100 dark:border-slate-900 flex items-center justify-between">
                            <h3 className="text-xs font-black uppercase tracking-widest text-slate-400 dark:text-slate-500">
                                {title}
                            </h3>
                        </div>
                    )}

                    <div className="space-y-6 animate-in fade-in slide-in-from-left-4 duration-500 fill-mode-backwards delay-75">
                        {children}
                    </div>
                </div>
            </div>
        </div>
    );
}
