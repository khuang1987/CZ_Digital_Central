'use client';

import React, { useEffect } from 'react';
import Link from 'next/link';
import FilterPanel from './FilterPanel';
import GlobalHeaderActions from './GlobalHeaderActions';
import { useUI } from '@/context/UIContext';

// Defines the tabs shown in the 2nd row header
export interface PageTab {
    label: string;
    href: string;
    icon?: React.ReactNode;
    active?: boolean;
    disabled?: boolean;
}

interface StandardPageLayoutProps {
    title: string;          // Simplified to string for new design
    description?: string;   // New subtitle
    icon?: React.ReactNode; // New header icon
    children: React.ReactNode;
    filters?: React.ReactNode;
    tabs?: PageTab[];
    actions?: React.ReactNode;
    theme?: 'light' | 'dark';
    toggleTheme?: () => void;
    onReset?: () => void;
}

export default function StandardPageLayout({ title, description, icon, children, filters, tabs = [], actions, theme, toggleTheme, onReset }: StandardPageLayoutProps) {
    const { setHasFilters, setResetHandler } = useUI();

    useEffect(() => {
        setHasFilters(!!filters);
        if (onReset) setResetHandler(() => onReset);

        return () => {
            setHasFilters(false);
            setResetHandler(() => { });
        };
    }, [filters, onReset, setHasFilters, setResetHandler]);

    return (
        <div className="flex flex-col h-full bg-[#f8fafc] dark:bg-[#020617] overflow-hidden">

            {/* 1. Standard Header (Two Rows) */}
            <header className="shrink-0 bg-white dark:bg-slate-950 border-b border-slate-200 dark:border-slate-800 shadow-sm z-10 w-full">
                {/* Row 1: Title & Actions */}
                <div className="h-16 px-6 flex items-center justify-between">
                    <div className="flex items-center gap-3">
                        {icon && (
                            <div className="p-2 bg-slate-100 dark:bg-slate-800 rounded-lg text-medtronic">
                                {icon}
                            </div>
                        )}
                        <div>
                            <h1 className="text-lg font-black tracking-tight text-slate-900 dark:text-white leading-tight">
                                {title}
                            </h1>
                            {description && (
                                <p className="text-xs font-medium text-slate-500 dark:text-slate-400">
                                    {description}
                                </p>
                            )}
                        </div>
                    </div>
                    <div className="flex items-center gap-3">
                        {actions}
                        <div className="hidden md:block w-[1px] h-5 bg-slate-200 dark:bg-slate-700 mx-2" />
                        <GlobalHeaderActions showExport={false} />
                    </div>
                </div>

                {/* Row 2: Tabs (Optional) */}
                {tabs.length > 0 && (
                    <div className="px-6 flex items-center gap-1 overflow-x-auto no-scrollbar border-t border-slate-100 dark:border-slate-900 bg-slate-50/50 dark:bg-slate-950/50">
                        {tabs.map((tab) => (
                            <Link
                                key={tab.href}
                                href={tab.disabled ? '#' : tab.href}
                                className={`
                                    relative flex items-center gap-2 px-4 py-3 text-xs font-bold transition-all border-b-[3px]
                                    ${tab.active
                                        ? 'border-[#002554] text-[#002554] bg-blue-50/30'
                                        : 'border-transparent text-slate-500 hover:text-slate-800 dark:hover:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-900'
                                    }
                                    ${tab.disabled ? 'opacity-40 cursor-not-allowed' : ''}
                                `}
                            >
                                {tab.icon}
                                <span>{tab.label}</span>
                            </Link>
                        ))}
                    </div>
                )}
            </header>

            {/* 2. Main Layout (Sidebar + Content) */}
            <div className="flex flex-1 min-h-0 overflow-hidden relative">

                {/* Adsorbed Filter Panel (Left) */}
                {filters && (
                    <FilterPanel title="Filters">
                        {filters}
                    </FilterPanel>
                )}

                {/* Main Content (Right) */}
                <main className="flex-1 overflow-y-auto overflow-x-hidden p-6 relative">
                    <div className="max-w-[1920px] mx-auto min-h-full">
                        {children}
                    </div>
                </main>

            </div>
        </div>
    );
}
