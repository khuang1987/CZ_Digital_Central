'use client';

import React from 'react';
import {
    LayoutDashboard, Activity, Box, Calendar, Settings, ChevronLeft, ChevronRight, User, MoreVertical, Filter, RotateCcw, ShieldAlert
} from 'lucide-react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { useUI } from '@/context/UIContext';

export default function Sidebar() {
    const pathname = usePathname();
    const { isSidebarCollapsed, toggleSidebar, isFilterOpen, toggleFilter, resetFilters } = useUI();

    const isReportPage = pathname.includes('/production/');

    const navItems = [
        { icon: <LayoutDashboard size={18} />, label: "Dashboard", href: "/" },
        { icon: <Activity size={18} />, label: "Production", href: "/production/labor-eh" },
        { icon: <ShieldAlert size={18} />, label: "EHS Safety", href: "/production/ehs" },
        { icon: <Box size={18} />, label: "Inventory", href: "/inventory", disabled: true },
        { icon: <Calendar size={18} />, label: "Schedule", href: "/schedule", disabled: true },
        { icon: <Settings size={18} />, label: "Settings", href: "/settings", disabled: true },
    ];

    const isActive = (href: string) => {
        if (href === '/') return pathname === '/';
        // Use exact check or specific prefix check to avoid highlighting multiple items in /production/
        return pathname === href;
    };

    return (
        <aside
            className={`${isSidebarCollapsed ? 'w-20' : 'w-64'} border-r border-[var(--border)] flex flex-col py-6 px-4 backdrop-blur-sm z-20 transition-all duration-300 relative shrink-0`}
            style={{ backgroundColor: 'color-mix(in srgb, var(--card), transparent 50%)' }}
        >


            <div className={`flex items-center gap-3 px-2 mb-10 overflow-hidden whitespace-nowrap`}>
                <div className="w-8 h-8 rounded-lg bg-medtronic flex items-center justify-center text-white shrink-0 shadow-lg shadow-blue-500/20">
                    <Activity size={20} />
                </div>
                {!isSidebarCollapsed && (
                    <div className="animate-in fade-in slide-in-from-left-2 duration-300">
                        <h1 className="text-sm font-bold tracking-tight text-medtronic">Medtronic</h1>
                        <p className="text-[10px] font-semibold text-slate-500 uppercase tracking-wider">CZ Digital Central</p>
                    </div>
                )}
            </div>

            <nav className="flex-1 space-y-1">
                {navItems.map((item) => (
                    <NavItem
                        key={item.label}
                        icon={item.icon}
                        label={item.label}
                        active={isActive(item.href)}
                        isCollapsed={isSidebarCollapsed}
                        href={item.disabled ? undefined : item.href}
                        disabled={item.disabled}
                    />
                ))}

                {/* Redundant Filter button removed from nav menu per user request */}
            </nav>

            {/* Internal Toolbar (Left Docked) */}
            <div className={`mt-auto mb-4 flex ${isSidebarCollapsed ? 'flex-col gap-4' : 'flex-row justify-around'} items-center p-2 rounded-xl bg-slate-50/50 dark:bg-slate-800/50 border border-slate-100 dark:border-slate-800/50 mx-2`}>
                {/* 1. Sidebar Toggle */}
                <button
                    onClick={toggleSidebar}
                    className="p-2 text-slate-400 hover:text-medtronic hover:bg-white dark:hover:bg-slate-800 rounded-lg transition-all shadow-sm hover:shadow"
                    title={isSidebarCollapsed ? "Expand Sidebar" : "Collapse Sidebar"}
                >
                    {isSidebarCollapsed ? <ChevronRight size={16} /> : <ChevronLeft size={16} />}
                </button>

                {/* 2. Filter Toggle */}
                {isReportPage && (
                    <button
                        onClick={toggleFilter}
                        className={`p-2 rounded-lg transition-all shadow-sm hover:shadow ${isFilterOpen
                            ? 'text-medtronic bg-white dark:bg-slate-800'
                            : 'text-slate-400 hover:text-medtronic hover:bg-white dark:hover:bg-slate-800'}`}
                        title={isFilterOpen ? "Collapse Filters" : "Expand Filters"}
                    >
                        <Filter size={16} className={isFilterOpen ? 'animate-pulse' : ''} />
                    </button>
                )}

                {/* 3. Global Reset */}
                {isReportPage && (
                    <button
                        onClick={resetFilters}
                        className="p-2 text-slate-400 hover:text-orange-500 hover:bg-white dark:hover:bg-slate-800 rounded-lg transition-all shadow-sm hover:shadow group/reset"
                        title="Reset All Filters"
                    >
                        <RotateCcw size={16} className="group-hover/reset:rotate-[-180deg] transition-transform duration-500" />
                    </button>
                )}
            </div>

            <div className={`ios-widget p-4 flex items-center gap-3 ${isSidebarCollapsed ? 'justify-center p-2' : ''}`}>
                <div className="w-10 h-10 rounded-full bg-slate-200 dark:bg-slate-800 flex items-center justify-center text-slate-600 dark:text-slate-400 shrink-0">
                    <User size={20} />
                </div>
                {!isSidebarCollapsed && (
                    <div className="flex-1 truncate animate-in fade-in slide-in-from-left-2 duration-300">
                        <p className="text-xs font-bold truncate">Project Admin</p>
                        <p className="text-[10px] text-slate-500">CZ Ops Center</p>
                    </div>
                )}
                {!isSidebarCollapsed && <MoreVertical size={14} className="text-slate-400" />}
            </div>
        </aside>
    );
}

function NavItem({ icon, label, active, isCollapsed, href, disabled }: { icon: React.ReactNode, label: string, active: boolean, isCollapsed: boolean, href?: string, disabled?: boolean }) {
    const content = (
        <div
            title={isCollapsed ? label : ''}
            className={`w-full flex items-center gap-3 px-4 py-3 rounded-2xl transition-all active:scale-95 group ${active
                ? 'ios-widget-active font-bold shadow-md scale-[1.02]'
                : 'text-slate-600 dark:text-slate-400 hover:bg-slate-50 dark:hover:bg-slate-800/50 hover:text-slate-900 dark:hover:text-slate-200'
                } ${isCollapsed ? 'justify-center px-0' : ''} ${disabled ? 'opacity-30 cursor-not-allowed' : 'cursor-pointer'}`}
        >
            <div className={`${active ? 'text-white' : 'text-slate-400 transition-colors group-hover:text-slate-600'} shrink-0`}>
                {icon}
            </div>
            {!isCollapsed && <span className="text-xs animate-in fade-in slide-in-from-left-2 duration-300">{label}</span>}
        </div>
    );

    if (href && !disabled) {
        return <Link href={href} className="block w-full">{content}</Link>;
    }

    return <div className="w-full text-left">{content}</div>;
}
