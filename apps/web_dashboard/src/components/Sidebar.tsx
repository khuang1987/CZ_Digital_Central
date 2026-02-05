'use client';

import React, { useEffect, useState } from 'react';
import {
    LayoutDashboard, Activity, Box, Calendar, Settings, ChevronLeft, ChevronRight, User, MoreVertical, Filter, RotateCcw, ShieldAlert, Database, Truck, Zap, FileCheck, Anchor, ChevronDown, DraftingCompass, Factory
} from 'lucide-react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { useUI } from '@/context/UIContext';

// Define NavItem type for recursion
type NavItemType = {
    icon?: React.ReactNode;
    label: string;
    href?: string;
    disabled?: boolean;
    children?: NavItemType[];
};

export default function Sidebar() {
    const pathname = usePathname();
    const { isSidebarCollapsed, toggleSidebar, isFilterOpen, toggleFilter, resetFilters, hasFilters, setSidebarCollapsed } = useUI();

    // State to track expanded menus (by label)
    const [expandedMenus, setExpandedMenus] = useState<Record<string, boolean>>({});

    const isReportPage = pathname.includes('/production/');

    // Auto-collapse sidebar after 15 seconds on initial mount
    useEffect(() => {
        const timer = setTimeout(() => {
            setSidebarCollapsed(true);
        }, 15000);

        return () => clearTimeout(timer);
    }, [setSidebarCollapsed]);

    // Flattened NavItems (Level 1 Only)
    const navItems: { icon: React.ReactNode; label: string; href: string; disabled?: boolean }[] = [
        { icon: <LayoutDashboard size={18} />, label: "仪表盘 (Dashboard)", href: "/" },
        { icon: <ShieldAlert size={18} />, label: "安全 (EHS)", href: "/ehs" },
        { icon: <FileCheck size={18} />, label: "质量 (Quality)", href: "/certification/belts" },
        { icon: <Truck size={18} />, label: "交付 (Delivery)", href: "/delivery/batch-records" },
        { icon: <Zap size={18} />, label: "效率 (Efficiency)", href: "/efficiency/oee" },
        { icon: <Factory size={18} />, label: "生产 (Production)", href: "/production/labor-eh" },
        { icon: <Box size={18} />, label: "供应链 (Supply Chain)", href: "/inventory" },
        { icon: <DraftingCompass size={18} />, label: "工程 (Engineering)", href: "/engineering" },
        { icon: <Settings size={18} />, label: "设置 (Setting)", href: "/settings" },
    ];

    const isActive = (href: string) => {
        if (href === '/') return pathname === '/';

        // Special handling for Production module to include its tabs (Changeover, Schedule etc.)
        if (href === '/production/labor-eh') {
            return pathname.startsWith('/production');
        }

        // Strict prefix match for others
        return pathname.startsWith(href);
    };

    return (
        <aside
            className={`${isSidebarCollapsed ? 'w-20' : 'w-64'} border-r border-slate-200 dark:border-slate-800/50 flex flex-col py-6 px-4 backdrop-blur-md z-20 transition-all duration-300 relative shrink-0 bg-white/80 dark:bg-slate-950/90`}
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

            <nav className="flex-1 space-y-1 overflow-y-auto custom-scrollbar -mr-2 pr-2">
                {navItems.map((item) => (
                    <NavItem
                        key={item.label}
                        icon={item.icon}
                        label={item.label}
                        active={isActive(item.href)}
                        isCollapsed={isSidebarCollapsed}
                        href={item.href}
                        disabled={item.disabled}
                    />
                ))}
            </nav>

            {/* Internal Toolbar (Left Docked) */}
            <div className={`mt-auto mb-2 flex ${isSidebarCollapsed ? 'flex-col gap-4' : 'flex-row justify-around'} items-center p-2 rounded-xl transition-colors mx-2`}>
                <button
                    onClick={toggleSidebar}
                    className="p-2 text-slate-400 hover:text-medtronic hover:bg-slate-100 dark:hover:bg-slate-800/50 rounded-lg transition-all"
                    title={isSidebarCollapsed ? "Expand Sidebar" : "Collapse Sidebar"}
                >
                    {isSidebarCollapsed ? <ChevronRight size={16} /> : <ChevronLeft size={16} />}
                </button>

                {hasFilters && (
                    <>
                        <button
                            onClick={toggleFilter}
                            className={`p-2 rounded-lg transition-all ${isFilterOpen
                                ? 'text-medtronic bg-slate-100 dark:bg-slate-800/50'
                                : 'text-slate-400 hover:text-medtronic hover:bg-slate-100 dark:hover:bg-slate-800/50'}`}
                            title={isFilterOpen ? "Collapse Filters" : "Expand Filters"}
                        >
                            <Filter size={16} className={isFilterOpen ? 'fill-current opacity-20' : ''} />
                        </button>

                        <button
                            onClick={resetFilters}
                            className="p-2 text-slate-400 hover:text-orange-500 hover:bg-slate-100 dark:hover:bg-slate-800/50 rounded-lg transition-all group/reset"
                            title="Reset All Filters"
                        >
                            <RotateCcw size={16} className="group-hover/reset:rotate-[-180deg] transition-transform duration-500" />
                        </button>
                    </>
                )}
            </div>

            <div className={`p-4 flex items-center gap-3 rounded-2xl hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors cursor-pointer ${isSidebarCollapsed ? 'justify-center p-2' : ''}`}>
                <div className="w-10 h-10 rounded-full bg-slate-100 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-700 flex items-center justify-center text-slate-600 dark:text-slate-400 shrink-0">
                    <User size={20} />
                </div>
                {!isSidebarCollapsed && (
                    <div className="flex-1 truncate animate-in fade-in slide-in-from-left-2 duration-300">
                        <p className="text-xs font-bold truncate text-slate-700 dark:text-slate-200">Project Admin</p>
                        <p className="text-[10px] text-slate-500">CZ Ops Center</p>
                    </div>
                )}
                {!isSidebarCollapsed && <MoreVertical size={14} className="text-slate-400" />}
            </div>
        </aside>
    );
}

function NavItem({ icon, label, active, isCollapsed, href, disabled }: { icon: React.ReactNode, label: string, active: boolean, isCollapsed: boolean, href: string, disabled?: boolean }) {
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

    if (!disabled) {
        return <Link href={href} className="block w-full mb-1">{content}</Link>;
    }

    return <div className="w-full text-left mb-1">{content}</div>;
}
