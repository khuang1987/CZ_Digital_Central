'use client';

import React, { useEffect, useState } from 'react';
import {
    LayoutDashboard, Activity, Box, Calendar, Settings, ChevronLeft, ChevronRight, User, MoreVertical, Filter, RotateCcw, ShieldAlert, Database, Truck, Zap, FileCheck, Anchor, ChevronDown, DraftingCompass, Factory
} from 'lucide-react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { useUI } from '@/context/UIContext';
import { useAuth } from '@/context/AuthContext';
import { LogOut } from 'lucide-react';
import { t } from '@/lib/translations';

// Define NavItem type for recursion
interface NavItemDefinition {
    icon: React.ReactNode;
    label: string;
    href: string;
    disabled?: boolean;
}

export default function Sidebar() {
    const pathname = usePathname();
    const { user, logout } = useAuth();
    const { isSidebarCollapsed, toggleSidebar, isFilterOpen, toggleFilter, resetFilters, hasFilters, setSidebarCollapsed, language } = useUI();

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

    // NavItems with translation support
    const navItems: NavItemDefinition[] = [
        { icon: <LayoutDashboard size={18} />, label: t('dashboard', language), href: "/" },
        { icon: <ShieldAlert size={18} />, label: t('safety', language), href: "/ehs" },
        { icon: <FileCheck size={18} />, label: t('quality', language), href: "/quality" },
        { icon: <Truck size={18} />, label: t('delivery', language), href: "/delivery/wip" },
        { icon: <Zap size={18} />, label: t('efficiency', language), href: "/efficiency/oee" },
        { icon: <Factory size={18} />, label: t('production', language), href: "/production/labor-eh" },
        { icon: <Box size={18} />, label: t('supply_chain', language), href: "/inventory" },
        { icon: <DraftingCompass size={18} />, label: t('engineering', language), href: "/engineering" },
        { icon: <Settings size={18} />, label: t('settings', language), href: "/settings" },
        { icon: <Database size={18} />, label: t('docs', language), href: "/docs" },
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
                <div className="w-8 h-8 rounded-lg bg-white flex items-center justify-center p-1 shrink-0 shadow-lg shadow-black/5">
                    <img src="/art-symbol-rgb-full-color.svg" alt="Medtronic" className="w-full h-full object-contain" />
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

            <div className={`p-4 flex items-center gap-3 rounded-2xl hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors group/user ${isSidebarCollapsed ? 'justify-center p-2' : ''}`}>
                <div className="w-10 h-10 rounded-full bg-slate-100 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-700 flex items-center justify-center text-slate-600 dark:text-slate-400 shrink-0 relative overflow-hidden">
                    <User size={20} />
                    {user?.role === 'admin' && (
                        <div className="absolute inset-x-0 bottom-0 h-1 bg-blue-500" title="Admin User" />
                    )}
                </div>
                {!isSidebarCollapsed && (
                    <div className="flex-1 truncate animate-in fade-in slide-in-from-left-2 duration-300">
                        <p className="text-xs font-bold truncate text-slate-700 dark:text-slate-200">
                            {user?.username || 'Guest'}
                        </p>
                        <p className="text-[10px] text-slate-500 uppercase tracking-widest font-black">
                            {user?.role === 'admin' ? 'Administrator' : 'Standard User'}
                        </p>
                    </div>
                )}
                {!isSidebarCollapsed && (
                    <button
                        onClick={() => logout()}
                        className="p-1.5 hover:bg-red-500/10 hover:text-red-500 rounded-lg text-slate-400 transition-all active:scale-90"
                        title="Log Out"
                    >
                        <LogOut size={14} />
                    </button>
                )}
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
