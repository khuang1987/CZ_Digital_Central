'use client';

import React, { useState, useEffect } from 'react';
import Sidebar from '@/components/Sidebar';
import { Search, Moon, Sun, Bell, ExternalLink, Activity, Zap, BarChart, Package, ShieldCheck, Maximize } from 'lucide-react';
import { usePathname } from 'next/navigation';
import Link from 'next/link';
import { useUI } from '@/context/UIContext';

export default function AppShell({ children }: { children: React.ReactNode }) {
    const [theme, setTheme] = useState<'light' | 'dark'>('light');
    // const { isSidebarCollapsed } = useUI(); // Unused
    const pathname = usePathname();

    useEffect(() => {
        const savedTheme = localStorage.getItem('theme') as 'light' | 'dark';
        if (savedTheme) {
            setTheme(savedTheme);
        } else if (window.matchMedia('(prefers-color-scheme: dark)').matches) {
            setTheme('dark');
        }
    }, []);

    useEffect(() => {
        document.documentElement.setAttribute('data-theme', theme);
        if (theme === 'dark') {
            document.documentElement.classList.add('dark');
        } else {
            document.documentElement.classList.remove('dark');
        }
        localStorage.setItem('theme', theme);
    }, [theme]);

    const toggleTheme = () => setTheme(prev => prev === 'light' ? 'dark' : 'light');

    const isProduction = pathname.startsWith('/production');

    const getPageTitle = () => {
        if (pathname === '/') return 'Executive Dashboard';
        if (pathname.includes('/production/labor-eh')) return 'Labor EH Analysis';
        if (pathname.includes('/production/changeover')) return 'Setup & Changeover Report';
        if (pathname.includes('/production/ehs')) return (
            <div className="flex items-center gap-2">
                <ShieldCheck className="text-emerald-500" size={24} />
                <span className="text-xl font-black tracking-tight text-slate-900 dark:text-white">EHS Safety Center</span>
            </div>
        );
        if (isProduction) return 'Production Management';
        return 'Medtronic Digital Central';
    };

    const productionTabs = [
        { label: '工时分析 (Labor EH)', href: '/production/labor-eh', icon: <Activity size={14} /> },
        { label: '调试换型 (Changeover)', href: '/production/changeover', icon: <Zap size={14} /> },
        { label: 'OEE 看板', href: '/production/oee', icon: <BarChart size={14} />, disabled: true },
        { label: '物料查询', href: '/production/material', icon: <Package size={14} />, disabled: true },
    ];

    return (
        <div className="flex bg-[var(--background)] h-screen w-full overflow-hidden transition-colors duration-300">
            <Sidebar />

            <main className="flex-1 flex flex-col min-w-0">
                <header className="glass-header flex flex-col shrink-0 px-8 border-b border-slate-200 dark:border-slate-800">
                    <div className="h-12 flex items-center justify-between">
                        <div className="text-xl font-black tracking-tight text-slate-900 dark:text-white transition-all">
                            {getPageTitle()}
                        </div>

                        <div className="flex items-center gap-3">
                            <div className="relative hidden md:block w-64 mr-2">
                                <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500" size={14} />
                                <input
                                    type="text"
                                    placeholder="搜索分析数据..."
                                    className="pl-9 pr-4 py-1.5 bg-slate-100/80 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-full text-[11px] font-bold outline-none focus:ring-2 focus:ring-medtronic w-full transition-all text-slate-900 dark:text-slate-100"
                                />
                            </div>

                            <button
                                onClick={toggleTheme}
                                className="p-2 rounded-full hover:bg-slate-100 dark:hover:bg-slate-800 transition-all text-slate-600 dark:text-slate-400"
                                title="切换主题"
                            >
                                {theme === 'light' ? <Moon size={18} /> : <Sun size={18} />}
                            </button>

                            <div className="w-[1px] h-5 bg-slate-200 dark:bg-slate-700 mx-1" />

                            <button className="p-2 rounded-full hover:bg-slate-100 dark:hover:bg-slate-800 transition-all text-slate-600 dark:text-slate-400 relative">
                                <Bell size={18} />
                                <span className="absolute top-1.5 right-1.5 w-1.5 h-1.5 bg-red-500 rounded-full border border-white dark:border-slate-900" />
                            </button>

                            <button
                                onClick={() => {
                                    if (!document.fullscreenElement) {
                                        document.documentElement.requestFullscreen();
                                    } else {
                                        document.exitFullscreen();
                                    }
                                }}
                                className="p-2 rounded-full hover:bg-slate-100 dark:hover:bg-slate-800 transition-all text-slate-600 dark:text-slate-400 mr-2"
                                title="全屏显示"
                            >
                                <Maximize size={18} />
                            </button>

                            <button className="flex items-center gap-2 ml-2 px-4 py-1.5 rounded-full bg-medtronic text-white text-[11px] font-black hover:brightness-110 transition-all shadow-sm active:scale-95">
                                <span className="hidden sm:inline">导出报告</span>
                                <ExternalLink size={12} />
                            </button>
                        </div>
                    </div>

                    {isProduction && !pathname.includes('/production/ehs') && (
                        <nav className="h-8 flex items-center gap-5 -mb-[1px]">
                            {productionTabs.map((tab) => {
                                const active = pathname === tab.href;
                                return (
                                    <Link
                                        key={tab.label}
                                        href={tab.disabled ? '#' : tab.href}
                                        className={`flex items-center gap-2 h-full text-[11px] font-black transition-all border-b-2 relative ${active
                                            ? 'border-medtronic text-medtronic'
                                            : 'border-transparent text-slate-800 dark:text-slate-400 hover:text-medtronic dark:hover:text-slate-200'
                                            } ${tab.disabled ? 'opacity-30 cursor-not-allowed' : 'cursor-pointer'}`}
                                    >
                                        {tab.icon}
                                        <span>{tab.label}</span>
                                    </Link>
                                );
                            })}
                        </nav>
                    )}
                </header>

                <div className="flex-1 overflow-hidden relative">
                    {children}
                </div>
            </main>
        </div>
    );
}
