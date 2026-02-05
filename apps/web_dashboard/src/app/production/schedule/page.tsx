'use client';

import React from 'react';
import Link from 'next/link';
import { ShieldCheck, ArrowRight, Calendar, GanttChart, Activity, Zap, BarChart3, Package } from 'lucide-react';
import StandardPageLayout, { PageTab } from '@/components/StandardPageLayout';

export default function SchedulePage() {
    const productionTabs: PageTab[] = [
        { label: '工时分析 (Labor EH)', href: '/production/labor-eh', icon: <Activity size={14} /> },
        { label: '调试换型 (Changeover)', href: '/production/changeover', icon: <Zap size={14} /> },
        { label: '排程 (Schedule)', href: '/production/schedule', icon: <Calendar size={14} />, active: true },
        { label: 'OEE 看板', href: '/production/oee', icon: <BarChart3 size={14} />, disabled: true },
        { label: '物料查询', href: '/production/material', icon: <Package size={14} />, disabled: true },
    ];

    return (
        <StandardPageLayout
            title="Production Schedule Center"
            description="Manage production validation and planning."
            icon={<Calendar size={24} />}
            tabs={productionTabs}
        >
            <div className="space-y-8 animate-in fade-in slide-in-from-bottom-4 duration-500 min-h-full">
                <div className="p-6 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl shadow-sm h-full">
                    {/* Inner header removed as it is now in the main header */}

                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                        {/* Batch Validation Card */}
                        <Link href="/production/schedule/validation">
                            <div className="group p-6 bg-slate-50 dark:bg-slate-800/50 rounded-xl border border-slate-200 dark:border-slate-700 hover:border-medtronic hover:shadow-lg hover:shadow-blue-500/10 transition-all cursor-pointer relative overflow-hidden">
                                <div className="absolute top-0 right-0 p-4 opacity-10 group-hover:opacity-20 transition-opacity">
                                    <ShieldCheck size={64} className="text-medtronic" />
                                </div>

                                <div className="relative z-10">
                                    <div className="w-10 h-10 rounded-lg bg-white dark:bg-slate-800 flex items-center justify-center text-medtronic shadow-sm mb-4 group-hover:scale-110 transition-transform">
                                        <ShieldCheck size={20} />
                                    </div>
                                    <h3 className="text-base font-bold text-slate-900 dark:text-white mb-2">Batch Validation</h3>
                                    <p className="text-xs text-slate-500 dark:text-slate-400 leading-relaxed mb-4">
                                        Compare SQL metrics with client-side logic to validate production data accuracy.
                                    </p>
                                    <div className="flex items-center text-xs font-bold text-medtronic gap-1 group-hover:gap-2 transition-all">
                                        <span>Enter Module</span>
                                        <ArrowRight size={12} />
                                    </div>
                                </div>
                            </div>
                        </Link>

                        {/* Placeholder: Gantt Chart */}
                        <div className="group p-6 bg-slate-50 dark:bg-slate-800/50 rounded-xl border border-dashed border-slate-300 dark:border-slate-700 opacity-60">
                            <div className="w-10 h-10 rounded-lg bg-slate-200 dark:bg-slate-700 flex items-center justify-center text-slate-400 mb-4">
                                <GanttChart size={20} />
                            </div>
                            <h3 className="text-base font-bold text-slate-500 dark:text-slate-400 mb-2">Gantt Schedule</h3>
                            <p className="text-xs text-slate-400 leading-relaxed mb-4">
                                Detailed visual production timeline and resource planning.
                            </p>
                            <div className="inline-flex items-center px-2 py-1 rounded bg-slate-200 dark:bg-slate-700 text-[10px] font-bold text-slate-500">
                                Coming Soon
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </StandardPageLayout>
    );
}
