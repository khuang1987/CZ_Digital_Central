'use client';

import React from 'react';
import { LayoutDashboard, Activity, AlertTriangle, Calendar, FileCheck, Truck, Zap, Box, ArrowRight, TrendingUp, TrendingDown, Users, ShieldCheck, Factory, ClipboardList } from 'lucide-react';
import StandardPageLayout from '@/components/StandardPageLayout';
import Link from 'next/link';
import { useUI } from '@/context/UIContext';
import { t } from '@/lib/translations';

export default function Home({ theme, toggleTheme }: { theme?: 'light' | 'dark', toggleTheme?: () => void }) {
  const { language } = useUI();

  return (
    <StandardPageLayout
      theme={theme}
      toggleTheme={toggleTheme}
      title={t('plant_overview', language)}
      description={t('plant_desc', language)}
      icon={<Factory size={24} />}
    >
      <div className="flex flex-col gap-6 h-[calc(100vh-280px)] min-h-[600px]">

        {/* 1. Factory Status Banner */}
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6 shrink-0">
          <div className="lg:col-span-3 p-5 bg-gradient-to-r from-slate-900 to-slate-800 text-white rounded-2xl shadow-sm flex items-center justify-between relative overflow-hidden group">
            <div className="absolute top-0 right-0 p-8 opacity-10 transform translate-x-4 -translate-y-4 group-hover:scale-105 transition-transform">
              <Factory size={120} />
            </div>
            <div className="z-10">
              <div className="flex items-center gap-2 mb-1">
                <span className="relative flex h-2 w-2">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                  <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
                </span>
                <h2 className="text-xs font-bold text-emerald-400 uppercase tracking-widest">{t('plant_status_nominal', language)}</h2>
              </div>
              <h1 className="text-2xl font-black tracking-tight mb-2">{t('shift_running', language)}</h1>
              <p className="text-sm text-slate-400 font-medium">{t('headcount', language)}: 142 / 145 (98%) • {t('next_changeover', language)}: 14:00 (Line 3)</p>
            </div>
            <div className="z-10 flex gap-8 pr-8">
              <div className="text-right">
                <div className="text-xs text-slate-400 font-bold uppercase">{t('safe_days', language)}</div>
                <div className="text-3xl font-black text-emerald-400">842</div>
              </div>
              <div className="text-right border-l border-slate-700 pl-8">
                <div className="text-xs text-slate-400 font-bold uppercase">{t('shift_output', language)}</div>
                <div className="text-3xl font-black text-white">1,250</div>
              </div>
            </div>
          </div>

          <div className="p-5 bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm flex flex-col justify-center">
            <h3 className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-3">{t('plant_health_oee', language)}</h3>
            <div className="flex items-baseline gap-2 mb-2">
              <span className="text-4xl font-black text-slate-800 dark:text-white">87.2%</span>
              <span className="text-xs font-bold text-emerald-500 flex items-center gap-1">
                <TrendingUp size={12} /> +2.4%
              </span>
            </div>
            <div className="w-full bg-slate-100 dark:bg-slate-800 rounded-full h-1.5 overflow-hidden">
              <div className="bg-gradient-to-r from-blue-500 to-cyan-400 h-full w-[87.2%]" />
            </div>
            <div className="flex justify-between mt-2 text-[10px] font-bold text-slate-400">
              <span>{t('target', language)}: 85%</span>
              <span>{t('world_class', language)}: 92%</span>
            </div>
          </div>
        </div>

        {/* 2. SQDCP Bento Grid */}
        <div className="flex-1 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-6 min-h-0">

          {/* S - Safety */}
          <Link href="/ehs" className="group flex flex-col bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm hover:border-emerald-500/50 hover:shadow-lg hover:shadow-emerald-500/10 transition-all overflow-hidden relative">
            <div className="absolute top-0 right-0 p-3 opacity-[0.03] group-hover:opacity-[0.07] transition-opacity">
              <ShieldCheck size={100} />
            </div>
            <div className="p-5 border-b border-slate-100 dark:border-slate-800">
              <div className="flex items-center gap-2 mb-2">
                <div className="p-1.5 bg-emerald-100 dark:bg-emerald-900/30 rounded text-emerald-600 dark:text-emerald-400">
                  <AlertTriangle size={16} />
                </div>
                <h3 className="font-black text-slate-700 dark:text-white">{t('safety_upper', language)}</h3>
              </div>
              <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">{t('zero_harm', language)}</div>
            </div>
            <div className="flex-1 p-5 flex flex-col justify-between">
              <div>
                <div className="flex justify-between items-end mb-1">
                  <span className="text-xs font-bold text-slate-500">{t('trir_ytd', language)}</span>
                  <span className="text-xl font-black text-slate-800 dark:text-white">0.12</span>
                </div>
                <div className="w-full bg-slate-100 dark:bg-slate-800 h-1 mb-4 rounded-full">
                  <div className="bg-emerald-500 h-full w-[12%]" />
                </div>
                <div className="space-y-2">
                  <div className="flex justify-between text-xs">
                    <span className="text-slate-500 font-medium">{t('first_aid', language)}</span>
                    <span className="font-bold text-slate-700 dark:text-slate-300">2</span>
                  </div>
                  <div className="flex justify-between text-xs">
                    <span className="text-slate-500 font-medium">{t('near_miss', language)}</span>
                    <span className="font-bold text-slate-700 dark:text-slate-300">14</span>
                  </div>
                </div>
              </div>
              <div className="mt-4 pt-3 border-t border-dashed border-slate-100 dark:border-slate-800 flex items-center justify-between text-[10px] font-bold text-emerald-500 uppercase tracking-wider group-hover:text-emerald-600 transition-colors">
                <span>{t('view_dashboard', language)}</span>
                <ArrowRight size={12} />
              </div>
            </div>
          </Link>

          {/* Q - Quality */}
          <Link href="/quality/nc" className="group flex flex-col bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm hover:border-blue-500/50 hover:shadow-lg hover:shadow-blue-500/10 transition-all overflow-hidden relative">
            <div className="absolute top-0 right-0 p-3 opacity-[0.03] group-hover:opacity-[0.07] transition-opacity">
              <FileCheck size={100} />
            </div>
            <div className="p-5 border-b border-slate-100 dark:border-slate-800">
              <div className="flex items-center gap-2 mb-2">
                <div className="p-1.5 bg-blue-100 dark:bg-blue-900/30 rounded text-blue-600 dark:text-blue-400">
                  <FileCheck size={16} />
                </div>
                <h3 className="font-black text-slate-700 dark:text-white">{t('quality_upper', language)}</h3>
              </div>
              <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">{t('first_pass_yield', language)}</div>
            </div>
            <div className="flex-1 p-5 flex flex-col justify-between">
              <div>
                <div className="flex justify-between items-end mb-1">
                  <span className="text-xs font-bold text-slate-500">{t('fpy_rolling', language)}</span>
                  <span className="text-xl font-black text-slate-800 dark:text-white">98.5%</span>
                </div>
                {/* Simulated Sparkline */}
                <div className="flex items-end gap-0.5 h-6 mb-4 opacity-50">
                  {[40, 60, 55, 70, 65, 80, 75, 90, 85, 95].map((h, i) => (
                    <div key={i} className="flex-1 bg-blue-500 rounded-t-sm" style={{ height: `${h}%` }} />
                  ))}
                </div>
                <div className="space-y-2">
                  <div className="flex justify-between text-xs">
                    <span className="text-slate-500 font-medium">{t('scrap_cost', language)}</span>
                    <span className="font-bold text-red-500">$420</span>
                  </div>
                  <div className="flex justify-between text-xs">
                    <span className="text-slate-500 font-medium">{t('non_conformance', language)}</span>
                    <span className="font-bold text-amber-500">3</span>
                  </div>
                </div>
              </div>
              <div className="mt-4 pt-3 border-t border-dashed border-slate-100 dark:border-slate-800 flex items-center justify-between text-[10px] font-bold text-blue-500 uppercase tracking-wider group-hover:text-blue-600 transition-colors">
                <span>{t('metric_details', language)}</span>
                <ArrowRight size={12} />
              </div>
            </div>
          </Link>

          {/* D - Delivery */}
          <Link href="/delivery/batch-records" className="group flex flex-col bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm hover:border-indigo-500/50 hover:shadow-lg hover:shadow-indigo-500/10 transition-all overflow-hidden relative">
            <div className="absolute top-0 right-0 p-3 opacity-[0.03] group-hover:opacity-[0.07] transition-opacity">
              <Truck size={100} />
            </div>
            <div className="p-5 border-b border-slate-100 dark:border-slate-800">
              <div className="flex items-center gap-2 mb-2">
                <div className="p-1.5 bg-indigo-100 dark:bg-indigo-900/30 rounded text-indigo-600 dark:text-indigo-400">
                  <Truck size={16} />
                </div>
                <h3 className="font-black text-slate-700 dark:text-white">{t('delivery_upper', language)}</h3>
              </div>
              <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">{t('schedule_attainment', language)}</div>
            </div>
            <div className="flex-1 p-5 flex flex-col justify-between">
              <div>
                <div className="flex justify-between items-end mb-1">
                  <span className="text-xs font-bold text-slate-500">{t('otp_weekly', language)}</span>
                  <span className="text-xl font-black text-amber-500">94.2%</span>
                </div>
                <div className="w-full bg-slate-100 dark:bg-slate-800 h-1 mb-4 rounded-full overflow-hidden">
                  <div className="bg-amber-500 h-full w-[94.2%]" />
                </div>
                <div className="space-y-2">
                  <div className="flex justify-between text-xs">
                    <span className="text-slate-500 font-medium">{t('batch_release', language)}</span>
                    <span className="font-bold text-slate-700 dark:text-slate-300">Avg 4.2h</span>
                  </div>
                  <div className="flex justify-between text-xs">
                    <span className="text-slate-500 font-medium">{t('backlog', language)}</span>
                    <span className="font-bold text-slate-700 dark:text-slate-300">120 Units</span>
                  </div>
                </div>
              </div>
              <div className="mt-4 pt-3 border-t border-dashed border-slate-100 dark:border-slate-800 flex items-center justify-between text-[10px] font-bold text-indigo-500 uppercase tracking-wider group-hover:text-indigo-600 transition-colors">
                <span>{t('view_logistics', language)}</span>
                <ArrowRight size={12} />
              </div>
            </div>
          </Link>

          {/* C - Cost / Inventory */}
          <Link href="/inventory" className="group flex flex-col bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm hover:border-purple-500/50 hover:shadow-lg hover:shadow-purple-500/10 transition-all overflow-hidden relative">
            <div className="absolute top-0 right-0 p-3 opacity-[0.03] group-hover:opacity-[0.07] transition-opacity">
              <Box size={100} />
            </div>
            <div className="p-5 border-b border-slate-100 dark:border-slate-800">
              <div className="flex items-center gap-2 mb-2">
                <div className="p-1.5 bg-purple-100 dark:bg-purple-900/30 rounded text-purple-600 dark:text-purple-400">
                  <Box size={16} />
                </div>
                <h3 className="font-black text-slate-700 dark:text-white">{t('cost_inv_upper', language)}</h3>
              </div>
              <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">{t('inventory_health', language)}</div>
            </div>
            <div className="flex-1 p-5 flex flex-col justify-between">
              <div>
                <div className="flex justify-between items-end mb-1">
                  <span className="text-xs font-bold text-slate-500">{t('wip_value', language)}</span>
                  <span className="text-xl font-black text-slate-800 dark:text-white">$1.2M</span>
                </div>
                <div className="flex items-center gap-1 text-[10px] font-bold text-emerald-500 mb-4">
                  <TrendingDown size={10} /> -4.5% vs Last Month
                </div>
                <div className="space-y-2">
                  <div className="flex justify-between text-xs">
                    <span className="text-slate-500 font-medium">{t('turns_annual', language)}</span>
                    <span className="font-bold text-slate-700 dark:text-slate-300">12.4</span>
                  </div>
                  <div className="flex justify-between text-xs">
                    <span className="text-slate-500 font-medium">{t('crit_shortages', language)}</span>
                    <span className="font-bold text-emerald-500">0</span>
                  </div>
                </div>
              </div>
              <div className="mt-4 pt-3 border-t border-dashed border-slate-100 dark:border-slate-800 flex items-center justify-between text-[10px] font-bold text-purple-500 uppercase tracking-wider group-hover:text-purple-600 transition-colors">
                <span>{t('view_inventory', language)}</span>
                <ArrowRight size={12} />
              </div>
            </div>
          </Link>

          {/* P - People / Production */}
          <Link href="/production/labor-eh" className="group flex flex-col bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm hover:border-cyan-500/50 hover:shadow-lg hover:shadow-cyan-500/10 transition-all overflow-hidden relative">
            <div className="absolute top-0 right-0 p-3 opacity-[0.03] group-hover:opacity-[0.07] transition-opacity">
              <Users size={100} />
            </div>
            <div className="p-5 border-b border-slate-100 dark:border-slate-800">
              <div className="flex items-center gap-2 mb-2">
                <div className="p-1.5 bg-cyan-100 dark:bg-cyan-900/30 rounded text-cyan-600 dark:text-cyan-400">
                  <Users size={16} />
                </div>
                <h3 className="font-black text-slate-700 dark:text-white">{t('people_upper', language)}</h3>
              </div>
              <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">{t('efficiency_labor', language)}</div>
            </div>
            <div className="flex-1 p-5 flex flex-col justify-between">
              <div>
                <div className="flex justify-between items-end mb-1">
                  <span className="text-xs font-bold text-slate-500">{t('eff_eh', language)}</span>
                  <span className="text-xl font-black text-slate-800 dark:text-white">102%</span>
                </div>
                <div className="w-full bg-slate-100 dark:bg-slate-800 h-1 mb-4 rounded-full overflow-hidden">
                  <div className="bg-cyan-500 h-full w-[100%]" />
                </div>
                <div className="space-y-2">
                  <div className="flex justify-between text-xs">
                    <span className="text-slate-500 font-medium">{t('headcount', language)}</span>
                    <span className="font-bold text-slate-700 dark:text-slate-300">142</span>
                  </div>
                  <div className="flex justify-between text-xs">
                    <span className="text-slate-500 font-medium">{t('training', language)}</span>
                    <span className="font-bold text-slate-700 dark:text-slate-300">99.2%</span>
                  </div>
                </div>
              </div>
              <div className="mt-4 pt-3 border-t border-dashed border-slate-100 dark:border-slate-800 flex items-center justify-between text-[10px] font-bold text-cyan-500 uppercase tracking-wider group-hover:text-cyan-600 transition-colors">
                <span>{t('labor_details', language)}</span>
                <ArrowRight size={12} />
              </div>
            </div>
          </Link>
        </div>
      </div>
    </StandardPageLayout>
  );
}
