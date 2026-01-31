'use client';

import React from 'react';
import {
  ChevronUp, ChevronDown, Activity, AlertTriangle, Calendar
} from 'lucide-react';

export default function Home() {
  return (
    <div className="p-8 overflow-y-auto w-full h-full max-w-[1920px] mx-auto space-y-6">
      {/* KPI Grid */}
      <section className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-6">
        <KpiWidget title="Yield (Last 30 Days)" value="98.5%" trend="+1.2%" status="success" />
        <KpiWidget title="WIP (Work in Progress)" value="1,250 Units" trend="0% change" status="neutral" />
        <KpiWidget title="OEE (Overall Efficiency)" value="87.2%" trend="+2.5%" status="success" />
        <KpiWidget title="Planned downtime" value="4.2h" trend="-0.5h" status="warning" />
      </section>

      {/* Middle Section: Main Trend + Alerts */}
      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6 h-auto xl:h-[400px]">
        <article className="xl:col-span-2 ios-widget p-6 flex flex-col min-w-0">
          <div className="flex justify-between items-center mb-6">
            <div>
              <h3 className="font-bold text-sm">Production Trends (Daily Output)</h3>
              <p className="text-[10px] text-slate-500 font-bold uppercase tracking-wider">ROLLING 30-DAY PERFORMANCE</p>
            </div>
            <div className="flex gap-2">
              <select className="bg-slate-100 dark:bg-slate-800 px-3 py-1 rounded-lg text-[10px] font-bold outline-none border-none cursor-pointer">
                <option>Last 30 Days</option>
                <option>Year to Date</option>
              </select>
            </div>
          </div>
          <div className="flex-1 min-h-[250px] bg-slate-50 dark:bg-slate-900/50 rounded-2xl border border-dashed border-[var(--border)] flex items-center justify-center relative group overflow-hidden">
            <div className="text-slate-400 group-hover:text-medtronic transition-colors flex flex-col items-center gap-2">
              <Activity size={32} />
              <span className="text-xs font-medium">Rendering Real-time Production Graph...</span>
            </div>
          </div>
        </article>

        <aside className="ios-widget p-6 flex flex-col min-w-0">
          <h3 className="font-bold text-sm mb-4">Recent Alerts</h3>
          <div className="space-y-4 flex-1 overflow-y-auto pr-1 max-h-[300px] xl:max-h-full">
            <AlertItem type="critical" time="09:45 AM" msg="Machine 4 Overheating" lab="1303-CZM" />
            <AlertItem type="warning" time="11:30 AM" msg="Material Shortage Line 2" lab="9997-CKH" />
            <AlertItem type="info" time="12:15 PM" msg="System Maintenance Scheduled" lab="General" />
            <AlertItem type="warning" time="01:20 PM" msg="SFC Sync Delay > 5min" lab="Infra" />
          </div>
          <button className="mt-4 w-full py-2 bg-slate-100 dark:bg-slate-800 rounded-xl text-[10px] font-bold hover:bg-slate-200 dark:hover:bg-slate-700 transition-all shrink-0">
            View All Notifications
          </button>
        </aside>
      </div>

      {/* Bottom Grid: Facility Status + Tasks */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6 h-auto xl:h-[300px]">
        <div className="ios-widget p-6 min-w-0 flex flex-col">
          <div className="flex justify-between items-center mb-6">
            <h3 className="font-bold text-sm">Facility Status</h3>
            <div className="flex gap-4 text-[10px] font-bold uppercase tracking-wider overflow-hidden">
              <span className="flex items-center gap-1.5 text-emerald-500 shrink-0"><div className="w-2 h-2 rounded-full bg-emerald-500" /> Active</span>
              <span className="flex items-center gap-1.5 text-amber-500 shrink-0"><div className="w-2 h-2 rounded-full bg-amber-500" /> Maintenance</span>
              <span className="flex items-center gap-1.5 text-red-500 shrink-0"><div className="w-2 h-2 rounded-full bg-red-400" /> Down</span>
            </div>
          </div>
          <div className="flex-1 grid grid-cols-10 gap-2 overflow-hidden min-h-[150px]">
            {Array.from({ length: 40 }).map((_, i) => (
              <div key={i} className={`rounded-md ${i % 7 === 0 ? 'bg-amber-400/20 border border-amber-400/30' : i % 13 === 0 ? 'bg-red-400/20 border border-red-400/30' : 'bg-emerald-400/10 border border-emerald-400/20'} cursor-pointer hover:scale-110 transition-transform`} />
            ))}
          </div>
        </div>

        <div className="ios-widget p-6 min-w-0 flex flex-col">
          <div className="flex justify-between items-center mb-6">
            <h3 className="font-bold text-sm">Scheduled Tasks</h3>
            <button className="text-[10px] font-bold text-medtronic hover:underline">Full Calendar</button>
          </div>
          <div className="space-y-3 flex-1 overflow-y-auto pr-1 max-h-[200px] xl:max-h-full">
            <TaskItem date="Tomorrow" task="Annual SAP Routing Audit" />
            <TaskItem date="Feb 3" task="CZM Machine M-102 Calibration" />
            <TaskItem date="Feb 5" task="Quarterly Quality Review" />
          </div>
        </div>
      </div>
    </div>
  );
}

// Reusable parts within this page (or move to components if shared)
function KpiWidget({ title, value, trend, status }: { title: string, value: string, trend: string, status: 'success' | 'warning' | 'neutral' }) {
  return (
    <div className="ios-widget p-6 group hover:translate-y-[-2px] hover:shadow-lg transition-all cursor-pointer">
      <h3 className="text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-1">{title}</h3>
      <div className="flex items-baseline gap-2 mb-2">
        <span className="text-2xl font-black tracking-tight truncate">{value}</span>
      </div>
      <div className={`flex items-center gap-1 text-[10px] font-bold ${status === 'success' ? 'text-emerald-500' : status === 'warning' ? 'text-amber-500' : 'text-slate-400'
        }`}>
        {status === 'success' ? <ChevronUp size={12} /> : status === 'warning' ? <ChevronDown size={12} /> : null}
        <span className="truncate">{trend}</span>
      </div>
    </div>
  );
}

function AlertItem({ type, time, msg, lab }: { type: 'critical' | 'warning' | 'info', time: string, msg: string, lab: string }) {
  const color = type === 'critical' ? 'text-red-500 bg-red-500/10' : type === 'warning' ? 'text-amber-500 bg-amber-500/10' : 'text-blue-500 bg-blue-500/10';
  return (
    <div className="flex gap-3 animate-in slide-in-from-right duration-300 overflow-hidden">
      <div className={`w-8 h-8 rounded-xl ${color} flex items-center justify-center shrink-0`}>
        <AlertTriangle size={16} />
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex justify-between items-start">
          <p className="text-[11px] font-bold truncate leading-tight uppercase">{msg}</p>
          <span className="text-[9px] text-slate-400 font-medium shrink-0 ml-2">{time}</span>
        </div>
        <p className="text-[10px] text-slate-500 font-medium">{lab}</p>
      </div>
    </div>
  );
}

function TaskItem({ date, task }: { date: string, task: string }) {
  return (
    <div className="flex items-center gap-4 group cursor-pointer p-2 hover:bg-slate-50 dark:hover:bg-slate-800 rounded-xl transition-all overflow-hidden">
      <div className="w-10 h-10 rounded-2xl bg-slate-100 dark:bg-slate-900 border border-[var(--border)] flex flex-col items-center justify-center text-medtronic shrink-0 transition-colors group-hover:bg-medtronic group-hover:text-white">
        <Calendar size={18} />
      </div>
      <div className="flex-1 min-w-0">
        <p className="text-xs font-bold truncate leading-tight">{task}</p>
        <p className="text-[10px] text-slate-500 font-medium">{date}</p>
      </div>
    </div>
  )
}
