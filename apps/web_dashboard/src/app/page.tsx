'use client';

import React, { useState, useEffect } from 'react';
import {
  LayoutDashboard,
  BarChart3,
  Settings,
  FlaskConical,
  ClipboardCheck,
  Package,
  RefreshCcw,
  Rocket,
  ChevronRight,
  ShieldCheck,
  Zap
} from 'lucide-react';

export default function Home() {
  const [logs, setLogs] = useState([
    { time: '10:45:32', type: 'INFO', msg: 'System initialized.' }
  ]);
  const [isRefreshing, setIsRefreshing] = useState(false);

  const addLog = (msg: string, type = 'INFO') => {
    const time = new Date().toLocaleTimeString();
    setLogs(prev => [...prev.slice(-4), { time, type, msg }]);
  };

  const handleRefresh = async () => {
    if (isRefreshing) return;
    setIsRefreshing(true);
    addLog('Starting full system refresh...', 'PROC');

    // Simulate steps
    setTimeout(() => addLog('Connecting to SQL Server...', 'INFO'), 800);
    setTimeout(() => addLog('Fetching Planner data (CZ-Machining)...', 'INFO'), 1800);
    setTimeout(() => addLog('Planner Data: 45 tasks synchronized.', 'SUCCESS'), 3200);
    setTimeout(() => {
      addLog('System refresh completed successfully.', 'SUCCESS');
      setIsRefreshing(false);
    }, 4500);
  };

  return (
    <div className="flex h-screen overflow-hidden bg-slate-950 text-slate-100 font-sans">
      {/* Sidebar */}
      <aside className="w-64 glass-sidebar p-6 flex flex-col gap-8 shrink-0">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-xl bg-cyan-500/20 flex items-center justify-center text-cyan-400 border border-cyan-500/30">
            <ShieldCheck size={24} />
          </div>
          <h1 className="text-xl font-bold tracking-tight">CZ DIGITAL CENTRAL</h1>
        </div>

        <nav className="flex-1 flex flex-col gap-2">
          <NavItem icon={<LayoutDashboard size={20} />} label="Dashboard" active />
          <NavItem icon={<BarChart3 size={20} />} label="Analytics" />
          <NavItem icon={<Zap size={20} />} label="Production" />
          <NavItem icon={<ClipboardCheck size={20} />} label="Quality" />
          <NavItem icon={<Settings size={20} />} label="Maintenance" />
          <NavItem icon={<Package size={20} />} label="Inventory" />
        </nav>

        <div className="mt-auto glass-card p-4 flex items-center gap-3">
          <div className="w-10 h-10 rounded-full bg-slate-800 flex items-center justify-center">
            <span className="text-xs font-bold font-mono">USER</span>
          </div>
          <div>
            <p className="text-sm font-medium">B1 Project</p>
            <p className="text-[10px] text-slate-400 uppercase tracking-widest">Administrator</p>
          </div>
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 p-8 overflow-y-auto bg-gradient-to-br from-slate-950 via-slate-900 to-slate-950">
        <header className="flex justify-between items-center mb-8">
          <div>
            <h2 className="text-3xl font-bold tracking-tight bg-clip-text text-transparent bg-gradient-to-r from-white to-slate-400">
              Operations Center
            </h2>
            <p className="text-slate-400">Mission control for real-time manufacturing data</p>
          </div>
          <div className="flex gap-4">
            <button
              onClick={handleRefresh}
              disabled={isRefreshing}
              className={`flex items-center gap-2 px-6 py-2.5 rounded-xl transition-all font-semibold ${isRefreshing
                  ? 'bg-slate-800 text-slate-500 cursor-not-allowed'
                  : 'bg-cyan-600 hover:bg-cyan-500 text-white shadow-[0_0_20px_rgba(8,145,178,0.4)] active:scale-95'
                }`}
            >
              <RefreshCcw size={18} className={isRefreshing ? 'animate-spin' : ''} />
              <span>{isRefreshing ? 'Refreshing...' : 'System Refresh'}</span>
            </button>
            <button className="flex items-center gap-2 px-6 py-2.5 rounded-xl bg-slate-800 hover:bg-slate-700 transition-all font-semibold border border-white/5">
              <Rocket size={18} className="text-emerald-400" />
              <span>Quick Deploy</span>
            </button>
          </div>
        </header>

        {/* Stats Grid */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
          <StatCard title="YIELD" value="98.5%" trend="+1.2% from yesterday" color="text-cyan-400" bgColor="bg-cyan-400/10" />
          <StatCard title="WIP" value="2,450" trend="+50 Units in progress" color="text-emerald-400" bgColor="bg-emerald-400/10" />
          <StatCard title="OEE" value="94.2%" trend="+0.8% efficiency improvement" color="text-blue-400" bgColor="bg-blue-400/10" />
        </div>

        {/* Large Chart Placeholder */}
        <div className="glass-card p-6 mb-8 border-l-4 border-l-cyan-500 shadow-cyan-900/10">
          <div className="flex justify-between items-center mb-6">
            <div className="flex items-baseline gap-3">
              <h3 className="text-lg font-semibold">Production Analytics</h3>
              <span className="text-xs text-slate-500">Live Trend / Last 30 Days</span>
            </div>
            <div className="flex bg-slate-900 p-1 rounded-lg border border-white/5">
              <button className="px-3 py-1 rounded-md text-xs text-slate-400 hover:text-white transition-colors">Daily</button>
              <button className="px-3 py-1 rounded-md text-xs bg-cyan-500/20 text-cyan-400 font-semibold border border-cyan-500/30 shadow-inner">Monthly</button>
            </div>
          </div>
          <div className="w-full h-80 bg-slate-900/50 rounded-xl border border-dashed border-white/10 flex items-center justify-center relative overflow-hidden group">
            <div className="absolute inset-0 bg-gradient-to-t from-cyan-500/5 to-transparent opacity-0 group-hover:opacity-100 transition-opacity" />
            <p className="text-slate-500 text-sm font-medium z-10">Waiting for Data Pipeline Visualization...</p>
          </div>
        </div>

        {/* Real-time Console */}
        <div className="glass-card p-4 border border-cyan-500/20 bg-slate-900/40">
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-2">
              <div className={`w-2 h-2 rounded-full ${isRefreshing ? 'bg-cyan-400 animate-pulse' : 'bg-emerald-400'}`} />
              <h4 className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">
                {isRefreshing ? 'Execution Stream [ACTIVE]' : 'System Terminal [IDLE]'}
              </h4>
            </div>
            <span className="text-[10px] text-slate-600 font-mono">TTY-01</span>
          </div>
          <div className="font-mono text-[12px] min-h-[100px] flex flex-col gap-1.5">
            {logs.map((log, i) => (
              <div key={i} className="flex gap-4 animate-in slide-in-from-left-2 duration-300">
                <span className="text-slate-600 shrink-0">{log.time}</span>
                <span className={`font-bold shrink-0 ${log.type === 'SUCCESS' ? 'text-emerald-400' :
                    log.type === 'PROC' ? 'text-cyan-400' : 'text-blue-400'
                  }`}>[{log.type}]</span>
                <span className="text-slate-300">{log.msg}</span>
              </div>
            ))}
          </div>
        </div>
      </main>
    </div>
  );
}

function NavItem({ icon, label, active = false }: { icon: React.ReactNode, label: string, active?: boolean }) {
  return (
    <div className={`flex items-center gap-3 px-4 py-3 rounded-xl cursor-pointer transition-all active:scale-95 ${active
        ? 'bg-white/10 text-cyan-400 border border-white/10 shadow-lg glow-cyan'
        : 'text-slate-400 hover:bg-white/5 hover:text-slate-200'
      }`}>
      {icon}
      <span className="text-sm font-semibold">{label}</span>
      {active && <ChevronRight className="ml-auto" size={16} />}
    </div>
  );
}

function StatCard({ title, value, trend, color, bgColor }: { title: string, value: string, trend: string, color: string, bgColor: string }) {
  return (
    <div className="glass-card p-6 flex flex-col gap-3 group hover:bg-white/10 transition-all cursor-pointer relative overflow-hidden">
      <div className={`absolute top-0 right-0 w-24 h-24 blur-3xl opacity-20 -mr-8 -mt-8 ${bgColor}`} />
      <h3 className="text-[10px] font-black text-slate-500 uppercase tracking-[0.2em]">{title}</h3>
      <div className="flex items-baseline gap-2">
        <div className={`text-4xl font-black ${color} tracking-tighter`}>{value}</div>
        <div className={`w-2 h-2 rounded-full mb-1 ${color}`} />
      </div>
      <p className="text-[11px] text-slate-400 font-medium">{trend}</p>

      {/* Decorative Sparkline Placeholder */}
      <div className="mt-2 h-1 w-full bg-slate-800 rounded-full overflow-hidden">
        <div className={`h-full ${color.replace('text', 'bg')} opacity-40`} style={{ width: '70%' }} />
      </div>
    </div>
  );
}
