'use client';

import React, { useState, useEffect } from 'react';
import {
    Activity, Box, Truck, AlertTriangle, CheckCircle, Clock,
    Factory, Settings, Map, Layers, BarChart3, Wind, Droplets, Zap,
    Cpu, Globe, ShieldCheck, ChevronRight, Pause, Play
} from 'lucide-react';

// Types
type ViewMode = 'OVERVIEW' | 'PRODUCTION' | 'QUALITY' | 'LOGISTICS';

export default function FactoryCommandCenter() {
    const [currentView, setCurrentView] = useState<ViewMode>('OVERVIEW');
    const [isPaused, setIsPaused] = useState(false);
    const [progress, setProgress] = useState(0);

    // Auto-cycle logic
    useEffect(() => {
        if (isPaused) return;

        const cycleTime = 10000; // 10 seconds
        const step = 100; // Update every 100ms
        const increment = (step / cycleTime) * 100;

        const timer = setInterval(() => {
            setProgress(prev => {
                if (prev >= 100) {
                    // Switch view
                    setCurrentView(prevView => {
                        const views: ViewMode[] = ['OVERVIEW', 'PRODUCTION', 'QUALITY', 'LOGISTICS'];
                        const idx = views.indexOf(prevView);
                        return views[(idx + 1) % views.length];
                    });
                    return 0;
                }
                return prev + increment;
            });
        }, step);

        return () => clearInterval(timer);
    }, [isPaused]);

    // Reset progress when manually switching
    const switchView = (view: ViewMode) => {
        setCurrentView(view);
        setProgress(0);
    };

    return (
        <div className="flex flex-col h-full bg-slate-50 dark:bg-slate-950 overflow-hidden relative selection:bg-cyan-500/30">
            {/* Ambient Background Effects */}
            <div className="absolute top-0 left-0 w-full h-[500px] bg-gradient-to-b from-blue-500/5 dark:from-blue-500/10 to-transparent pointer-events-none" />
            <div className="absolute bottom-0 right-0 w-[500px] h-[500px] bg-cyan-500/5 dark:bg-cyan-500/10 blur-[100px] rounded-full pointer-events-none" />

            {/* Header Area */}
            <header className="flex items-center justify-between px-8 py-6 z-10 shrink-0">
                <div>
                    <h1 className="text-2xl font-black uppercase tracking-widest text-slate-800 dark:text-white flex items-center gap-3">
                        <Factory className="text-cyan-500" size={32} />
                        <span className="bg-clip-text text-transparent bg-gradient-to-r from-slate-800 to-slate-500 dark:from-white dark:to-slate-500">
                            CZ Digital Factory
                        </span>
                    </h1>
                    <div className="flex items-center gap-2 text-xs font-bold text-slate-400 uppercase tracking-[0.2em] mt-1 pl-1">
                        <span className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse" />
                        System Online
                        <span className="text-slate-600 dark:text-slate-700">|</span>
                        Medtronic Global Ops
                    </div>
                </div>

                {/* View Controls */}
                <div className="flex items-center gap-2 bg-white/50 dark:bg-slate-900/50 backdrop-blur-md p-1.5 rounded-xl border border-slate-200 dark:border-slate-800 shadow-sm">
                    {(['OVERVIEW', 'PRODUCTION', 'QUALITY', 'LOGISTICS'] as ViewMode[]).map(view => (
                        <button
                            key={view}
                            onClick={() => switchView(view)}
                            className={`px-4 py-2 rounded-lg text-[10px] font-black uppercase tracking-wider transition-all ${currentView === view
                                ? 'bg-cyan-500 text-white shadow-lg shadow-cyan-500/30 scale-105'
                                : 'text-slate-500 hover:bg-slate-200 dark:hover:bg-slate-800'
                                }`}
                        >
                            {view}
                        </button>
                    ))}
                    <div className="w-px h-6 bg-slate-300 dark:bg-slate-700 mx-1" />
                    <button
                        onClick={() => setIsPaused(!isPaused)}
                        className="p-2 text-slate-400 hover:text-cyan-500 transition-colors"
                    >
                        {isPaused ? <Play size={14} fill="currentColor" /> : <Pause size={14} fill="currentColor" />}
                    </button>
                </div>
            </header>

            {/* Progress Bar Line */}
            <div className="w-full h-1 bg-slate-200 dark:bg-slate-900 shrink-0">
                <div
                    className="h-full bg-gradient-to-r from-cyan-400 to-blue-600 transition-all duration-100 ease-linear shadow-[0_0_10px_rgba(34,211,238,0.5)]"
                    style={{ width: `${progress}%` }}
                />
            </div>

            {/* Main Content Area - Animate Presence Simulated with Key */}
            <main className="flex-1 p-6 relative overflow-hidden flex flex-col min-h-0 z-0">
                <div key={currentView} className="flex-1 animate-in fade-in slide-in-from-right-4 duration-500 flex flex-col min-h-0">
                    {currentView === 'OVERVIEW' && <OverviewScene />}
                    {currentView === 'PRODUCTION' && <ProductionScene />}
                    {currentView === 'QUALITY' && <QualityScene />}
                    {currentView === 'LOGISTICS' && <LogisticsScene />}
                </div>
            </main>

            {/* Footer Ticker */}
            <footer className="shrink-0 bg-white/80 dark:bg-slate-900/80 backdrop-blur border-t border-slate-200 dark:border-slate-800 px-6 py-3 flex items-center justify-between z-10">
                <div className="flex items-center gap-4 text-xs font-mono text-slate-500">
                    <span className="flex items-center gap-2 text-cyan-600 font-bold">
                        <Activity size={14} /> LIVE FEED
                    </span>
                    <span className="opacity-50">|</span>
                    <div className="animate-pulse flex items-center gap-2">
                        <span className="text-emerald-500">●</span> 1303-CZM: Operational
                    </div>
                    <span className="opacity-50">|</span>
                    <div className="flex items-center gap-2">
                        <span className="text-amber-500">▲</span> 9997-CKH: Maintenance (Line 4)
                    </div>
                </div>
                <div className="text-[10px] uppercase font-bold text-slate-400 tracking-widest">
                    v2.4.0-stable
                </div>
            </footer>
        </div>
    );
}

// --- Sub-Scenes ---

function OverviewScene() {
    return (
        <div className="grid grid-cols-12 gap-6 h-full min-h-0">
            {/* 3D Map / Central Visual */}
            <div className="col-span-12 lg:col-span-8 bg-white dark:bg-slate-900 rounded-3xl p-6 border border-slate-200 dark:border-slate-800 shadow-xl relative overflow-hidden group">
                <div className="absolute inset-0 bg-[radial-gradient(circle_at_50%_50%,rgba(6,182,212,0.05),transparent_70%)]" />
                <div className="absolute top-6 left-6 z-10">
                    <h2 className="text-lg font-bold text-slate-700 dark:text-slate-200 flex items-center gap-2">
                        <Globe className="text-cyan-500" />
                        Global Site Overview
                    </h2>
                </div>

                {/* Simulated Map Visual */}
                <div className="w-full h-full flex items-center justify-center relative">
                    <div className="w-[600px] h-[300px] border-2 border-slate-200 dark:border-slate-700/50 rounded-[100%] absolute opacity-20 animate-[spin_60s_linear_infinite]" />
                    <div className="w-[450px] h-[225px] border border-cyan-500/20 rounded-[100%] absolute animate-[spin_40s_linear_infinite_reverse]" />

                    {/* Site Nodes */}
                    <div className="absolute bottom-[40%] left-[20%] group/node cursor-pointer">
                        <div className="w-4 h-4 rounded-full bg-cyan-500 shadow-[0_0_20px_rgba(6,182,212,0.8)] animate-pulse relative z-10" />
                        <div className="absolute top-6 left-1/2 -translate-x-1/2 bg-slate-900/90 text-white px-3 py-2 rounded-lg text-xs whitespace-nowrap opacity-0 group-hover/node:opacity-100 transition-opacity border border-cyan-500/30">
                            <div className="font-bold">Changzhou (1303)</div>
                            <div className="text-emerald-400">98% Efficiency</div>
                        </div>
                    </div>

                    <div className="absolute top-[30%] right-[30%] group/node cursor-pointer">
                        <div className="w-3 h-3 rounded-full bg-indigo-500 shadow-[0_0_20px_rgba(99,102,241,0.8)] relative z-10" />
                        <div className="absolute top-6 left-1/2 -translate-x-1/2 bg-slate-900/90 text-white px-3 py-2 rounded-lg text-xs whitespace-nowrap opacity-0 group-hover/node:opacity-100 transition-opacity border border-indigo-500/30">
                            <div className="font-bold">Kanghui (9997)</div>
                            <div className="text-amber-400">Maintenance</div>
                        </div>
                    </div>
                </div>
            </div>

            {/* Key Metrics */}
            <div className="col-span-12 lg:col-span-4 flex flex-col gap-4">
                <HeroCard title="Total Output (YTD)" value="1,240,500" unit="Units" trend="+5.2%" color="emerald" icon={<Box />} />
                <HeroCard title="Safety Incidents" value="0" unit="Days LTI" trend="Safe" color="cyan" icon={<ShieldCheck />} />
                <HeroCard title="Energy Usage" value="45.2" unit="MWh" trend="-1.2%" color="amber" icon={<Zap />} />

                {/* Weather / Environment Mini-Widget */}
                <div className="flex-1 bg-gradient-to-br from-indigo-500 to-purple-600 rounded-3xl p-6 text-white relative overflow-hidden">
                    <div className="relative z-10">
                        <div className="flex items-center gap-2 opacity-70 mb-4">
                            <Wind size={16} /> Environmental Control
                        </div>
                        <div className="grid grid-cols-2 gap-4">
                            <div>
                                <div className="text-3xl font-black">22.5°C</div>
                                <div className="text-xs opacity-70">Shop Floor Temp</div>
                            </div>
                            <div>
                                <div className="text-3xl font-black">45%</div>
                                <div className="text-xs opacity-70">Humidity</div>
                            </div>
                        </div>
                    </div>
                    <Droplets className="absolute bottom-[-20px] right-[-20px] opacity-20" size={120} />
                </div>
            </div>
        </div>
    );
}

function ProductionScene() {
    return (
        <div className="flex flex-col gap-6 h-full">
            <h2 className="text-xl font-bold text-slate-800 dark:text-slate-100 flex items-center gap-3">
                <Cpu className="text-cyan-500" /> Real-time Production Status
            </h2>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                {[1, 2, 3, 4].map(line => (
                    <div key={line} className="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl p-5 relative overflow-hidden group hover:border-cyan-500/50 transition-colors">
                        <div className="flex justify-between items-start mb-4">
                            <div className="text-xs font-black text-slate-400 uppercase tracking-widest">Line 0{line} - CNC</div>
                            <div className={`w-2 h-2 rounded-full ${line === 3 ? 'bg-amber-500 animate-pulse' : 'bg-emerald-500'}`} />
                        </div>

                        <div className="flex items-end justify-between mb-4 relative z-10">
                            <div>
                                <div className="text-3xl font-black text-slate-700 dark:text-slate-200">
                                    {line === 3 ? '0' : Math.floor(Math.random() * 200 + 800)}
                                </div>
                                <div className="text-[10px] text-slate-400 font-bold">UNITS / SHIFT</div>
                            </div>
                            <Activity size={32} className={`${line === 3 ? 'text-amber-500' : 'text-emerald-500'} opacity-30`} />
                        </div>

                        {/* Progress Bar */}
                        <div className="w-full bg-slate-100 dark:bg-slate-800 h-1.5 rounded-full overflow-hidden">
                            <div
                                className={`h-full ${line === 3 ? 'bg-amber-500' : 'bg-cyan-500'}`}
                                style={{ width: `${line === 3 ? 15 : Math.random() * 30 + 60}%` }}
                            />
                        </div>

                        {/* Hover Effect */}
                        <div className="absolute -bottom-10 -right-10 w-32 h-32 bg-gradient-to-br from-cyan-400/20 to-transparent rounded-full blur-2xl group-hover:scale-150 transition-transform duration-700" />
                    </div>
                ))}
            </div>

            {/* Detailed Gantt / Timeline Placeholder */}
            <div className="flex-1 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl p-6 relative">
                <div className="flex justify-between items-center mb-6">
                    <h3 className="text-sm font-bold text-slate-500 uppercase">Work Order Schedule</h3>
                    <div className="flex gap-2 text-[10px] font-bold">
                        <span className="px-2 py-1 rounded bg-emerald-100 dark:bg-emerald-900/30 text-emerald-600 dark:text-emerald-400">Running</span>
                        <span className="px-2 py-1 rounded bg-amber-100 dark:bg-amber-900/30 text-amber-600 dark:text-amber-400">Setup</span>
                        <span className="px-2 py-1 rounded bg-slate-100 dark:bg-slate-800 text-slate-500">Idle</span>
                    </div>
                </div>

                <div className="space-y-4">
                    {['WO-2024-001 (Knee Implant)', 'WO-2024-002 (Spine Cage)', 'WO-2024-005 (Surgical Drill)'].map((wo, i) => (
                        <div key={i} className="flex items-center gap-4">
                            <div className="w-48 text-xs font-bold text-slate-600 dark:text-slate-300 truncate shrink-0">{wo}</div>
                            <div className="flex-1 h-8 bg-slate-50 dark:bg-slate-800 rounded-lg relative overflow-hidden flex items-center px-1">
                                <div
                                    className={`h-6 rounded mx-1 ${i === 0 ? 'bg-emerald-500' : i === 1 ? 'bg-indigo-500' : 'bg-amber-500'}`}
                                    style={{ width: `${60 - i * 15}%`, marginLeft: `${i * 20}%` }}
                                />
                            </div>
                            <div className="w-16 text-right text-xs font-mono text-slate-400">
                                {100 - i * 20}%
                            </div>
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
}

function QualityScene() {
    return (
        <div className="grid grid-cols-3 gap-6 h-full">
            <div className="col-span-3 lg:col-span-1 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-3xl p-6 flex flex-col items-center justify-center text-center relative overflow-hidden">
                <div className="relative z-10">
                    <div className="text-6xl font-black text-emerald-500 mb-2 tracking-tighter">99.92%</div>
                    <div className="text-sm font-bold text-slate-400 uppercase tracking-widest">First Pass Yield</div>
                </div>
                <CheckCircle size={200} className="absolute text-emerald-500/5 -rotate-12" />
            </div>

            <div className="col-span-3 lg:col-span-2 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-3xl p-6">
                <h3 className="text-sm font-bold text-slate-500 uppercase tracking-widest mb-6">Defect Pareto Analysis</h3>
                <div className="h-[200px] flex items-end gap-4 px-4">
                    {[{ l: 'Scratch', v: 40 }, { l: 'Dim', v: 25 }, { l: 'Burr', v: 15 }, { l: 'Color', v: 10 }, { l: 'Other', v: 5 }].map((d, i) => (
                        <div key={i} className="flex-1 flex flex-col items-center gap-2 group">
                            <div className="w-full bg-slate-100 dark:bg-slate-800 rounded-t-xl relative overflow-hidden group-hover:bg-slate-200 dark:group-hover:bg-slate-700 transition-colors" style={{ height: '100%' }}>
                                <div
                                    className="absolute bottom-0 w-full bg-rose-500 transition-all duration-1000 ease-out"
                                    style={{ height: `${d.v * 2}%` }}
                                />
                            </div>
                            <span className="text-[10px] font-bold text-slate-500 uppercase">{d.l}</span>
                        </div>
                    ))}
                </div>
            </div>

            <div className="col-span-3 grid grid-cols-4 gap-4">
                {['Incoming', 'In-Process', 'Final', 'Sterilization'].map((stage, i) => (
                    <div key={i} className="bg-slate-50 dark:bg-slate-800/50 rounded-xl p-4 flex items-center justify-between">
                        <span className="text-xs font-bold text-slate-600 dark:text-slate-300">{stage}</span>
                        <span className="px-2 py-0.5 rounded text-[10px] font-black bg-emerald-100 text-emerald-600">PASS</span>
                    </div>
                ))}
            </div>
        </div>
    );
}

function LogisticsScene() {
    return (
        <div className="h-full flex flex-col gap-6">
            <div className="grid grid-cols-3 gap-4">
                <HeroCard title="Pending Shipments" value="8" unit="Orders" trend="On Time" color="blue" icon={<Truck />} />
                <HeroCard title="Avg Lead Time" value="3.2" unit="Days" trend="-0.4" color="violet" icon={<Clock />} />
                <HeroCard title="Inventory Value" value="$4.2M" unit="USD" trend="Stable" color="emerald" icon={<Box />} />
            </div>

            <div className="flex-1 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-3xl p-0 overflow-hidden relative">
                {/* Simulated Map Background */}
                <div className="absolute inset-0 bg-slate-100 dark:bg-slate-950 opacity-50">
                    <div className="w-full h-full" style={{ backgroundImage: 'radial-gradient(#cbd5e1 1px, transparent 1px)', backgroundSize: '20px 20px' }} />
                </div>

                <div className="absolute inset-0 p-8">
                    <h3 className="text-sm font-bold text-slate-500 uppercase tracking-widest mb-4 relative z-10">Real-time Logistics Map</h3>

                    {/* Routes */}
                    <svg className="absolute inset-0 w-full h-full pointer-events-none">
                        <path d="M 200 300 Q 400 100 600 250" fill="none" stroke="#0ea5e9" strokeWidth="2" strokeDasharray="5,5" className="animate-[dash_20s_linear_infinite]" />
                        <circle cx="200" cy="300" r="4" fill="#0ea5e9" />
                        <circle cx="600" cy="250" r="4" fill="#0ea5e9" />

                        {/* Moving Truck */}
                        <circle r="6" fill="#f59e0b">
                            <animateMotion
                                dur="10s"
                                repeatCount="indefinite"
                                path="M 200 300 Q 400 100 600 250"
                            />
                        </circle>
                    </svg>

                    {/* Checkpoints */}
                    <div className="relative z-10 grid grid-cols-2 gap-8 mt-20 max-w-md">
                        <div className="bg-white/90 dark:bg-slate-800/90 backdrop-blur p-4 rounded-xl border-l-4 border-cyan-500 shadow-lg">
                            <div className="text-xs font-bold text-slate-400 uppercase">Current Shipment</div>
                            <div className="text-lg font-black text-slate-800 dark:text-white">Batch #99201</div>
                            <div className="text-xs text-cyan-600 font-bold mt-1">En Route to Shanghai DC</div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}

// --- Helpers ---

function HeroCard({ title, value, unit, trend, color, icon }: any) {
    const colors: any = {
        emerald: 'text-emerald-500 bg-emerald-500/10',
        cyan: 'text-cyan-500 bg-cyan-500/10',
        amber: 'text-amber-500 bg-amber-500/10',
        blue: 'text-blue-500 bg-blue-500/10',
        violet: 'text-violet-500 bg-violet-500/10',
    };

    return (
        <div className="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl p-5 flex items-center justify-between group hover:scale-[1.02] transition-transform">
            <div>
                <p className="text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-1">{title}</p>
                <div className="flex items-baseline gap-2">
                    <span className="text-2xl font-black text-slate-800 dark:text-white">{value}</span>
                    <span className="text-xs font-bold text-slate-400">{unit}</span>
                </div>
                <div className={`text-xs font-bold mt-1 ${colors[color].split(' ')[0]}`}>{trend}</div>
            </div>
            <div className={`w-12 h-12 rounded-xl flex items-center justify-center ${colors[color]}`}>
                {React.cloneElement(icon, { size: 24 })}
            </div>
        </div>
    );
}
