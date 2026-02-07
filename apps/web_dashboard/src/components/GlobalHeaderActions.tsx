'use client';

import React, { useState, useEffect } from 'react';
import { Moon, Sun, Bell, Maximize, Minimize, ExternalLink, Search, Globe } from 'lucide-react';
import { useUI } from '@/context/UIContext';
import { t } from '@/lib/translations';

interface GlobalHeaderActionsProps {
    showExport?: boolean;
}

export default function GlobalHeaderActions({ showExport = true }: GlobalHeaderActionsProps) {
    const { theme, toggleTheme, language, toggleLanguage } = useUI();
    const [isFullscreen, setIsFullscreen] = useState(false);

    useEffect(() => {
        const handleFullscreenChange = () => {
            setIsFullscreen(!!document.fullscreenElement);
        };
        document.addEventListener('fullscreenchange', handleFullscreenChange);
        return () => document.removeEventListener('fullscreenchange', handleFullscreenChange);
    }, []);

    const toggleFullscreen = () => {
        if (!document.fullscreenElement) {
            document.documentElement.requestFullscreen();
        } else {
            document.exitFullscreen();
        }
    };

    return (
        <div className="flex items-center gap-3">
            {/* Global Search */}
            <div className="relative group hidden sm:block mr-2">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400 group-focus-within:text-medtronic transition-colors pointer-events-none" size={14} />
                <input
                    type="text"
                    placeholder={t('search_placeholder', language)}
                    className="w-48 pl-9 pr-4 py-2 bg-slate-100 dark:bg-slate-800/50 border-none rounded-full text-xs font-bold text-slate-600 dark:text-slate-300 outline-none focus:ring-2 focus:ring-medtronic/50 transition-all"
                />
            </div>

            <div className="w-[1px] h-5 bg-slate-200 dark:bg-slate-700 mx-1" />

            {/* Language Toggle */}
            <button
                onClick={toggleLanguage}
                className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-full hover:bg-slate-100 dark:hover:bg-slate-800 transition-all text-slate-600 dark:text-slate-400 group"
                title={t('toggle_lang', language)}
            >
                <Globe size={16} className="group-hover:text-medtronic transition-colors" />
                <span className="text-[10px] font-black uppercase tracking-tighter">
                    {language === 'en' ? 'EN' : '中文'}
                </span>
            </button>

            <button
                onClick={toggleTheme}
                className="p-2 rounded-full hover:bg-slate-100 dark:hover:bg-slate-800 transition-all text-slate-600 dark:text-slate-400"
                title={t('toggle_theme', language)}
            >
                {theme === 'light' ? <Moon size={18} /> : <Sun size={18} />}
            </button>

            <button
                className="p-2 rounded-full hover:bg-slate-100 dark:hover:bg-slate-800 transition-all text-slate-600 dark:text-slate-400 relative"
                title={t('notifications', language)}
            >
                <Bell size={18} />
                <span className="absolute top-1.5 right-1.5 w-1.5 h-1.5 bg-red-500 rounded-full border border-white dark:border-slate-900" />
            </button>

            <button
                onClick={toggleFullscreen}
                className="p-2 rounded-full hover:bg-slate-100 dark:hover:bg-slate-800 transition-all text-slate-600 dark:text-slate-400 mr-2"
                title={t('fullscreen', language)}
            >
                {isFullscreen ? <Minimize size={18} /> : <Maximize size={18} />}
            </button>

            {showExport && (
                <button className="flex items-center gap-2 ml-2 px-4 py-1.5 rounded-full bg-medtronic text-white text-[11px] font-black hover:brightness-110 transition-all shadow-sm active:scale-95">
                    <span className="hidden sm:inline">Export Report</span>
                    <ExternalLink size={12} />
                </button>
            )}
        </div>
    );
}
