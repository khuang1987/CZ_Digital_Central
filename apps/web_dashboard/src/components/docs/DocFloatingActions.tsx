'use client';

import React, { useState } from 'react';
import { Maximize2, Minimize2 } from 'lucide-react';

export default function DocFloatingActions() {
    const [isExpanded, setIsExpanded] = useState(true);

    const toggleAll = () => {
        const details = document.querySelectorAll('details');
        const nextState = !isExpanded;

        details.forEach((d) => {
            if (nextState) {
                (d as HTMLDetailsElement).open = true;
            } else {
                (d as HTMLDetailsElement).open = false;
            }
        });

        setIsExpanded(nextState);
    };

    return (
        <div className="fixed bottom-10 right-10 z-[100] animate-in slide-in-from-bottom-5 duration-700">
            <button
                onClick={toggleAll}
                className="flex items-center gap-3 px-8 py-4 rounded-full bg-slate-900/95 dark:bg-white/10 text-white dark:text-slate-100 shadow-2xl backdrop-blur-xl hover:bg-black dark:hover:bg-white/20 hover:scale-[1.05] active:scale-95 transition-all outline-none border border-white/10 dark:border-white/5 group"
            >
                <div className="relative">
                    {isExpanded ? (
                        <Minimize2 size={20} className="animate-in fade-in zoom-in duration-300" />
                    ) : (
                        <Maximize2 size={20} className="animate-in fade-in zoom-in duration-300" />
                    )}
                </div>
                <span className="font-bold tracking-wider text-sm">
                    {isExpanded ? '全部收纳' : '全部展开'}
                </span>

                {/* Subtle indicator dot */}
                <span className={`absolute -top-1 -right-1 w-3 h-3 rounded-full border-2 border-white shadow-sm ${isExpanded ? 'bg-green-400' : 'bg-orange-400'} transition-colors duration-500`} />
            </button>
        </div>
    );
}
