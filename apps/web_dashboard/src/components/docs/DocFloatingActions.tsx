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
                className="flex items-center gap-4 px-8 py-4 rounded-2xl bg-[#004b87] text-white shadow-[0_15px_40px_rgba(0,37,84,0.25)] hover:bg-[#003a6a] hover:scale-[1.03] active:scale-95 transition-all outline-none border border-white/10 group relative"
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

                {/* Status indicator aligned with the new aesthetic */}
                <div className={`absolute -top-1 -right-1 w-4 h-4 rounded-full border-2 border-white shadow-md ${isExpanded ? 'bg-emerald-400' : 'bg-orange-400'} transition-colors duration-500`} />
            </button>
        </div>
    );
}
