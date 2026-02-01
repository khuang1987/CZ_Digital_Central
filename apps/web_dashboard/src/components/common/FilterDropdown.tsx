import React, { useState, useRef, useEffect } from 'react';
import { ChevronDown, Search } from 'lucide-react';

interface FilterDropdownProps {
    title: string;
    options: string[];
    selected: string[];
    onChange: (selected: string[]) => void;
    placeholder?: string;
    emptyText?: string;
}

export default function FilterDropdown({ title, options, selected, onChange, placeholder = "Select...", emptyText = "No options" }: FilterDropdownProps) {
    const [isOpen, setIsOpen] = useState(false);
    const containerRef = useRef<HTMLDivElement>(null);
    const [searchTerm, setSearchTerm] = useState('');

    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
                setIsOpen(false);
            }
        };
        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);

    const filteredOptions = options.filter(opt =>
        opt && opt.toLowerCase().includes(searchTerm.toLowerCase())
    );

    const toggleOption = (opt: string) => {
        if (selected.includes(opt)) {
            onChange(selected.filter(s => s !== opt));
        } else {
            onChange([...selected, opt]);
        }
    };

    return (
        <div className="relative group" ref={containerRef}>
            <label className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-1.5 block ml-1">{title}</label>
            <button
                onClick={() => setIsOpen(!isOpen)}
                className={`w-full flex items-center justify-between px-3 py-2 rounded-xl text-left transition-all border ${selected.length > 0 || isOpen
                    ? 'bg-white dark:bg-slate-800 border-medtronic/50 shadow-sm ring-1 ring-medtronic/10'
                    : 'bg-slate-50 dark:bg-slate-800/50 border-slate-200 dark:border-slate-700 hover:border-slate-300 dark:hover:border-slate-600'
                    }`}
            >
                <div className="flex-1 truncate mr-2">
                    {selected.length === 0 ? (
                        <span className="text-xs text-slate-400 font-semibold">{placeholder}</span>
                    ) : (
                        <span className="text-xs font-bold text-slate-700 dark:text-slate-200">
                            {selected.length} Selected
                        </span>
                    )}
                </div>
                <ChevronDown size={14} className={`text-slate-400 transition-transform duration-200 ${isOpen ? 'rotate-180' : ''}`} />
            </button>

            {isOpen && (
                <div className="absolute left-0 top-full mt-1 w-full bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl shadow-xl z-50 overflow-hidden flex flex-col max-h-[300px]">
                    <div className="p-2 border-b border-slate-100 dark:border-slate-800 bg-slate-50/50 dark:bg-slate-900/50">
                        <div className="relative">
                            <Search size={12} className="absolute left-2 top-1/2 -translate-y-1/2 text-slate-400" />
                            <input
                                type="text"
                                placeholder="Search..."
                                value={searchTerm}
                                onChange={(e) => setSearchTerm(e.target.value)}
                                className="w-full bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg pl-7 pr-2 py-1.5 text-xs outline-none focus:border-medtronic transition-colors"
                                autoFocus
                            />
                        </div>
                    </div>
                    <div className="overflow-y-auto p-1.5 space-y-0.5 flex-1">
                        <button
                            onClick={() => onChange(selected.length === options.length ? [] : [...options])}
                            className="w-full flex items-center gap-2 px-2 py-1.5 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-800 transition-colors text-left"
                        >
                            <div className={`w-3.5 h-3.5 rounded border-2 flex items-center justify-center transition-colors ${selected.length === options.length && options.length > 0
                                ? 'bg-medtronic border-medtronic text-white'
                                : 'border-slate-300 dark:border-slate-600'
                                }`}>
                                {selected.length === options.length && options.length > 0 && <span className="text-[8px]">✓</span>}
                            </div>
                            <span className="text-xs font-bold text-medtronic">Select All</span>
                        </button>
                        <div className="h-px bg-slate-100 dark:bg-slate-800 my-1 mx-2" />
                        {filteredOptions.length > 0 ? (
                            filteredOptions.map(opt => (
                                <button
                                    key={opt}
                                    onClick={() => toggleOption(opt)}
                                    className="w-full flex items-center gap-2 px-2 py-1.5 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-800 transition-colors text-left group/item"
                                >
                                    <div className={`w-3.5 h-3.5 rounded border-2 flex items-center justify-center transition-colors ${selected.includes(opt)
                                        ? 'bg-medtronic border-medtronic text-white'
                                        : 'border-slate-300 dark:border-slate-600 group-hover/item:border-medtronic/50'
                                        }`}>
                                        {selected.includes(opt) && <span className="text-[8px]">✓</span>}
                                    </div>
                                    <span className={`text-xs ${selected.includes(opt) ? 'font-bold text-slate-800 dark:text-slate-100' : 'font-medium text-slate-600 dark:text-slate-400'}`}>
                                        {opt}
                                    </span>
                                </button>
                            ))
                        ) : (
                            <div className="px-2 py-4 text-center text-xs text-slate-400 italic">
                                {options.length === 0 ? emptyText : 'No matches'}
                            </div>
                        )}
                    </div>
                </div>
            )}
        </div>
    );
}
