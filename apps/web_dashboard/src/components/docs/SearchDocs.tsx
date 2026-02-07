'use client';

import React, { useState, useEffect, useRef } from 'react';
import { Search, FileText, ChevronRight, X, Loader2 } from 'lucide-react';
import { useRouter } from 'next/navigation';

interface SearchResult {
    file: string;
    fileName: string;
    section: string;
    snippet: string;
    line: number;
}

export default function SearchDocs() {
    const [query, setQuery] = useState('');
    const [results, setResults] = useState<SearchResult[]>([]);
    const [isLoading, setIsLoading] = useState(false);
    const [isOpen, setIsOpen] = useState(false);
    const router = useRouter();
    const dropdownRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
                setIsOpen(false);
            }
        };
        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);

    useEffect(() => {
        const delayDebounceFn = setTimeout(async () => {
            if (query.length >= 2) {
                setIsLoading(true);
                try {
                    const response = await fetch(`/api/docs/search?q=${encodeURIComponent(query)}`);
                    const data = await response.json();
                    setResults(data.results || []);
                    setIsOpen(true);
                } catch (error) {
                    console.error('Search failed:', error);
                } finally {
                    setIsLoading(false);
                }
            } else {
                setResults([]);
                setIsOpen(false);
            }
        }, 300);

        return () => clearTimeout(delayDebounceFn);
    }, [query]);

    const handleSelect = (file: string) => {
        router.push(`/docs?file=${file}`);
        setIsOpen(false);
        setQuery('');
    };

    return (
        <div className="relative w-full max-w-md" ref={dropdownRef}>
            <div className="relative group">
                <Search className={`absolute left-4 top-1/2 -translate-y-1/2 transition-colors ${isLoading ? 'text-medtronic animate-pulse' : 'text-slate-400 group-focus-within:text-medtronic'}`} size={18} />
                <input
                    type="text"
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    onFocus={() => query.length >= 2 && setIsOpen(true)}
                    placeholder="搜索文档内容 (组件, 表名, 逻辑...)"
                    className="w-full pl-12 pr-10 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl text-sm font-medium focus:ring-2 focus:ring-medtronic/20 focus:border-medtronic outline-none transition-all"
                />
                {query && (
                    <button
                        onClick={() => setQuery('')}
                        className="absolute right-3 top-1/2 -translate-y-1/2 p-1 hover:bg-slate-200 dark:hover:bg-slate-800 rounded-full transition-colors"
                    >
                        <X size={14} className="text-slate-400" />
                    </button>
                )}
            </div>

            {/* Results Dropdown */}
            {isOpen && (results.length > 0 || isLoading) && (
                <div className="absolute top-full mt-2 w-full bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl shadow-2xl z-[150] overflow-hidden animate-in fade-in slide-in-from-top-2 duration-200">
                    <div className="max-h-[400px] overflow-y-auto p-2 space-y-1">
                        {isLoading ? (
                            <div className="flex items-center justify-center p-8 text-slate-400 gap-2">
                                <Loader2 size={20} className="animate-spin text-medtronic" />
                                <span className="text-xs font-bold uppercase tracking-widest text-slate-500">Searching...</span>
                            </div>
                        ) : results.length > 0 ? (
                            results.map((res, idx) => (
                                <button
                                    key={`${res.file}-${idx}`}
                                    onClick={() => handleSelect(res.file)}
                                    className="w-full text-left p-3 rounded-xl hover:bg-slate-50 dark:hover:bg-slate-800 transition-all group border border-transparent hover:border-slate-100 dark:hover:border-slate-700"
                                >
                                    <div className="flex items-center gap-2 mb-1">
                                        <FileText size={14} className="text-medtronic" />
                                        <span className="text-xs font-bold text-slate-900 dark:text-slate-100 uppercase tracking-wider">{res.fileName}</span>
                                        <ChevronRight size={12} className="text-slate-300 ml-auto group-hover:translate-x-1 transition-transform" />
                                    </div>
                                    <div className="text-[10px] text-slate-500 font-medium mb-2">{res.section}</div>
                                    <div className="text-[11px] text-slate-600 dark:text-slate-400 leading-relaxed font-light line-clamp-2 italic bg-slate-100/50 dark:bg-slate-950/50 p-2 rounded-lg" dangerouslySetInnerHTML={{ __html: res.snippet.replace(new RegExp(query, 'gi'), match => `<strong class="text-medtronic font-bold">${match}</strong>`) }} />
                                </button>
                            ))
                        ) : (
                            <div className="p-8 text-center text-slate-400 italic text-sm">No results found for "{query}"</div>
                        )}
                    </div>
                </div>
            )}
        </div>
    );
}
