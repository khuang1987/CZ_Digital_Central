'use client';

import React, { createContext, useContext, useState, ReactNode, useEffect } from 'react';

interface UIContextType {
    isSidebarCollapsed: boolean;
    setSidebarCollapsed: (collapsed: boolean) => void;
    toggleSidebar: () => void;
    isFilterOpen: boolean;
    setFilterOpen: (open: boolean) => void;
    toggleFilter: () => void;
    resetFilters: () => void;
    setResetHandler: (handler: () => void) => void;
    hasFilters: boolean;
    setHasFilters: (has: boolean) => void;
    theme: 'light' | 'dark';
    toggleTheme: () => void;
}

const UIContext = createContext<UIContextType | undefined>(undefined);

export function UIProvider({ children }: { children: ReactNode }) {
    const [isSidebarCollapsed, setSidebarCollapsed] = useState(false);
    const [isFilterOpen, setFilterOpen] = useState(true);
    const [resetHandler, setResetHandler] = useState<(() => void) | null>(null);
    const [hasFilters, setHasFilters] = useState(false);

    // Theme State
    const [theme, setTheme] = useState<'light' | 'dark'>('light');

    useEffect(() => {
        const savedTheme = localStorage.getItem('theme') as 'light' | 'dark' | null;
        if (savedTheme) {
            setTheme(savedTheme);
            document.documentElement.classList.add(savedTheme);
            if (savedTheme === 'dark') document.documentElement.classList.remove('light');
        } else {
            document.documentElement.classList.add('light');
        }
    }, []);

    const toggleTheme = () => {
        setTheme((prevTheme) => {
            const newTheme = prevTheme === 'light' ? 'dark' : 'light';
            localStorage.setItem('theme', newTheme);
            document.documentElement.classList.remove(prevTheme);
            document.documentElement.classList.add(newTheme);
            return newTheme;
        });
    };

    const toggleSidebar = () => setSidebarCollapsed(prev => !prev);
    const toggleFilter = () => setFilterOpen(prev => !prev);

    // Global Hotkey implementation
    useEffect(() => {
        const handleKeyDown = (e: KeyboardEvent) => {
            // Check if user is typing in an input/textarea/contentEditable
            const target = e.target as HTMLElement;
            const isTyping = target.matches('input, textarea, [contenteditable="true"]');

            if (isTyping) return;

            if (e.key.toLowerCase() === 'f') {
                // Only toggle if filters are actually enabled on this page
                if (hasFilters) {
                    e.preventDefault();
                    setFilterOpen(prev => !prev);
                }
            }
        };

        window.addEventListener('keydown', handleKeyDown);
        return () => window.removeEventListener('keydown', handleKeyDown);
    }, [hasFilters]);

    const resetFilters = () => {
        if (resetHandler) resetHandler();
    };

    return (
        <UIContext.Provider value={{
            isSidebarCollapsed,
            setSidebarCollapsed,
            toggleSidebar,
            isFilterOpen,
            setFilterOpen,
            toggleFilter,
            resetFilters,
            setResetHandler,
            hasFilters,
            setHasFilters,
            theme,
            toggleTheme
        }}>
            {children}
        </UIContext.Provider>
    );
}

export function useUI() {
    const context = useContext(UIContext);
    if (context === undefined) {
        throw new Error('useUI must be used within a UIProvider');
    }
    return context;
}
