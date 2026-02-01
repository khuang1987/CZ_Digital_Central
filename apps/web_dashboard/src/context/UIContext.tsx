'use client';

import React, { createContext, useContext, useState, ReactNode } from 'react';

interface UIContextType {
    isSidebarCollapsed: boolean;
    toggleSidebar: () => void;
    isFilterOpen: boolean;
    setFilterOpen: (open: boolean) => void;
    toggleFilter: () => void;
    resetFilters: () => void;
    setResetHandler: (handler: () => void) => void;
}

const UIContext = createContext<UIContextType | undefined>(undefined);

export function UIProvider({ children }: { children: ReactNode }) {
    const [isSidebarCollapsed, setSidebarCollapsed] = useState(false);
    const [isFilterOpen, setFilterOpen] = useState(true);
    const [resetHandler, setResetHandler] = useState<(() => void) | null>(null);

    const toggleSidebar = () => setSidebarCollapsed(prev => !prev);
    const toggleFilter = () => setFilterOpen(prev => !prev);
    const resetFilters = () => {
        if (resetHandler) resetHandler();
    };

    return (
        <UIContext.Provider value={{
            isSidebarCollapsed,
            toggleSidebar,
            isFilterOpen,
            setFilterOpen,
            toggleFilter,
            resetFilters,
            setResetHandler
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
