'use client';

import React, { useState, useEffect } from 'react';
import Sidebar from '@/components/Sidebar';
import { usePathname } from 'next/navigation';

export default function AppShell({ children }: { children: React.ReactNode }) {
    const [theme, setTheme] = useState<'light' | 'dark'>('light');
    const pathname = usePathname();

    useEffect(() => {
        const savedTheme = localStorage.getItem('theme') as 'light' | 'dark';
        if (savedTheme) {
            setTheme(savedTheme);
        } else if (window.matchMedia('(prefers-color-scheme: dark)').matches) {
            setTheme('dark');
        }
    }, []);

    useEffect(() => {
        document.documentElement.setAttribute('data-theme', theme);
        if (theme === 'dark') {
            document.documentElement.classList.add('dark');
        } else {
            document.documentElement.classList.remove('dark');
        }
        localStorage.setItem('theme', theme);
    }, [theme]);

    const toggleTheme = () => setTheme(prev => prev === 'light' ? 'dark' : 'light');

    return (
        <div className="flex bg-[var(--background)] h-screen w-full overflow-hidden transition-colors duration-300">
            <Sidebar />

            <main className="flex-1 flex flex-col min-w-0 relative">
                {/* Main content now handles its own header via StandardPageLayout */}
                <div className="flex-1 overflow-hidden">
                    {React.Children.map(children, child => {
                        // Pass theme props to children if they are standard page layouts
                        if (React.isValidElement(child)) {
                            return React.cloneElement(child as React.ReactElement<any>, { theme, toggleTheme });
                        }
                        return child;
                    })}
                </div>
            </main>
        </div>
    );
}
