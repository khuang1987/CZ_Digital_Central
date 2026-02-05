'use client';

import React, { createContext, useContext, useState, useEffect, ReactNode } from 'react';

interface User {
    username: string;
    role: 'admin' | 'user';
}

interface AuthContextType {
    user: User | null;
    login: (username: string, password: string, remember: boolean) => Promise<boolean>;
    logout: () => void;
    isLoading: boolean;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

const USERS = [
    { username: 'admin', password: '0000', role: 'admin' as const },
    { username: 'user1', password: '0000', role: 'user' as const },
];

export function AuthProvider({ children }: { children: ReactNode }) {
    const [user, setUser] = useState<User | null>(null);
    const [isLoading, setIsLoading] = useState(true);

    useEffect(() => {
        // Check persistent storage first (Remember Me)
        const savedUser = localStorage.getItem('auth_user');
        if (savedUser) {
            setUser(JSON.parse(savedUser));
        } else {
            // Check session storage (Active Session)
            const sessionUser = sessionStorage.getItem('auth_user');
            if (sessionUser) {
                setUser(JSON.parse(sessionUser));
            }
        }
        setIsLoading(false);
    }, []);

    const login = async (username: string, password: string, remember: boolean): Promise<boolean> => {
        const foundUser = USERS.find(u => u.username === username && u.password === password);

        if (foundUser) {
            const userData = { username: foundUser.username, role: foundUser.role };
            setUser(userData);

            if (remember) {
                localStorage.setItem('auth_user', JSON.stringify(userData));
                localStorage.setItem('remembered_username', username);
            } else {
                localStorage.removeItem('auth_user');
                sessionStorage.setItem('auth_user', JSON.stringify(userData));
            }
            return true;
        }
        return false;
    };

    const logout = () => {
        setUser(null);
        localStorage.removeItem('auth_user');
        sessionStorage.removeItem('auth_user');
    };

    return (
        <AuthContext.Provider value={{ user, login, logout, isLoading }}>
            {children}
        </AuthContext.Provider>
    );
}

export function useAuth() {
    const context = useContext(AuthContext);
    if (context === undefined) {
        throw new Error('useAuth must be used within an AuthProvider');
    }
    return context;
}
