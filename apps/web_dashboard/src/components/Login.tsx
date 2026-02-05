'use client';

import React, { useState, useEffect } from 'react';
import { useAuth } from '@/context/AuthContext';
import { Lock, User, Eye, EyeOff, Loader2, CheckCircle2, AlertCircle } from 'lucide-react';

const translations = {
    en: {
        campus: 'Changzhou Campus',
        title: 'Digital Central',
        username: 'Username',
        password: 'Password',
        remember: 'Remember my account',
        signIn: 'Sign In to Dashboard',
        verifying: 'Verifying...',
        error: 'Invalid username or password.',
        author: 'Authorized Personnel Only',
        slogan: 'Engineering the extraordinary',
        brand: 'Medtronic'
    }
};

export default function Login() {
    const { login } = useAuth();
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
    const [remember, setRemember] = useState(false);
    const [showPassword, setShowPassword] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [isLoading, setIsLoading] = useState(false);
    const [isSuccess, setIsSuccess] = useState(false);

    const t = translations.en;

    useEffect(() => {
        const savedUsername = localStorage.getItem('remembered_username');
        if (savedUsername) {
            setUsername(savedUsername);
            setRemember(true);
        }
    }, []);

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        setError(null);
        setIsLoading(true);

        try {
            const success = await login(username, password, remember);
            if (success) {
                setIsSuccess(true);
            } else {
                setError(t.error);
            }
        } catch (err) {
            setError('An unexpected error occurred.');
        } finally {
            setIsLoading(false);
        }
    };

    return (
        <div className="fixed inset-0 z-[200] flex items-center justify-center bg-[#002554] overflow-hidden">
            {/* Background Decorative Elements */}
            <div className="absolute top-[-10%] right-[-10%] w-[40%] h-[40%] bg-blue-500/10 blur-[120px] rounded-full" />
            <div className="absolute bottom-[-10%] left-[-10%] w-[40%] h-[40%] bg-blue-400/10 blur-[120px] rounded-full" />

            {/* Top-Left Branding */}
            <div className="absolute top-10 left-10 flex items-center gap-4 opacity-95 group/brand z-[210]">
                <div className="w-14 h-14 rounded-2xl bg-white flex items-center justify-center p-2.5 shadow-xl shadow-white/5 transition-transform group-hover/brand:scale-105 duration-500">
                    <img src="/art-symbol-rgb-full-color.svg" alt="Medtronic" className="w-full h-full object-contain" />
                </div>
                <div className="text-left py-1">
                    <div className="flex items-baseline gap-3">
                        <h2 className="text-[26px] font-bold text-white leading-none tracking-tight">Medtronic</h2>
                        <span className="text-[22px] font-bold text-white leading-none opacity-80">美敦力</span>
                    </div>
                    <div className="mt-2 text-left -mr-[1em]">
                        <p className="text-[14px] font-medium text-white tracking-[1em] leading-none uppercase">生命因科技不凡</p>
                    </div>
                </div>
            </div>

            <div className="w-full max-w-xl px-6 animate-in fade-in slide-in-from-bottom-8 duration-700 relative z-20">
                {/* Hero Title Area */}
                <div className="text-center mb-10 relative">
                    <div className="flex items-center justify-center gap-4 mb-3">
                        <div className="h-px w-10 bg-gradient-to-r from-transparent via-blue-500/30 to-blue-500/50" />
                        <p className="text-[12px] font-bold text-slate-400 tracking-[0.3em] whitespace-nowrap uppercase">
                            {t.campus}
                        </p>
                        <div className="h-px w-10 bg-gradient-to-l from-transparent via-blue-500/30 to-blue-500/50" />
                    </div>
                    <h1 className="text-7xl font-black text-white tracking-tighter drop-shadow-2xl leading-none whitespace-nowrap">
                        Digital <span className="text-blue-400 bg-gradient-to-r from-white via-blue-100 to-blue-400 bg-clip-text text-transparent [-webkit-background-clip:text]">Central</span>
                    </h1>
                </div>

                {/* Login Card */}
                <div className="bg-white/[0.03] border border-white/10 backdrop-blur-2xl rounded-[2.5rem] p-10 shadow-2xl relative overflow-hidden group">
                    {/* Success Overlay */}
                    {isSuccess && (
                        <div className="absolute inset-0 bg-emerald-500/20 backdrop-blur-md flex flex-col items-center justify-center z-10 animate-in fade-in duration-500">
                            <CheckCircle2 size={48} className="text-emerald-400 mb-4 animate-bounce" />
                            <span className="text-white font-black uppercase tracking-widest">Access Granted</span>
                        </div>
                    )}

                    <form onSubmit={handleSubmit} className="space-y-6">
                        {/* Username */}
                        <div className="space-y-2">
                            <label className="text-[10px] font-black text-slate-400 uppercase tracking-widest ml-1">{t.username}</label>
                            <div className="relative group/input">
                                <div className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-500 group-focus-within/input:text-blue-400 transition-colors">
                                    <User size={18} />
                                </div>
                                <input
                                    type="text"
                                    value={username}
                                    onChange={(e) => setUsername(e.target.value)}
                                    placeholder="Enter your username"
                                    className="w-full bg-white/[0.05] border border-white/10 text-white pl-12 pr-4 py-4 rounded-2xl focus:outline-none focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500/50 transition-all placeholder:text-slate-600"
                                    required
                                />
                            </div>
                        </div>

                        {/* Password */}
                        <div className="space-y-2">
                            <label className="text-[10px] font-black text-slate-400 uppercase tracking-widest ml-1">{t.password}</label>
                            <div className="relative group/input">
                                <div className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-500 group-focus-within/input:text-blue-400 transition-colors">
                                    <Lock size={18} />
                                </div>
                                <input
                                    type={showPassword ? 'text' : 'password'}
                                    value={password}
                                    onChange={(e) => setPassword(e.target.value)}
                                    placeholder="••••••••"
                                    className="w-full bg-white/[0.05] border border-white/10 text-white pl-12 pr-12 py-4 rounded-2xl focus:outline-none focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500/50 transition-all placeholder:text-slate-600"
                                    required
                                />
                                <button
                                    type="button"
                                    onClick={() => setShowPassword(!showPassword)}
                                    className="absolute right-4 top-1/2 -translate-y-1/2 text-slate-500 hover:text-white transition-colors"
                                >
                                    {showPassword ? <EyeOff size={18} /> : <Eye size={18} />}
                                </button>
                            </div>
                        </div>

                        {/* Options */}
                        <div className="flex items-center justify-between px-1">
                            <label className="flex items-center gap-2 cursor-pointer group/check">
                                <input
                                    type="checkbox"
                                    checked={remember}
                                    onChange={(e) => setRemember(e.target.checked)}
                                    className="w-4 h-4 rounded border-white/10 bg-white/5 text-blue-500 focus:ring-0 focus:ring-offset-0"
                                />
                                <span className="text-[11px] font-bold text-slate-400 group-hover/check:text-slate-200 transition-colors">{t.remember}</span>
                            </label>
                        </div>

                        {/* Submit */}
                        <button
                            type="submit"
                            disabled={isLoading}
                            className="w-full bg-blue-600 hover:bg-blue-500 text-white font-black py-4 rounded-2xl shadow-xl shadow-blue-600/20 active:scale-[0.98] transition-all flex items-center justify-center gap-3 disabled:opacity-50 group/btn overflow-hidden relative"
                        >
                            {isLoading ? (
                                <>
                                    <Loader2 size={18} className="animate-spin" />
                                    <span className="uppercase tracking-widest text-xs">{t.verifying}</span>
                                </>
                            ) : (
                                <>
                                    <span className="uppercase tracking-[0.2em] text-xs font-black">{t.signIn}</span>
                                    <div className="w-1 h-1 rounded-full bg-white/30 group-hover/btn:scale-[20] transition-transform duration-700" />
                                </>
                            )}
                        </button>

                        {/* Error Message */}
                        {error && (
                            <div className="flex items-center gap-2 text-red-400 bg-red-400/10 p-4 rounded-2xl animate-in slide-in-from-top-2">
                                <AlertCircle size={16} />
                                <span className="text-xs font-bold">{error}</span>
                            </div>
                        )}
                    </form>
                </div>

                {/* Footer Info */}
                <div className="mt-10 text-center space-y-4">
                    <p className="text-[10px] font-black text-slate-500 uppercase tracking-[0.3em] opacity-50">
                        {t.author}
                    </p>
                </div>
            </div>
        </div>
    );
}
