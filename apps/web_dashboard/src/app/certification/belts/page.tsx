'use client';
import StandardPageLayout, { PageTab } from '@/components/StandardPageLayout';
import { Award, Target } from 'lucide-react';

export default function Page() {
    const tabs: PageTab[] = [
        { label: '黄带/绿带/黑带 (Belt Certs)', href: '/certification/belts', icon: <Award size={14} />, active: true },
        { label: 'A3 项目 (A3 Projects)', href: '/certification/a3', icon: <Target size={14} /> },
    ];

    return (
        <StandardPageLayout
            title="Belt Certification"
            description="Track Six Sigma belt certifications and training status."
            icon={<Award size={24} />}
            tabs={tabs}
        >
            <div className="flex flex-col items-center justify-center py-20 text-slate-400">
                <p className="text-lg font-medium">黄带/绿带/黑带 (Belt Certs)</p>
                <p className="text-sm">Module under construction...</p>
            </div>
        </StandardPageLayout>
    );
}
