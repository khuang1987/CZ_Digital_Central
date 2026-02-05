'use client';
import StandardPageLayout, { PageTab } from '@/components/StandardPageLayout';
import { Activity, FileText, Wrench } from 'lucide-react';

export default function Page() {
    const tabs: PageTab[] = [
        { label: '服务器状态 (Status)', href: '/server', icon: <Activity size={14} /> },
        { label: '服务器日志 (Logs)', href: '/server/logs', icon: <FileText size={14} /> },
        { label: '服务器维护 (Maintenance)', href: '/server/maintenance', icon: <Wrench size={14} />, active: true },
    ];

    return (
        <StandardPageLayout title="工程 (Engineering)" tabs={tabs}>
            <div className="flex flex-col items-center justify-center py-20 text-slate-400">
                <p className="text-lg font-medium">服务器维护 (Maintenance)</p>
                <p className="text-sm">Functions: Data Collection, Refresh Stats</p>
            </div>
        </StandardPageLayout>
    );
}
