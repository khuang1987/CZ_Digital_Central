'use client';
import StandardPageLayout, { PageTab } from '@/components/StandardPageLayout';
import { Zap, Clock, Users, Wrench } from 'lucide-react';

export default function Page() {
    const tabs: PageTab[] = [
        { label: 'OEE (设备综合效率)', href: '/efficiency/oee', icon: <Zap size={14} /> },
        { label: '人工效率 (Labor Eff.)', href: '/efficiency/labor', icon: <Users size={14} /> },
        { label: '停机时间 (Downtime)', href: '/efficiency/downtime', icon: <Clock size={14} />, active: true },
        { label: '设备维护 (Maintenance)', href: "/efficiency/maintenance", icon: <Wrench size={14} /> },
    ];

    return (
        <StandardPageLayout
            title="Downtime Analysis"
            description="Analyze equipment downtime causes and frequency."
            icon={<Clock size={24} />}
            tabs={tabs}
        >
            <div className="flex flex-col items-center justify-center py-20 text-slate-400">
                <p className="text-lg font-medium">停机时间 (Downtime)</p>
                <p className="text-sm">Module under construction...</p>
            </div>
        </StandardPageLayout>
    );
}
