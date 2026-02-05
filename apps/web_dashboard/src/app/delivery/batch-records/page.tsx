'use client';
import StandardPageLayout, { PageTab } from '@/components/StandardPageLayout';
import { FileCheck, ClipboardCheck, FileText } from 'lucide-react';

export default function Page() {
    const tabs: PageTab[] = [
        { label: '批记录 (Batch Records)', href: '/delivery/batch-records', icon: <ClipboardCheck size={14} />, active: true },
        { label: '验证状态 (Validation)', href: '/delivery/validation', icon: <FileCheck size={14} /> },
    ];

    return (
        <StandardPageLayout
            title="Batch Record Review"
            description="Review and approve digital batch records and history."
            icon={<FileText size={24} />}
            tabs={tabs}
        >
            <div className="flex flex-col items-center justify-center py-20 text-slate-400">
                <p className="text-lg font-medium">批记录 (Batch Records)</p>
                <p className="text-sm">Module under construction...</p>
            </div>
        </StandardPageLayout>
    );
}
