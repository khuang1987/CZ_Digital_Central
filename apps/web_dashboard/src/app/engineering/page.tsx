'use client';
import StandardPageLayout from '@/components/StandardPageLayout';
import { DraftingCompass } from 'lucide-react';

export default function Page() {
    return (
        <StandardPageLayout
            title="Engineering Projects"
            description="New Product Development & Improvement Projects"
            icon={<DraftingCompass size={24} />}
        >
            <div className="flex flex-col items-center justify-center py-20 text-slate-400">
                <DraftingCompass size={48} className="mb-4 text-slate-300" />
                <p className="text-lg font-medium">工程 (Engineering)</p>
                <p className="text-sm">New Product Introduction & CIP...</p>
            </div>
        </StandardPageLayout>
    );
}
