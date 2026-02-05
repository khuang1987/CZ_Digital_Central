'use client';
import StandardPageLayout, { PageTab } from '@/components/StandardPageLayout';
import { Box, Package, Truck, BarChart3 } from 'lucide-react';

export default function Page() {
    const tabs: PageTab[] = [
        { label: '库存概览 (Overview)', href: '/inventory', icon: <BarChart3 size={14} />, active: true },
        { label: '入库 (Inbound)', href: '/inventory/inbound', icon: <Package size={14} />, disabled: true },
        { label: '出库 (Outbound)', href: '/inventory/outbound', icon: <Truck size={14} />, disabled: true },
    ];

    return (
        <StandardPageLayout
            title="Supply Chain Overview"
            description="Track inventory levels and material movements."
            icon={<Box size={24} />}
            tabs={tabs}
        >
            <div className="flex flex-col items-center justify-center py-20 text-slate-400">
                <p className="text-lg font-medium">库存概览 (Inventory Overview)</p>
                <p className="text-sm">Module under construction...</p>
            </div>
        </StandardPageLayout>
    );
}
