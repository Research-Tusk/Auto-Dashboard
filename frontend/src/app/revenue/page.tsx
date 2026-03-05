'use client';

import { useEffect, useState, useMemo } from 'react';
import { createClient } from '@/lib/supabase';
import type { RevenueRow, SegmentCode, RevenueChartPoint } from '@/types';
import { KPICard } from '@/components/ui/KPICard';
import { DataTable, type Column } from '@/components/ui/DataTable';
import { SegmentTabs } from '@/components/ui/SegmentTabs';
import { RevenueBarChart } from '@/components/charts/RevenueBarChart';
import { SkeletonKPIRow, SkeletonChart, SkeletonTable } from '@/components/ui/Skeleton';
import { Disclaimer } from '@/components/ui/Disclaimer';
import { formatCrores, formatUnits, formatLakhs, formatPctPlain } from '@/lib/format';
import { clsx } from 'clsx';

// ---------------------------------------------------------------------------
// Revenue page — quarterly proxy summary
// ---------------------------------------------------------------------------

// Join oem_id → name via dim_oem (fetched separately)
interface OEMInfo {
  oem_id: number;
  oem_name: string;
  nse_ticker: string | null;
  primary_segments: string[];
}

interface RevenueTableRow {
  fy_quarter: string;
  oem_id: number;
  oem_name: string;
  nse_ticker: string | null;
  segment_code: string;
  units_retail: number | null;
  asp_used: number | null;
  revenue_retail_cr: number | null;
  data_completeness: number | null;
}

const SEGMENT_ID_MAP: Record<number, SegmentCode> = {
  1: 'PV',
  2: 'CV',
  3: '2W',
};

const SEGMENT_CODE_TO_ID: Record<string, number> = {
  PV: 1, CV: 2, '2W': 3,
};

export default function RevenuePage() {
  const [revenue, setRevenue] = useState<RevenueRow[]>([]);
  const [oems, setOems] = useState<OEMInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [segment, setSegment] = useState<SegmentCode>('PV');
  const [selectedQuarter, setSelectedQuarter] = useState<string>('');

  useEffect(() => {
    const supabase = createClient();
    const fetchData = async () => {
      const [revResult, oemResult] = await Promise.all([
        supabase
          .from('est_quarterly_revenue')
          .select('fy_quarter,oem_id,segment_id,units_retail,units_wholesale,asp_used,revenue_retail_cr,revenue_wholesale_cr,data_completeness,generated_at')
          .order('fy_quarter', { ascending: false })
          .limit(400),
        supabase
          .from('dim_oem')
          .select('oem_id,oem_name,nse_ticker,primary_segments')
          .eq('is_in_scope', true)
          .order('oem_name'),
      ]);

      if (!revResult.error) setRevenue((revResult.data ?? []) as RevenueRow[]);
      if (!oemResult.error) setOems((oemResult.data ?? []) as OEMInfo[]);
      setLoading(false);
    };

    fetchData();
  }, []);

  const oemMap = useMemo(
    () => new Map(oems.map(o => [o.oem_id, o])),
    [oems]
  );

  const quarters = useMemo(() => {
    const qSet = new Set(revenue.map(r => r.fy_quarter));
    return Array.from(qSet).sort().reverse();
  }, [revenue]);

  // Set default quarter on first load
  useEffect(() => {
    if (quarters.length > 0 && !selectedQuarter) setSelectedQuarter(quarters[0]);
  }, [quarters, selectedQuarter]);

  const segmentId = SEGMENT_CODE_TO_ID[segment];

  const tableRows = useMemo((): RevenueTableRow[] => {
    return revenue
      .filter(r => r.segment_id === segmentId && r.fy_quarter === selectedQuarter)
      .map(r => {
        const oem = oemMap.get(r.oem_id);
        return {
          fy_quarter: r.fy_quarter,
          oem_id: r.oem_id,
          oem_name: oem?.oem_name ?? `OEM #${r.oem_id}`,
          nse_ticker: oem?.nse_ticker ?? null,
          segment_code: SEGMENT_ID_MAP[r.segment_id] ?? String(r.segment_id),
          units_retail: r.units_retail,
          asp_used: r.asp_used,
          revenue_retail_cr: r.revenue_retail_cr,
          data_completeness: r.data_completeness,
        };
      })
      .sort((a, b) => (b.revenue_retail_cr ?? 0) - (a.revenue_retail_cr ?? 0));
  }, [revenue, segmentId, selectedQuarter, oemMap]);

  // Bar chart data: top OEMs by revenue in selected quarter/segment
  const chartData = useMemo((): RevenueChartPoint[] => {
    return tableRows.slice(0, 10).map(r => ({
      quarter: r.oem_name.length > 12 ? r.oem_name.slice(0, 12) + '…' : r.oem_name,
      revenue_cr: r.revenue_retail_cr ?? 0,
      units: r.units_retail ?? 0,
      asp: r.asp_used ?? 0,
    }));
  }, [tableRows]);

  // Totals for KPIs
  const totalRevenue = tableRows.reduce((s, r) => s + (r.revenue_retail_cr ?? 0), 0);
  const totalUnits = tableRows.reduce((s, r) => s + (r.units_retail ?? 0), 0);
  const avgCompleteness = tableRows.length > 0
    ? tableRows.reduce((s, r) => s + (r.data_completeness ?? 0), 0) / tableRows.length
    : null;

  const columns: Column<RevenueTableRow>[] = [
    {
      key: 'oem_name',
      label: 'OEM',
      sortable: true,
      render: (row) => (
        <div>
          <div className="font-medium text-slate-900 text-sm">{row.oem_name}</div>
          {row.nse_ticker && <div className="text-xs text-slate-400">{row.nse_ticker}</div>}
        </div>
      ),
    },
    {
      key: 'units_retail',
      label: 'Units (Retail)',
      sortable: true,
      align: 'right',
      render: (row) => <span className="tabular-nums">{formatUnits(row.units_retail)}</span>,
    },
    {
      key: 'asp_used',
      label: 'ASP (₹L)',
      sortable: true,
      align: 'right',
      render: (row) => <span className="tabular-nums">{formatLakhs(row.asp_used)}</span>,
    },
    {
      key: 'revenue_retail_cr',
      label: 'Revenue Proxy (Cr)',
      sortable: true,
      align: 'right',
      render: (row) => (
        <span className="tabular-nums font-semibold text-brand-700">
          {formatCrores(row.revenue_retail_cr)}
        </span>
      ),
    },
    {
      key: 'data_completeness',
      label: 'Completeness',
      sortable: true,
      align: 'right',
      render: (row) => {
        const pct = (row.data_completeness ?? 0) * 100;
        return (
          <span className={clsx(
            'tabular-nums text-xs font-semibold',
            pct >= 90 ? 'text-emerald-600' : pct >= 70 ? 'text-amber-600' : 'text-red-500'
          )}>
            {pct.toFixed(0)}%
          </span>
        );
      },
    },
  ];

  return (
    <div className="space-y-6 max-w-[1400px]">
      {/* Page header */}
      <div className="flex flex-col sm:flex-row sm:items-start justify-between gap-3">
        <div>
          <h1 className="text-xl font-bold text-slate-900 tracking-tight">Revenue Proxy</h1>
          <p className="text-sm text-slate-500 mt-0.5">
            Demand-based estimates · Registrations × ASP assumption
          </p>
        </div>
        <div className="flex items-center gap-3">
          <SegmentTabs value={segment} onChange={setSegment} compact />
          {quarters.length > 0 && (
            <select
              value={selectedQuarter}
              onChange={e => setSelectedQuarter(e.target.value)}
              className="text-sm border border-slate-200 rounded-lg px-3 py-1.5 bg-white text-slate-700 focus:outline-none focus:ring-2 focus:ring-brand-500"
            >
              {quarters.map(q => (
                <option key={q} value={q}>{q}</option>
              ))}
            </select>
          )}
        </div>
      </div>

      {/* Disclaimer — prominent */}
      <Disclaimer variant="banner" />

      {/* KPI row */}
      {loading ? (
        <SkeletonKPIRow count={3} />
      ) : (
        <div className="grid grid-cols-2 sm:grid-cols-3 gap-4">
          <KPICard
            label={`Total Revenue Proxy · ${segment} · ${selectedQuarter}`}
            value={formatCrores(totalRevenue)}
            accent="brand"
          />
          <KPICard
            label="Total Units (Retail)"
            value={formatUnits(totalUnits)}
          />
          <KPICard
            label="Avg Data Completeness"
            value={avgCompleteness != null ? formatPctPlain(avgCompleteness * 100) : '—'}
            accent={avgCompleteness != null && avgCompleteness >= 0.9 ? 'positive' : 'negative'}
          />
        </div>
      )}

      {/* Bar chart — revenue by OEM */}
      <div className="section-card">
        <h2 className="text-sm font-semibold text-slate-900 mb-4">
          Revenue by OEM — {segment} · {selectedQuarter}
          <span className="ml-2 text-xs font-normal text-slate-400">top 10</span>
        </h2>
        {loading ? (
          <SkeletonChart height="h-56" />
        ) : (
          <RevenueBarChart data={chartData} height={240} />
        )}
      </div>

      {/* Detail table */}
      <div className="section-card">
        <h2 className="text-sm font-semibold text-slate-900 mb-4">
          OEM × Segment Revenue — {selectedQuarter}
        </h2>
        {loading ? (
          <SkeletonTable rows={8} cols={5} />
        ) : (
          <DataTable<RevenueTableRow>
            columns={columns}
            data={tableRows}
            rowKey={(row) => `${row.oem_id}-${row.fy_quarter}-${row.segment_code}`}
            defaultSortKey="revenue_retail_cr"
            defaultSortDir="desc"
            emptyMessage={`No revenue data for ${segment} · ${selectedQuarter}`}
          />
        )}
      </div>
    </div>
  );
}
