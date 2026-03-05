'use client';

import { useEffect, useState, useMemo } from 'react';
import { useParams } from 'next/navigation';
import { createClient } from '@/lib/supabase';
import type { SegmentCode } from '@/types';
import { KPICard } from '@/components/ui/KPICard';
import { DataTable, type Column } from '@/components/ui/DataTable';
import { SegmentTabs } from '@/components/ui/SegmentTabs';
import { TIVLineChart } from '@/components/charts/TIVLineChart';
import { SegmentTrendChart } from '@/components/charts/SegmentTrendChart';
import { SkeletonKPIRow, SkeletonChart, SkeletonTable } from '@/components/ui/Skeleton';
import { Disclaimer } from '@/components/ui/Disclaimer';
import { formatCrores, formatUnits, formatPct, formatLakhs } from '@/lib/format';
import { clsx } from 'clsx';

// ---------------------------------------------------------------------------
// OEM deep-dive page — /oem/[ticker]
// ---------------------------------------------------------------------------

const SEGMENT_CODE_TO_ID: Record<string, number> = { PV: 1, CV: 2, '2W': 3 };
const SEGMENT_ID_MAP: Record<number, SegmentCode> = { 1: 'PV', 2: 'CV', 3: '2W' };

interface OEMInfo {
  oem_id: number;
  oem_name: string;
  nse_ticker: string | null;
  primary_segments: string[];
  description: string | null;
}

interface MSHistory {
  fy_quarter: string;
  segment_id: number;
  units_retail: number | null;
  ms_pct: number | null;
  ms_pct_qoq: number | null;
  ms_pct_yoy: number | null;
}

interface RevenueRow {
  fy_quarter: string;
  segment_id: number;
  units_retail: number | null;
  asp_used: number | null;
  revenue_retail_cr: number | null;
  data_completeness: number | null;
}

interface TIVRow {
  fy_quarter: string;
  segment_id: number;
  tiv_retail: number | null;
  tiv_yoy_pct: number | null;
}

interface FlatRevenueRow extends RevenueRow {
  segment_code: SegmentCode;
}

export default function OEMPage() {
  const { ticker } = useParams<{ ticker: string }>();
  const [oem, setOEM] = useState<OEMInfo | null>(null);
  const [msHistory, setMSHistory] = useState<MSHistory[]>([]);
  const [revenue, setRevenue] = useState<RevenueRow[]>([]);
  const [tiv, setTIV] = useState<TIVRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [segment, setSegment] = useState<SegmentCode>('PV');

  useEffect(() => {
    if (!ticker) return;
    const supabase = createClient();
    const fetchData = async () => {
      // 1. Fetch OEM info
      const oemResult = await supabase
        .from('dim_oem')
        .select('oem_id,oem_name,nse_ticker,primary_segments,description')
        .eq('nse_ticker', ticker.toUpperCase())
        .single();

      if (oemResult.error || !oemResult.data) {
        setLoading(false);
        return;
      }

      const oemData = oemResult.data as OEMInfo;
      setOEM(oemData);

      // 2. Fetch MS history, revenue, TIV in parallel
      const [msResult, revResult, tivResult] = await Promise.all([
        supabase
          .from('oem_ms_history')
          .select('fy_quarter,segment_id,units_retail,ms_pct,ms_pct_qoq,ms_pct_yoy')
          .eq('oem_id', oemData.oem_id)
          .order('fy_quarter', { ascending: true })
          .limit(60),
        supabase
          .from('est_quarterly_revenue')
          .select('fy_quarter,segment_id,units_retail,asp_used,revenue_retail_cr,data_completeness')
          .eq('oem_id', oemData.oem_id)
          .order('fy_quarter', { ascending: false })
          .limit(60),
        supabase
          .from('fact_tiv')
          .select('fy_quarter,segment_id,tiv_retail,tiv_yoy_pct')
          .order('fy_quarter', { ascending: true })
          .limit(60),
      ]);

      if (!msResult.error) setMSHistory((msResult.data ?? []) as MSHistory[]);
      if (!revResult.error) setRevenue((revResult.data ?? []) as RevenueRow[]);
      if (!tivResult.error) setTIV((tivResult.data ?? []) as TIVRow[]);
      setLoading(false);
    };

    fetchData();
  }, [ticker]);

  const segmentId = SEGMENT_CODE_TO_ID[segment];

  // MS chart data
  const msChartData = useMemo(() => {
    return msHistory
      .filter(h => h.segment_id === segmentId)
      .map(h => ({
        quarter: h.fy_quarter,
        [oem?.oem_name ?? 'OEM']: parseFloat(((h.ms_pct ?? 0) * 100).toFixed(2)),
      }));
  }, [msHistory, segmentId, oem]);

  // TIV chart data
  const tivChartData = useMemo(() => {
    return tiv
      .filter(t => t.segment_id === segmentId)
      .map(t => ({
        quarter: t.fy_quarter,
        tiv: t.tiv_retail ?? 0,
        yoy_pct: t.tiv_yoy_pct != null ? parseFloat((t.tiv_yoy_pct * 100).toFixed(1)) : null,
      }));
  }, [tiv, segmentId]);

  // Latest KPIs
  const latestMS = useMemo(() => {
    const rows = msHistory.filter(h => h.segment_id === segmentId);
    return rows[rows.length - 1] ?? null;
  }, [msHistory, segmentId]);

  const latestRevenue = useMemo(() => {
    const rows = revenue.filter(r => r.segment_id === segmentId);
    return rows[0] ?? null; // sorted desc
  }, [revenue, segmentId]);

  // Revenue table
  const revenueRows = useMemo((): FlatRevenueRow[] => {
    return revenue
      .filter(r => r.segment_id === segmentId)
      .map(r => ({ ...r, segment_code: SEGMENT_ID_MAP[r.segment_id] }))
      .slice(0, 12);
  }, [revenue, segmentId]);

  const revColumns: Column<FlatRevenueRow>[] = [
    { key: 'fy_quarter', label: 'Quarter', sortable: true },
    {
      key: 'units_retail',
      label: 'Units',
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
      label: 'Revenue (Cr)',
      sortable: true,
      align: 'right',
      render: (row) => <span className="tabular-nums font-semibold text-brand-700">{formatCrores(row.revenue_retail_cr)}</span>,
    },
    {
      key: 'data_completeness',
      label: 'Completeness',
      sortable: true,
      align: 'right',
      render: (row) => {
        const pct = (row.data_completeness ?? 0) * 100;
        return (
          <span className={clsx('tabular-nums text-xs font-semibold', pct >= 90 ? 'text-emerald-600' : pct >= 70 ? 'text-amber-600' : 'text-red-500')}>
            {pct.toFixed(0)}%
          </span>
        );
      },
    },
  ];

  if (!loading && !oem) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-center">
          <p className="text-lg font-semibold text-slate-700">OEM not found</p>
          <p className="text-sm text-slate-400 mt-1">Ticker <span className="font-mono">{ticker}</span> is not in scope.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6 max-w-[1400px]">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-start justify-between gap-3">
        <div>
          {loading ? (
            <div className="h-7 w-48 bg-slate-100 rounded animate-pulse mb-1" />
          ) : (
            <h1 className="text-xl font-bold text-slate-900 tracking-tight">
              {oem?.oem_name}
              {oem?.nse_ticker && (
                <span className="ml-2 text-sm font-normal text-slate-400 font-mono">{oem.nse_ticker}</span>
              )}
            </h1>
          )}
          {oem?.description && (
            <p className="text-sm text-slate-500 mt-0.5 max-w-xl">{oem.description}</p>
          )}
          {oem?.primary_segments && (
            <div className="flex gap-1 mt-1">
              {oem.primary_segments.map(s => (
                <span key={s} className="px-2 py-0.5 rounded-full bg-brand-50 text-brand-700 text-xs font-medium border border-brand-100">{s}</span>
              ))}
            </div>
          )}
        </div>
        <SegmentTabs value={segment} onChange={setSegment} compact />
      </div>

      <Disclaimer variant="inline" />

      {/* KPI row */}
      {loading ? (
        <SkeletonKPIRow count={4} />
      ) : (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
          <KPICard
            label={`Market Share · ${segment} · Latest`}
            value={latestMS?.ms_pct != null ? formatPct(latestMS.ms_pct) : '—'}
            accent="brand"
          />
          <KPICard
            label="MS QoQ Change"
            value={latestMS?.ms_pct_qoq != null ? `${latestMS.ms_pct_qoq >= 0 ? '+' : ''}${(latestMS.ms_pct_qoq * 100).toFixed(1)}pp` : '—'}
            accent={latestMS?.ms_pct_qoq != null && latestMS.ms_pct_qoq >= 0 ? 'positive' : 'negative'}
          />
          <KPICard
            label="Latest Revenue Proxy (Cr)"
            value={formatCrores(latestRevenue?.revenue_retail_cr ?? null)}
          />
          <KPICard
            label="Latest Units (Retail)"
            value={formatUnits(latestRevenue?.units_retail ?? null)}
          />
        </div>
      )}

      {/* MS trend chart */}
      <div className="section-card">
        <h2 className="text-sm font-semibold text-slate-900 mb-4">Market Share Trend — {segment}</h2>
        {loading ? (
          <SkeletonChart height="h-56" />
        ) : (
          <SegmentTrendChart data={msChartData} lines={oem ? [oem.oem_name] : []} height={240} />
        )}
      </div>

      {/* TIV chart */}
      <div className="section-card">
        <h2 className="text-sm font-semibold text-slate-900 mb-4">Industry TIV — {segment}</h2>
        {loading ? (
          <SkeletonChart height="h-56" />
        ) : (
          <TIVLineChart data={tivChartData} height={240} />
        )}
      </div>

      {/* Revenue table */}
      <div className="section-card">
        <h2 className="text-sm font-semibold text-slate-900 mb-4">Quarterly Revenue Proxy — {segment}</h2>
        {loading ? (
          <SkeletonTable rows={8} cols={5} />
        ) : (
          <DataTable<FlatRevenueRow>
            columns={revColumns}
            data={revenueRows}
            rowKey={(row) => `${row.fy_quarter}-${row.segment_id}`}
            defaultSortKey="fy_quarter"
            defaultSortDir="desc"
            emptyMessage={`No revenue data for ${segment}`}
          />
        )}
      </div>
    </div>
  );
}
