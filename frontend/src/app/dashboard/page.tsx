'use client';

import { useEffect, useState, useMemo } from 'react';
import Link from 'next/link';
import type { DashboardAPIResponse, TIVRow, OEMShareRow, SegmentCode, TIVChartPoint } from '@/types';
import { KPICard } from '@/components/ui/KPICard';
import { SegmentTabs } from '@/components/ui/SegmentTabs';
import { DataTable, type Column } from '@/components/ui/DataTable';
import { SkeletonKPIRow, SkeletonChart, SkeletonTable } from '@/components/ui/Skeleton';
import { TIVLineChart } from '@/components/charts/TIVLineChart';
import {
  formatUnits,
  formatPctPlain,
  formatPct,
  formatMonthLabel,
  momGrowth,
  deltaColor,
} from '@/lib/format';
import { clsx } from 'clsx';

// ---------------------------------------------------------------------------
// OEM share table row type (augmented)
// ---------------------------------------------------------------------------
interface ShareTableRow extends OEMShareRow {
  mom_pct?: number | null;
}

// ---------------------------------------------------------------------------
// Prepare chart data from TIV rows
// ---------------------------------------------------------------------------
function prepareTIVChart(rows: TIVRow[]): TIVChartPoint[] {
  return rows
    .slice()
    .reverse()
    .map(r => ({
      month: formatMonthLabel(r.month_key),
      total: r.tiv_units,
      ev: r.ev_units,
      evPct: r.ev_penetration_pct ?? 0,
    }));
}

// ---------------------------------------------------------------------------
// Prepare OEM share table: latest month, sorted by units
// ---------------------------------------------------------------------------
function prepareShareTable(rows: OEMShareRow[], segment: SegmentCode): ShareTableRow[] {
  const segRows = rows.filter(r => r.segment_code === segment);
  if (!segRows.length) return [];

  // Get the most recent month
  const latestMonth = segRows.reduce((a, b) => (a.month_key > b.month_key ? a : b)).month_key;
  const prevMonth = segRows
    .filter(r => r.month_key < latestMonth)
    .reduce((a, b) => (a ? (a.month_key > b.month_key ? a : b) : b), null as OEMShareRow | null)
    ?.month_key ?? null;

  const latestRows = segRows.filter(r => r.month_key === latestMonth);
  const prevRows = prevMonth ? segRows.filter(r => r.month_key === prevMonth) : [];

  return latestRows
    .map(r => {
      const prev = prevRows.find(p => p.oem_name === r.oem_name);
      return {
        ...r,
        mom_pct: prev ? momGrowth(r.oem_units, prev.oem_units) : null,
      };
    })
    .sort((a, b) => b.oem_units - a.oem_units);
}

// ---------------------------------------------------------------------------
// OEM share table columns
// ---------------------------------------------------------------------------
const shareColumns: Column<ShareTableRow>[] = [
  {
    key: 'oem_name',
    label: 'OEM',
    sortable: true,
    render: (row) => (
      <div>
        <div className="font-medium text-slate-900 text-sm">{row.oem_name}</div>
        {row.nse_ticker && (
          <div className="text-xs text-slate-400 tabular-nums">{row.nse_ticker}</div>
        )}
      </div>
    ),
  },
  {
    key: 'oem_units',
    label: 'Units',
    sortable: true,
    align: 'right',
    render: (row) => (
      <span className="tabular-nums font-medium">{formatUnits(row.oem_units)}</span>
    ),
  },
  {
    key: 'market_share_pct',
    label: 'Share %',
    sortable: true,
    align: 'right',
    render: (row) => (
      <div className="flex items-center justify-end gap-2">
        <div
          className="h-1.5 bg-brand-200 rounded-full overflow-hidden w-16"
          title={`${row.market_share_pct?.toFixed(1)}%`}
        >
          <div
            className="h-full bg-brand-600 rounded-full"
            style={{ width: `${Math.min(row.market_share_pct ?? 0, 100)}%` }}
          />
        </div>
        <span className="tabular-nums w-10 text-right">
          {formatPctPlain(row.market_share_pct)}
        </span>
      </div>
    ),
  },
  {
    key: 'mom_pct',
    label: 'MoM',
    sortable: true,
    align: 'right',
    render: (row) => {
      if (row.mom_pct == null) return <span className="text-slate-300">—</span>;
      return (
        <span className={clsx('tabular-nums text-xs font-semibold', deltaColor(row.mom_pct))}>
          {row.mom_pct > 0 ? '▲' : row.mom_pct < 0 ? '▼' : ''}{' '}
          {Math.abs(row.mom_pct).toFixed(1)}%
        </span>
      );
    },
  },
  {
    key: 'nse_ticker',
    label: 'Ticker',
    render: (row) => row.nse_ticker ? (
      <Link
        href={`/oem/${row.nse_ticker}`}
        className="text-xs text-brand-600 hover:underline font-medium"
      >
        {row.nse_ticker} →
      </Link>
    ) : <span className="text-slate-300 text-xs">—</span>,
  },
];

// ---------------------------------------------------------------------------
// Dashboard Page
// ---------------------------------------------------------------------------
export default function DashboardPage() {
  const [data, setData] = useState<DashboardAPIResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [segment, setSegment] = useState<SegmentCode>('PV');

  useEffect(() => {
    setLoading(true);
    fetch('/api/dashboard')
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((d: DashboardAPIResponse) => {
        setData(d);
        setLoading(false);
      })
      .catch(e => {
        setError(e.message);
        setLoading(false);
      });
  }, []);

  // Filter TIV rows for active segment
  const tivRows = useMemo(
    () => (data?.tiv ?? []).filter(r => r.segment_code === segment),
    [data, segment]
  );

  const tivChartData = useMemo(() => prepareTIVChart(tivRows), [tivRows]);
  const shareTableData = useMemo(
    () => prepareShareTable(data?.share ?? [], segment),
    [data, segment]
  );

  // KPI values from most recent row
  const latestTIV = tivRows[0] ?? null;
  const prevTIV = tivRows[1] ?? null;
  const mom = latestTIV && prevTIV ? momGrowth(latestTIV.tiv_units, prevTIV.tiv_units) : null;

  // Latest month label for section headers
  const latestMonthLabel = latestTIV ? formatMonthLabel(latestTIV.month_key) : '';

  return (
    <div className="space-y-6 max-w-[1400px]">
      {/* Page header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <div>
          <h1 className="text-xl font-bold text-slate-900 tracking-tight">Industry Pulse</h1>
          <p className="text-sm text-slate-500 mt-0.5">
            VAHAN registrations · All India · {latestMonthLabel || 'Loading…'}
          </p>
        </div>
        <SegmentTabs value={segment} onChange={setSegment} />
      </div>

      {/* Error state */}
      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg px-4 py-3 text-sm text-red-700">
          Failed to load data: {error}. Verify Supabase credentials in environment.
        </div>
      )}

      {/* KPI Cards */}
      {loading ? (
        <SkeletonKPIRow count={4} />
      ) : (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
          <KPICard
            label={`MTD Units · ${segment}`}
            value={formatUnits(latestTIV?.tiv_units)}
            delta={mom}
            deltaLabel="vs prev month"
          />
          <KPICard
            label="EV Penetration"
            value={latestTIV?.ev_penetration_pct != null
              ? formatPctPlain(latestTIV.ev_penetration_pct)
              : '—'}
            accent="purple"
            subLabel={`${formatUnits(latestTIV?.ev_units)} EV units`}
          />
          <KPICard
            label="FY Quarter"
            value={latestTIV?.fy_quarter ?? '—'}
            accent="brand"
          />
          <KPICard
            label="MoM Growth"
            value={mom != null ? formatPct(mom) : '—'}
            accent={mom != null ? (mom > 0 ? 'positive' : 'negative') : 'default'}
            subLabel={prevTIV ? `prev: ${formatUnits(prevTIV.tiv_units)}` : undefined}
          />
        </div>
      )}

      {/* TIV Trend Chart */}
      <div className="section-card">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-sm font-semibold text-slate-900">
            Monthly TIV — {segment}
            <span className="ml-2 text-xs font-normal text-slate-400">last 13 months</span>
          </h2>
        </div>
        {loading ? (
          <SkeletonChart height="h-64" />
        ) : tivChartData.length === 0 ? (
          <div className="flex items-center justify-center h-40 text-slate-400 text-sm">
            No TIV data for {segment}
          </div>
        ) : (
          <TIVLineChart data={tivChartData} height={264} />
        )}
      </div>

      {/* OEM Market Share Table */}
      <div className="section-card">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-sm font-semibold text-slate-900">
            OEM Market Share — {segment}
            {latestMonthLabel && (
              <span className="ml-2 text-xs font-normal text-slate-400">{latestMonthLabel}</span>
            )}
          </h2>
          <span className="text-xs text-slate-400">
            {shareTableData.length} OEMs
          </span>
        </div>
        {loading ? (
          <SkeletonTable rows={8} cols={5} />
        ) : (
          <DataTable<ShareTableRow>
            columns={shareColumns}
            data={shareTableData}
            rowKey={(row) => `${row.oem_name}-${row.month_key}`}
            defaultSortKey="oem_units"
            defaultSortDir="desc"
            emptyMessage={`No market share data for ${segment}`}
          />
        )}
      </div>
    </div>
  );
}
