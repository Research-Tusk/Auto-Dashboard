'use client';

import { useEffect, useState, useMemo } from 'react';
import Link from 'next/link';
import { createClient } from '@/lib/supabase';
import type { OEMScorecard, SegmentCode } from '@/types';
import { KPICard } from '@/components/ui/KPICard';
import { DataTable, type Column } from '@/components/ui/DataTable';
import { SegmentTabs } from '@/components/ui/SegmentTabs';
import { SkeletonKPIRow, SkeletonTable } from '@/components/ui/Skeleton';
import { Disclaimer } from '@/components/ui/Disclaimer';
import { formatCrores, formatUnits, formatPct } from '@/lib/format';
import { clsx } from 'clsx';

// ---------------------------------------------------------------------------
// Scorecard page — OEM competitive snapshot
// ---------------------------------------------------------------------------

const SEGMENT_ID_MAP: Record<number, SegmentCode> = { 1: 'PV', 2: 'CV', 3: '2W' };
const SEGMENT_CODE_TO_ID: Record<string, number> = { PV: 1, CV: 2, '2W': 3 };

interface ScorecardRow extends OEMScorecard {
  oem_name: string;
  nse_ticker: string | null;
  segment_code: SegmentCode;
  rank: number;
}

export default function ScorecardPage() {
  const [scorecards, setScorecards] = useState<OEMScorecard[]>([]);
  const [oems, setOems] = useState<{ oem_id: number; oem_name: string; nse_ticker: string | null }[]>([]);
  const [loading, setLoading] = useState(true);
  const [segment, setSegment] = useState<SegmentCode>('PV');

  useEffect(() => {
    const supabase = createClient();
    const fetchData = async () => {
      const [scResult, oemResult] = await Promise.all([
        supabase
          .from('oem_scorecard')
          .select('*')
          .order('fy_quarter', { ascending: false }),
        supabase
          .from('dim_oem')
          .select('oem_id,oem_name,nse_ticker')
          .eq('is_in_scope', true)
          .order('oem_name'),
      ]);
      if (!scResult.error) setScorecards((scResult.data ?? []) as OEMScorecard[]);
      if (!oemResult.error) setOems(oemResult.data ?? []);
      setLoading(false);
    };
    fetchData();
  }, []);

  const oemMap = useMemo(
    () => new Map(oems.map(o => [o.oem_id, o])),
    [oems]
  );

  // Latest quarter available
  const latestQuarter = useMemo(() => {
    const quarters = [...new Set(scorecards.map(s => s.fy_quarter))].sort().reverse();
    return quarters[0] ?? '';
  }, [scorecards]);

  const segmentId = SEGMENT_CODE_TO_ID[segment];

  const rows = useMemo((): ScorecardRow[] => {
    return scorecards
      .filter(s => s.segment_id === segmentId && s.fy_quarter === latestQuarter)
      .map(s => {
        const oem = oemMap.get(s.oem_id);
        return {
          ...s,
          oem_name: oem?.oem_name ?? `OEM #${s.oem_id}`,
          nse_ticker: oem?.nse_ticker ?? null,
          segment_code: SEGMENT_ID_MAP[s.segment_id],
          rank: 0,
        };
      })
      .sort((a, b) => (b.ms_pct ?? 0) - (a.ms_pct ?? 0))
      .map((r, i) => ({ ...r, rank: i + 1 }));
  }, [scorecards, segmentId, latestQuarter, oemMap]);

  // KPI totals
  const totalUnits = rows.reduce((s, r) => s + (r.units_retail_qtd ?? 0), 0);
  const topOEM = rows[0];

  const msBar = (pct: number | null) => {
    if (pct == null) return null;
    return (
      <div className="flex items-center gap-2">
        <div className="flex-1 bg-slate-100 rounded-full h-1.5 min-w-[60px]">
          <div
            className="bg-brand-500 h-1.5 rounded-full"
            style={{ width: `${Math.min(pct * 100, 100)}%` }}
          />
        </div>
        <span className="tabular-nums text-xs w-12 text-right">{formatPct(pct)}</span>
      </div>
    );
  };

  const columns: Column<ScorecardRow>[] = [
    {
      key: 'rank',
      label: '#',
      align: 'center',
      render: (row) => (
        <span className={clsx(
          'tabular-nums text-xs font-bold',
          row.rank === 1 ? 'text-amber-500' : row.rank === 2 ? 'text-slate-400' : row.rank === 3 ? 'text-orange-400' : 'text-slate-300'
        )}>{row.rank}</span>
      ),
    },
    {
      key: 'oem_name',
      label: 'OEM',
      sortable: true,
      render: (row) => (
        <div className="flex items-center gap-2">
          <div>
            <div className="font-medium text-slate-900 text-sm">
              {row.nse_ticker ? (
                <Link href={`/oem/${row.nse_ticker}`} className="hover:text-brand-600 hover:underline">
                  {row.oem_name}
                </Link>
              ) : row.oem_name}
            </div>
            {row.nse_ticker && <div className="text-xs text-slate-400">{row.nse_ticker}</div>}
          </div>
        </div>
      ),
    },
    {
      key: 'ms_pct',
      label: 'Market Share',
      sortable: true,
      render: (row) => msBar(row.ms_pct),
    },
    {
      key: 'units_retail_qtd',
      label: 'Units (QTD)',
      sortable: true,
      align: 'right',
      render: (row) => <span className="tabular-nums">{formatUnits(row.units_retail_qtd)}</span>,
    },
    {
      key: 'revenue_retail_cr',
      label: 'Revenue (Cr)',
      sortable: true,
      align: 'right',
      render: (row) => <span className="tabular-nums">{formatCrores(row.revenue_retail_cr)}</span>,
    },
    {
      key: 'ms_pct_qoq',
      label: 'MS QoQ',
      sortable: true,
      align: 'right',
      render: (row) => {
        const v = row.ms_pct_qoq;
        if (v == null) return <span className="text-slate-300">—</span>;
        return (
          <span className={clsx('tabular-nums text-xs font-semibold', v >= 0 ? 'text-emerald-600' : 'text-red-500')}>
            {v >= 0 ? '+' : ''}{(v * 100).toFixed(1)}pp
          </span>
        );
      },
    },
    {
      key: 'ms_pct_yoy',
      label: 'MS YoY',
      sortable: true,
      align: 'right',
      render: (row) => {
        const v = row.ms_pct_yoy;
        if (v == null) return <span className="text-slate-300">—</span>;
        return (
          <span className={clsx('tabular-nums text-xs font-semibold', v >= 0 ? 'text-emerald-600' : 'text-red-500')}>
            {v >= 0 ? '+' : ''}{(v * 100).toFixed(1)}pp
          </span>
        );
      },
    },
  ];

  return (
    <div className="space-y-6 max-w-[1400px]">
      <div className="flex flex-col sm:flex-row sm:items-start justify-between gap-3">
        <div>
          <h1 className="text-xl font-bold text-slate-900 tracking-tight">OEM Scorecard</h1>
          <p className="text-sm text-slate-500 mt-0.5">
            Market share snapshot · {latestQuarter || '—'}
          </p>
        </div>
        <SegmentTabs value={segment} onChange={setSegment} compact />
      </div>

      <Disclaimer variant="inline" />

      {loading ? (
        <SkeletonKPIRow count={3} />
      ) : (
        <div className="grid grid-cols-2 sm:grid-cols-3 gap-4">
          <KPICard
            label={`Total Units · ${segment} · ${latestQuarter}`}
            value={formatUnits(totalUnits)}
            accent="brand"
          />
          <KPICard
            label="Market Leader"
            value={topOEM?.oem_name ?? '—'}
          />
          <KPICard
            label="Leader Market Share"
            value={topOEM?.ms_pct != null ? formatPct(topOEM.ms_pct) : '—'}
            accent="positive"
          />
        </div>
      )}

      <div className="section-card">
        <h2 className="text-sm font-semibold text-slate-900 mb-4">
          Competitive Snapshot — {segment} · {latestQuarter}
        </h2>
        {loading ? (
          <SkeletonTable rows={8} cols={7} />
        ) : (
          <DataTable<ScorecardRow>
            columns={columns}
            data={rows}
            rowKey={(row) => `${row.oem_id}-${row.fy_quarter}-${row.segment_id}`}
            defaultSortKey="ms_pct"
            defaultSortDir="desc"
            emptyMessage={`No scorecard data for ${segment} · ${latestQuarter}`}
          />
        )}
      </div>
    </div>
  );
}
