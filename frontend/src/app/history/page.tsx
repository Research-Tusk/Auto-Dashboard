'use client';

import { useEffect, useState, useMemo } from 'react';
import { createClient } from '@/lib/supabase';
import type { SegmentCode } from '@/types';
import { KPICard } from '@/components/ui/KPICard';
import { DataTable, type Column } from '@/components/ui/DataTable';
import { SegmentTabs } from '@/components/ui/SegmentTabs';
import { OEMTrendChart } from '@/components/charts/OEMTrendChart';
import { SkeletonKPIRow, SkeletonChart, SkeletonTable } from '@/components/ui/Skeleton';
import { Disclaimer } from '@/components/ui/Disclaimer';
import { formatUnits, formatPct } from '@/lib/format';
import { clsx } from 'clsx';

// ---------------------------------------------------------------------------
// History page — OEM market-share trend (rolling)
// ---------------------------------------------------------------------------

interface OEMInfo {
  oem_id: number;
  oem_name: string;
  nse_ticker: string | null;
}

interface MSHistoryRow {
  fy_quarter: string;
  oem_id: number;
  segment_id: number;
  units_retail: number | null;
  ms_pct: number | null;
  ms_pct_qoq: number | null;
  ms_pct_yoy: number | null;
}

interface FlatRow {
  fy_quarter: string;
  oem_id: number;
  oem_name: string;
  nse_ticker: string | null;
  units_retail: number | null;
  ms_pct: number | null;
  ms_pct_qoq: number | null;
  ms_pct_yoy: number | null;
}

const SEGMENT_CODE_TO_ID: Record<string, number> = { PV: 1, CV: 2, '2W': 3 };

export default function HistoryPage() {
  const [history, setHistory] = useState<MSHistoryRow[]>([]);
  const [oems, setOems] = useState<OEMInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [segment, setSegment] = useState<SegmentCode>('PV');
  const [selectedOEMs, setSelectedOEMs] = useState<number[]>([]);

  useEffect(() => {
    const supabase = createClient();
    const fetchData = async () => {
      const [histResult, oemResult] = await Promise.all([
        supabase
          .from('oem_ms_history')
          .select('fy_quarter,oem_id,segment_id,units_retail,ms_pct,ms_pct_qoq,ms_pct_yoy')
          .order('fy_quarter', { ascending: true })
          .limit(600),
        supabase
          .from('dim_oem')
          .select('oem_id,oem_name,nse_ticker')
          .eq('is_in_scope', true)
          .order('oem_name'),
      ]);
      if (!histResult.error) setHistory((histResult.data ?? []) as MSHistoryRow[]);
      if (!oemResult.error) setOems(oemResult.data ?? []);
      setLoading(false);
    };
    fetchData();
  }, []);

  const oemMap = useMemo(() => new Map(oems.map(o => [o.oem_id, o])), [oems]);

  const segmentId = SEGMENT_CODE_TO_ID[segment];

  // Filtered rows for current segment
  const segmentHistory = useMemo(
    () => history.filter(h => h.segment_id === segmentId),
    [history, segmentId]
  );

  // Unique OEMs in this segment (sorted by latest market share)
  const segmentOEMs = useMemo(() => {
    const latestQ = [...new Set(segmentHistory.map(h => h.fy_quarter))].sort().reverse()[0];
    const latest = segmentHistory.filter(h => h.fy_quarter === latestQ);
    const sorted = [...latest].sort((a, b) => (b.ms_pct ?? 0) - (a.ms_pct ?? 0));
    return sorted.map(h => oemMap.get(h.oem_id)).filter(Boolean) as OEMInfo[];
  }, [segmentHistory, oemMap]);

  // Default: top 5 OEMs selected
  useEffect(() => {
    if (segmentOEMs.length > 0 && selectedOEMs.length === 0) {
      setSelectedOEMs(segmentOEMs.slice(0, 5).map(o => o.oem_id));
    }
  }, [segmentOEMs, selectedOEMs]);

  // Reset selection on segment change
  const handleSegmentChange = (s: SegmentCode) => {
    setSegment(s);
    setSelectedOEMs([]);
  };

  // Chart data
  const chartData = useMemo(() => {
    const quarters = [...new Set(segmentHistory.map(h => h.fy_quarter))].sort();
    return quarters.map(q => {
      const obj: Record<string, number | string> = { quarter: q };
      for (const oemId of selectedOEMs) {
        const row = segmentHistory.find(h => h.fy_quarter === q && h.oem_id === oemId);
        const oem = oemMap.get(oemId);
        if (oem && row?.ms_pct != null) {
          obj[oem.oem_name] = parseFloat((row.ms_pct * 100).toFixed(2));
        }
      }
      return obj;
    });
  }, [segmentHistory, selectedOEMs, oemMap]);

  const chartLines = useMemo(() => {
    return selectedOEMs
      .map(id => oemMap.get(id)?.oem_name)
      .filter(Boolean) as string[];
  }, [selectedOEMs, oemMap]);

  // Table: latest quarter data for all OEMs in segment
  const latestQ = useMemo(() => {
    return [...new Set(segmentHistory.map(h => h.fy_quarter))].sort().reverse()[0] ?? '';
  }, [segmentHistory]);

  const tableRows = useMemo((): FlatRow[] => {
    return segmentHistory
      .filter(h => h.fy_quarter === latestQ)
      .map(h => ({
        ...h,
        oem_name: oemMap.get(h.oem_id)?.oem_name ?? `OEM #${h.oem_id}`,
        nse_ticker: oemMap.get(h.oem_id)?.nse_ticker ?? null,
      }))
      .sort((a, b) => (b.ms_pct ?? 0) - (a.ms_pct ?? 0));
  }, [segmentHistory, latestQ, oemMap]);

  const totalUnits = tableRows.reduce((s, r) => s + (r.units_retail ?? 0), 0);

  const columns: Column<FlatRow>[] = [
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
      key: 'ms_pct',
      label: 'Market Share',
      sortable: true,
      render: (row) => {
        const pct = row.ms_pct ?? 0;
        return (
          <div className="flex items-center gap-2">
            <div className="flex-1 bg-slate-100 rounded-full h-1.5 min-w-[60px]">
              <div className="bg-brand-500 h-1.5 rounded-full" style={{ width: `${Math.min(pct * 100, 100)}%` }} />
            </div>
            <span className="tabular-nums text-xs w-12 text-right">{formatPct(row.ms_pct)}</span>
          </div>
        );
      },
    },
    {
      key: 'units_retail',
      label: 'Units',
      sortable: true,
      align: 'right',
      render: (row) => <span className="tabular-nums">{formatUnits(row.units_retail)}</span>,
    },
    {
      key: 'ms_pct_qoq',
      label: 'QoQ',
      sortable: true,
      align: 'right',
      render: (row) => {
        const v = row.ms_pct_qoq;
        if (v == null) return <span className="text-slate-300">—</span>;
        return <span className={clsx('tabular-nums text-xs font-semibold', v >= 0 ? 'text-emerald-600' : 'text-red-500')}>{v >= 0 ? '+' : ''}{(v * 100).toFixed(1)}pp</span>;
      },
    },
    {
      key: 'ms_pct_yoy',
      label: 'YoY',
      sortable: true,
      align: 'right',
      render: (row) => {
        const v = row.ms_pct_yoy;
        if (v == null) return <span className="text-slate-300">—</span>;
        return <span className={clsx('tabular-nums text-xs font-semibold', v >= 0 ? 'text-emerald-600' : 'text-red-500')}>{v >= 0 ? '+' : ''}{(v * 100).toFixed(1)}pp</span>;
      },
    },
  ];

  return (
    <div className="space-y-6 max-w-[1400px]">
      <div className="flex flex-col sm:flex-row sm:items-start justify-between gap-3">
        <div>
          <h1 className="text-xl font-bold text-slate-900 tracking-tight">Market Share History</h1>
          <p className="text-sm text-slate-500 mt-0.5">Rolling quarterly trend by OEM</p>
        </div>
        <SegmentTabs value={segment} onChange={handleSegmentChange} compact />
      </div>

      <Disclaimer variant="inline" />

      {loading ? (
        <SkeletonKPIRow count={2} />
      ) : (
        <div className="grid grid-cols-2 gap-4">
          <KPICard label={`Segment Units · ${segment} · ${latestQ}`} value={formatUnits(totalUnits)} accent="brand" />
          <KPICard label="OEMs Tracked" value={String(segmentOEMs.length)} />
        </div>
      )}

      {/* OEM selector */}
      <div className="section-card">
        <h2 className="text-sm font-semibold text-slate-900 mb-3">Select OEMs to chart</h2>
        <div className="flex flex-wrap gap-2">
          {segmentOEMs.map(oem => (
            <button
              key={oem.oem_id}
              onClick={() => {
                setSelectedOEMs(prev =>
                  prev.includes(oem.oem_id)
                    ? prev.filter(id => id !== oem.oem_id)
                    : [...prev, oem.oem_id]
                );
              }}
              className={clsx(
                'px-3 py-1 rounded-full text-xs font-medium border transition-colors',
                selectedOEMs.includes(oem.oem_id)
                  ? 'bg-brand-600 text-white border-brand-600'
                  : 'bg-white text-slate-600 border-slate-200 hover:border-brand-300'
              )}
            >
              {oem.oem_name}
            </button>
          ))}
        </div>
      </div>

      {/* Trend chart */}
      <div className="section-card">
        <h2 className="text-sm font-semibold text-slate-900 mb-4">Market Share Trend — {segment}</h2>
        {loading ? (
          <SkeletonChart height="h-64" />
        ) : (
          <OEMTrendChart data={chartData} lines={chartLines} height={280} />
        )}
      </div>

      {/* Table */}
      <div className="section-card">
        <h2 className="text-sm font-semibold text-slate-900 mb-4">
          Latest Quarter — {segment} · {latestQ}
        </h2>
        {loading ? (
          <SkeletonTable rows={8} cols={5} />
        ) : (
          <DataTable<FlatRow>
            columns={columns}
            data={tableRows}
            rowKey={(row) => `${row.oem_id}-${row.fy_quarter}`}
            defaultSortKey="ms_pct"
            defaultSortDir="desc"
            emptyMessage={`No history data for ${segment}`}
          />
        )}
      </div>
    </div>
  );
}
