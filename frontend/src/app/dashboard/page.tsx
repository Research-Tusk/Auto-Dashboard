'use client';

import { useEffect, useState } from 'react';
import { RevenueBarChart } from '@/components/charts/RevenueBarChart';
import { TIVLineChart } from '@/components/charts/TIVLineChart';

const SEGMENTS = ['PV', 'CV', '2W'] as const;

export default function DashboardPage() {
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [activeSegment, setActiveSegment] = useState<'PV' | 'CV' | '2W'>('PV');

  useEffect(() => {
    fetch('/api/dashboard')
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);

  const tivData = data?.tiv?.filter((r: any) => r.segment_code === activeSegment) ?? [];

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-slate-900">Industry Pulse</h1>
        <div className="flex gap-2">
          {SEGMENTS.map(seg => (
            <button
              key={seg}
              onClick={() => setActiveSegment(seg)}
              className={`px-3 py-1 rounded text-sm font-medium transition-colors ${
                activeSegment === seg
                  ? 'bg-brand-600 text-white'
                  : 'bg-white text-slate-600 border border-slate-200 hover:border-brand-400'
              }`}
            >
              {seg}
            </button>
          ))}
        </div>
      </div>

      {/* KPI cards */}
      {loading ? (
        <div className="grid grid-cols-4 gap-4">
          {[...Array(4)].map((_, i) => (
            <div key={i} className="kpi-card"><div className="skeleton h-8 w-24 mb-2" /><div className="skeleton h-4 w-16" /></div>
          ))}
        </div>
      ) : (
        <div className="grid grid-cols-4 gap-4">
          {/* Latest month KPIs */}
          {tivData[0] && (
            <>
              <div className="kpi-card">
                <div className="text-2xl font-bold text-slate-900">{(tivData[0].tiv_units / 1000).toFixed(0)}K</div>
                <div className="text-sm text-slate-500 mt-1">MTD Units ({activeSegment})</div>
              </div>
              <div className="kpi-card">
                <div className="text-2xl font-bold text-violet-600">{tivData[0].ev_penetration_pct?.toFixed(1)}%</div>
                <div className="text-sm text-slate-500 mt-1">EV Penetration</div>
              </div>
              <div className="kpi-card">
                <div className="text-2xl font-bold text-slate-900">{tivData[0].fy_quarter}</div>
                <div className="text-sm text-slate-500 mt-1">FY Quarter</div>
              </div>
              <div className="kpi-card">
                <div className="text-2xl font-bold text-emerald-600">
                  {tivData[0].tiv_units > 0 && tivData[1]?.tiv_units
                    ? ((tivData[0].tiv_units / tivData[1].tiv_units - 1) * 100).toFixed(1) + '%'
                    : '—'
                  }
                </div>
                <div className="text-sm text-slate-500 mt-1">MoM Growth</div>
              </div>
            </>
          )}
        </div>
      )}

      {/* TIV trend chart */}
      <div className="bg-white rounded-xl border border-slate-200 p-6">
        <h2 className="text-lg font-semibold text-slate-900 mb-4">Monthly TIV — {activeSegment}</h2>
        {loading
          ? <div className="skeleton h-64 w-full" />
          : <TIVLineChart data={tivData.slice(0, 13).reverse()} />
        }
      </div>
    </div>
  );
}
