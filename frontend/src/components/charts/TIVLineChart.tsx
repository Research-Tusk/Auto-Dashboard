'use client';

import {
  ComposedChart,
  Area,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  type TooltipProps,
} from 'recharts';
import type { TIVChartPoint } from '@/types';
import { formatAxisUnits, formatUnits, formatPctPlain } from '@/lib/format';

interface Props {
  data: TIVChartPoint[];
  height?: number;
}

function CustomTooltip({ active, payload, label }: TooltipProps<number, string>) {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-white border border-slate-200 rounded-lg shadow-lg px-3 py-2 text-xs">
      <p className="font-semibold text-slate-700 mb-1">{label}</p>
      {payload.map((entry) => (
        <div key={entry.name} className="flex items-center gap-2">
          <span className="w-2 h-2 rounded-full" style={{ background: entry.color }} />
          <span className="text-slate-600">{entry.name}:</span>
          <span className="font-medium text-slate-900">
            {entry.name === 'YoY %'
              ? `${entry.value != null ? entry.value.toFixed(1) : '—'}%`
              : formatUnits(entry.value as number)}
          </span>
        </div>
      ))}
    </div>
  );
}

export function TIVLineChart({ data, height = 240 }: Props) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <ComposedChart data={data} margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
        <XAxis
          dataKey="quarter"
          tick={{ fontSize: 11, fill: '#94a3b8' }}
          axisLine={false}
          tickLine={false}
        />
        <YAxis
          yAxisId="units"
          orientation="left"
          tickFormatter={formatAxisUnits}
          tick={{ fontSize: 11, fill: '#94a3b8' }}
          axisLine={false}
          tickLine={false}
          width={52}
        />
        <YAxis
          yAxisId="pct"
          orientation="right"
          tickFormatter={(v) => `${v}%`}
          tick={{ fontSize: 11, fill: '#94a3b8' }}
          axisLine={false}
          tickLine={false}
          width={40}
        />
        <Tooltip content={<CustomTooltip />} />
        <Legend wrapperStyle={{ fontSize: 11, paddingTop: 8 }} />
        <Area
          yAxisId="units"
          type="monotone"
          dataKey="tiv"
          name="TIV (units)"
          fill="#eff6ff"
          stroke="#3b82f6"
          strokeWidth={2}
          dot={false}
          activeDot={{ r: 4 }}
        />
        <Line
          yAxisId="pct"
          type="monotone"
          dataKey="yoy_pct"
          name="YoY %"
          stroke="#f59e0b"
          strokeWidth={1.5}
          dot={false}
          activeDot={{ r: 3 }}
          strokeDasharray="4 2"
        />
      </ComposedChart>
    </ResponsiveContainer>
  );
}
