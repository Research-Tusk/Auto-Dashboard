'use client';

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  type TooltipProps,
} from 'recharts';

const POWERTRAIN_COLORS: Record<string, string> = {
  ICE: '#64748b',
  EV: '#10b981',
  HEV: '#3b82f6',
  PHEV: '#8b5cf6',
  CNG: '#f59e0b',
  OTHER: '#e2e8f0',
};

interface PowertrainDataPoint {
  quarter: string;
  [powertrain: string]: number | string;
}

interface Props {
  data: PowertrainDataPoint[];
  keys: string[];
  height?: number;
}

function CustomTooltip({ active, payload, label }: TooltipProps<number, string>) {
  if (!active || !payload?.length) return null;
  const total = payload.reduce((s, p) => s + (p.value as number ?? 0), 0);
  return (
    <div className="bg-white border border-slate-200 rounded-lg shadow-lg px-3 py-2 text-xs">
      <p className="font-semibold text-slate-700 mb-1">{label}</p>
      {payload.map((entry) => (
        <div key={entry.name} className="flex items-center gap-2">
          <span className="w-2 h-2 rounded-full" style={{ background: entry.color }} />
          <span className="text-slate-600">{entry.name}:</span>
          <span className="font-medium text-slate-900">
            {total > 0 ? ((entry.value as number / total) * 100).toFixed(1) : '0.0'}%
          </span>
        </div>
      ))}
    </div>
  );
}

export function PowertrainMixChart({ data, keys, height = 240 }: Props) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={data} margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
        <XAxis
          dataKey="quarter"
          tick={{ fontSize: 11, fill: '#94a3b8' }}
          axisLine={false}
          tickLine={false}
        />
        <YAxis
          tick={{ fontSize: 11, fill: '#94a3b8' }}
          axisLine={false}
          tickLine={false}
          width={48}
          tickFormatter={(v) => `${(v / 1000).toFixed(0)}k`}
        />
        <Tooltip content={<CustomTooltip />} />
        <Legend wrapperStyle={{ fontSize: 11, paddingTop: 8 }} />
        {keys.map((key) => (
          <Bar
            key={key}
            dataKey={key}
            stackId="a"
            fill={POWERTRAIN_COLORS[key] ?? '#94a3b8'}
          />
        ))}
      </BarChart>
    </ResponsiveContainer>
  );
}
