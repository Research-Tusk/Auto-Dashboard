'use client';

import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts';

interface OEMTrendDataPoint {
  month_key: string;
  [key: string]: string | number;
}

interface OEMTrendChartProps {
  data: OEMTrendDataPoint[];
}

const SEGMENT_COLORS: Record<string, string> = {
  PV_ICE: '#3b82f6',
  PV_EV:  '#8b5cf6',
  CV_ICE: '#f59e0b',
  CV_EV:  '#f97316',
  '2W_ICE': '#10b981',
  '2W_EV':  '#34d399',
};

export function OEMTrendChart({ data }: OEMTrendChartProps) {
  if (!data.length) {
    return <div className="h-64 flex items-center justify-center text-slate-400">No data</div>;
  }

  // Determine which series keys exist
  const seriesKeys = Object.keys(data[0]).filter(k => k !== 'month_key');

  return (
    <ResponsiveContainer width="100%" height={250}>
      <LineChart data={data} margin={{ top: 5, right: 20, left: 20, bottom: 5 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
        <XAxis
          dataKey="month_key"
          tickFormatter={(v) => v?.slice(0, 7) ?? ''}
          tick={{ fontSize: 11, fill: '#94a3b8' }}
          axisLine={false}
          tickLine={false}
        />
        <YAxis
          tickFormatter={(v) => `${(v / 1000).toFixed(0)}K`}
          tick={{ fontSize: 11, fill: '#94a3b8' }}
          axisLine={false}
          tickLine={false}
        />
        <Tooltip
          formatter={(value: number, name: string) => [value.toLocaleString(), name]}
          contentStyle={{ borderRadius: '8px', border: '1px solid #e2e8f0' }}
        />
        <Legend />
        {seriesKeys.map(key => (
          <Line
            key={key}
            type="monotone"
            dataKey={key}
            stroke={SEGMENT_COLORS[key] ?? '#94a3b8'}
            strokeWidth={2}
            dot={false}
            name={key.replace('_', ' ')}
          />
        ))}
      </LineChart>
    </ResponsiveContainer>
  );
}
