'use client';

import {
  PieChart, Pie, Cell, Tooltip, Legend, ResponsiveContainer
} from 'recharts';

interface PowertrainDataPoint {
  name: string;
  value: number;
}

interface PowertrainMixChartProps {
  data: PowertrainDataPoint[];
}

const POWERTRAIN_COLORS: Record<string, string> = {
  'PV ICE': '#3b82f6',
  'PV EV':  '#8b5cf6',
  'CV ICE': '#f59e0b',
  'CV EV':  '#f97316',
  '2W ICE': '#10b981',
  '2W EV':  '#34d399',
};

const DEFAULT_COLORS = ['#6366f1', '#f59e0b', '#10b981', '#ef4444', '#8b5cf6', '#06b6d4'];

export function PowertrainMixChart({ data }: PowertrainMixChartProps) {
  if (!data.length || data.every(d => d.value === 0)) {
    return <div className="h-64 flex items-center justify-center text-slate-400">No data</div>;
  }

  return (
    <ResponsiveContainer width="100%" height={250}>
      <PieChart>
        <Pie
          data={data}
          cx="50%"
          cy="50%"
          innerRadius={60}
          outerRadius={100}
          paddingAngle={2}
          dataKey="value"
          label={({ name, percent }) =>
            percent > 0.05 ? `${name} ${(percent * 100).toFixed(0)}%` : ''
          }
          labelLine={false}
        >
          {data.map((entry, index) => (
            <Cell
              key={entry.name}
              fill={POWERTRAIN_COLORS[entry.name] ?? DEFAULT_COLORS[index % DEFAULT_COLORS.length]}
            />
          ))}
        </Pie>
        <Tooltip
          formatter={(value: number) => [value.toLocaleString(), 'Units']}
          contentStyle={{ borderRadius: '8px', border: '1px solid #e2e8f0' }}
        />
        <Legend />
      </PieChart>
    </ResponsiveContainer>
  );
}
