'use client';

import { clsx } from 'clsx';
import type { SegmentCode } from '@/types';

const SEGMENTS: SegmentCode[] = ['PV', 'CV', '2W'];

const LABELS: Record<SegmentCode, string> = {
  PV: 'Passenger Vehicles',
  CV: 'Commercial Vehicles',
  '2W': 'Two Wheelers',
};

interface SegmentTabsProps {
  value: SegmentCode;
  onChange: (s: SegmentCode) => void;
  compact?: boolean;
}

export function SegmentTabs({ value, onChange, compact = false }: SegmentTabsProps) {
  return (
    <div className="flex rounded-lg border border-slate-200 bg-slate-50 p-0.5 gap-0.5">
      {SEGMENTS.map((seg) => (
        <button
          key={seg}
          onClick={() => onChange(seg)}
          className={clsx(
            'rounded-md px-3 text-xs font-medium transition-all',
            compact ? 'py-1' : 'py-1.5',
            value === seg
              ? 'bg-white text-slate-900 shadow-sm border border-slate-200'
              : 'text-slate-500 hover:text-slate-700'
          )}
        >
          {compact ? seg : LABELS[seg]}
        </button>
      ))}
    </div>
  );
}
